// helpers.js
import fs from "fs/promises";
import { createHash } from "crypto";
import fetch from "node-fetch";
import { parse as csvParse } from "csv-parse/sync";
import { XMLParser } from "fast-xml-parser";
import { Client } from "pg";
import { Vector } from "pgvector";
import FormData from "form-data";

/* ---------- FS / download / parse helpers ---------- */
export async function mkdirIfNotExists(path) {
  try {
    await fs.mkdir(path, { recursive: true });
  } catch (e) {}
}

export async function downloadToString(url, headers = {}) {
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`Failed to download ${url} - ${res.status}`);
  return await res.text();
}

export async function saveRawFile(baseDir, filename, content) {
  await mkdirIfNotExists(baseDir);
  const path = `${baseDir}/${filename}`;
  await fs.writeFile(path, content, "utf-8");
  return path;
}

export function checksumString(str) {
  return createHash("sha256").update(str).digest("hex");
}

export function parseDocumentText(raw, mime = "", url = "") {
  if (mime.includes("xml") || url.endsWith(".xml")) {
    const p = new XMLParser({ ignoreAttributes: false });
    const obj = p.parse(raw);
    return JSON.stringify(obj, null, 2);
  }
  if (mime.includes("json") || url.endsWith(".json")) {
    try {
      const obj = JSON.parse(raw);
      if (Array.isArray(obj)) {
        return obj.map((r) => JSON.stringify(r)).join("\n");
      }
      return JSON.stringify(obj, null, 2);
    } catch (e) {}
  }
  if (mime.includes("csv") || url.endsWith(".csv") || url.endsWith(".tsv")) {
    try {
      const records = csvParse(raw, { columns: true, skip_empty_lines: true });
      return records.map((r) => Object.entries(r).map(([k, v]) => `${k}: ${v}`).join("\n")).join("\n\n---\n\n");
    } catch (e) {}
  }
  return raw;
}

export function chunkText(text, wordsPerChunk = 800, overlapWords = 128) {
  const words = text.split(/\s+/);
  const chunks = [];
  let i = 0;
  let idx = 0;
  while (i < words.length) {
    const start = Math.max(0, i - overlapWords);
    const slice = words.slice(start, start + wordsPerChunk);
    const chunkText = slice.join(" ").trim();
    chunks.push({
      chunkIndex: idx,
      text: chunkText,
      summary: chunkText.slice(0, 300).replace(/\s+/g, " ")
    });
    i += wordsPerChunk - overlapWords;
    idx++;
  }
  return chunks;
}

/* ---------- OpenAI embeddings ---------- */
export async function createOpenAIEmbedding(openaiKey, model, input) {
  const res = await fetch("https://api.openai.com/v1/embeddings", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${openaiKey}`
    },
    body: JSON.stringify({ model, input })
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`OpenAI embeddings error: ${res.status} ${text}`);
  }
  const j = await res.json();
  return j.data?.[0]?.embedding ?? null;
}

/* ---------- Postgres + pgvector helpers ---------- */

/**
 * Ensure a pg Client is created and connected.
 * Pass DATABASE_URL env var or connection options.
 */
export async function createPgClient(databaseUrl) {
  if (!databaseUrl) throw new Error("DATABASE_URL is required for Postgres client");
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  return client;
}

/**
 * Upsert vectors into Postgres table `vectors`:
 * schema: id text primary key, embedding vector(DIM), metadata jsonb
 * Uses pgvector's Vector wrapper for embedding parameter.
 *
 * vectors: [{ id, embedding: [float...], metadata: {...} }, ...]
 * dimension must match the model embedding dimension (e.g. 1536)
 */
export async function upsertPgVectors(pgClient, vectors, dimension = 1536) {
  if (!Array.isArray(vectors) || vectors.length === 0) return;
  // Prepare query
  // Use a single transaction + upsert loop for reliability
  await pgClient.query("BEGIN");
  try {
    for (const v of vectors) {
      // use pgvector Vector wrapper
      const vec = new Vector(v.embedding);
      await pgClient.query(
        `INSERT INTO vectors (id, embedding, metadata)
         VALUES ($1, $2, $3)
         ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding, metadata = EXCLUDED.metadata`,
        [v.id, vec, v.metadata || {}]
      );
    }
    await pgClient.query("COMMIT");
  } catch (err) {
    await pgClient.query("ROLLBACK");
    throw err;
  }
}

/**
 * Query nearest neighbors using cosine distance <=> operator.
 * Returns rows: { id, metadata, distance }
 */
export async function queryPgNearest(pgClient, queryEmbedding, topK = 5) {
  const vec = new Vector(queryEmbedding);
  // Use cosine distance operator <=> (pgvector) - smaller = more similar
  const q = `
    SELECT id, metadata, embedding <=> $1 AS distance
    FROM vectors
    ORDER BY embedding <=> $1
    LIMIT $2
  `;
  const result = await pgClient.query(q, [vec, topK]);
  return result.rows;
}

/* ---------- Neo4j nodes helper (unchanged) ---------- */
export async function createNeo4jNodes(driver, { source, dataset, document, chunks }) {
  const session = driver.session();
  try {
    const params = {
      source_id: source.source_id,
      source_name: source.name,
      source_base: source.base_url,
      dataset_id: dataset.dataset_id,
      dataset_title: dataset.title,
      dataset_desc: dataset.description || "",
      doc_id: document.doc_id,
      title: document.title,
      file_url: document.file_url,
      mime: document.mime || "",
      published_at: document.published_at || new Date().toISOString()
    };

    const createDatasetCypher = `
    MERGE (s:Source {source_id:$source_id})
    SET s.name=$source_name, s.base_url=$source_base
    MERGE (d:Dataset {dataset_id:$dataset_id})
    SET d.title=$dataset_title, d.description=$dataset_desc
    MERGE (s)-[:PUBLISHES]->(d)
    CREATE (doc:Document {
      doc_id:$doc_id,
      title:$title,
      file_url:$file_url,
      mime:$mime,
      published_at:datetime($published_at)
    })
    CREATE (d)-[:HAS_DOCUMENT]->(doc)
    RETURN id(doc) as created
    `;
    await session.run(createDatasetCypher, params);

    for (const c of chunks) {
      const chunkCypher = `
      MATCH (doc:Document {doc_id:$doc_id})
      CREATE (c:Chunk {
        chunk_id:$chunk_id,
        chunk_index:$chunk_index,
        text_summary:$text_summary,
        token_count:$token_count,
        citation_label:$citation_label
      })
      CREATE (doc)-[:HAS_CHUNK]->(c)
      RETURN c
      `;
      await session.run(chunkCypher, {
        doc_id: document.doc_id,
        chunk_id: c.chunk_id,
        chunk_index: c.chunk_index,
        text_summary: c.summary,
        token_count: c.token_count || 0,
        citation_label: c.citation_label
      });
    }
  } finally {
    await session.close();
  }
}
