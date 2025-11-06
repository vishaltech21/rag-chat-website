// index.js
import fetch from "node-fetch";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import neo4j from "neo4j-driver";
import {
  downloadToString,
  saveRawFile,
  checksumString,
  parseDocumentText,
  chunkText,
  createOpenAIEmbedding,
  createPgClient,
  upsertPgVectors,
  createNeo4jNodes,
  mkdirIfNotExists,
  queryPgNearest
} from "./helpers.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// CONFIG via env
const OPENAI_KEY = process.env.OPENAI_API_KEY;
const OPENAI_EMBED_MODEL = process.env.OPENAI_EMBED_MODEL || "text-embedding-3-small";
const DATABASE_URL = process.env.DATABASE_URL; // postgres connection string
const VECTOR_DIM = parseInt(process.env.VECTOR_DIM || "1536");
const NEO4J_URI = process.env.NEO4J_URI;
const NEO4J_USER = process.env.NEO4J_USER;
const NEO4J_PASSWORD = process.env.NEO4J_PASSWORD;
const INGEST_BASE_DIR = process.env.INGEST_BASE_DIR || path.join(__dirname, "data");
const CHUNK_WORDS = parseInt(process.env.CHUNK_WORDS || "800");
const OVERLAP_WORDS = parseInt(process.env.OVERLAP_WORDS || "128");

// simple source config (OpenCanada implemented; StatCan placeholder)
const SOURCE_CONFIG = {
  open_canada: {
    name: "OpenCanada",
    list_url: "https://open.canada.ca/data/en/api/3/action/package_list",
    package_show_base: "https://open.canada.ca/data/en/api/3/action/package_show?id="
  },
  statcan: {
    name: "StatCan",
    list_url: "https://www150.statcan.gc.ca/n1/en/dai/data.json"
  }
};

// connect Neo4j if provided
let neo4jDriver = null;
if (NEO4J_URI && NEO4J_USER && NEO4J_PASSWORD) {
  neo4jDriver = neo4j.driver(NEO4J_URI, neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD));
}

// Ensure data dir exists
await mkdirIfNotExists(INGEST_BASE_DIR);

// Create pg client (used for upsert)
let pgClient = null;
if (DATABASE_URL) {
  pgClient = await createPgClient(DATABASE_URL);
}

// helpers for OpenCanada
async function listOpenCanadaDatasets() {
  const txt = await downloadToString(SOURCE_CONFIG.open_canada.list_url);
  const j = JSON.parse(txt);
  const arr = j.result || [];
  return arr.slice(0, 50);
}

async function fetchOpenCanadaPackage(packageId) {
  const url = SOURCE_CONFIG.open_canada.package_show_base + encodeURIComponent(packageId);
  const txt = await downloadToString(url);
  return JSON.parse(txt);
}

function extractResourcesFromPackage(pkgJson) {
  const result = pkgJson.result || {};
  const resources = result.resources || [];
  return resources.map((r) => ({
    id: r.id,
    url: r.url,
    format: (r.format || r.mimetype || "").toLowerCase(),
    name: r.name || r.id,
    last_modified: r.last_modified || r.metadata_modified || null
  }));
}

async function processResource(sourceKey, datasetId, datasetMeta, resource) {
  try {
    if (!resource.url) return { skipped: true };
    console.log("Downloading resource:", resource.url);
    const raw = await downloadToString(resource.url);

    const checksum = checksumString(raw);
    const safeName = `${datasetId}__${resource.id || resource.name}`.replace(/[^\w.-]/g, "_");
    const rawPath = path.join(INGEST_BASE_DIR, `${safeName}.raw`);
    const checksumFile = rawPath + ".sha256";
    let previousChecksum = null;
    try { previousChecksum = await fs.readFile(checksumFile, "utf-8"); } catch (e) {}

    if (previousChecksum === checksum) {
      console.log("No change detected, skipping:", resource.url);
      return { skipped: true };
    }

    await fs.writeFile(rawPath, raw, "utf-8");
    await fs.writeFile(checksumFile, checksum, "utf-8");

    const text = parseDocumentText(raw, resource.format, resource.url);
    const chunks = chunkText(text, CHUNK_WORDS, OVERLAP_WORDS).map(c => ({
      chunk_id: `${safeName}_chunk_${c.chunkIndex}`,
      chunk_index: c.chunkIndex,
      text: c.text,
      summary: c.summary,
      token_count: Math.max(1, Math.floor(c.text.split(/\s+/).length / 0.75)),
      citation_label: `[${datasetId} • ${resource.name} • chunk#${c.chunkIndex}]`
    }));

    // create embeddings (one by one; consider batching later)
    const vectors = [];
    for (const c of chunks) {
      const emb = await createOpenAIEmbedding(OPENAI_KEY, OPENAI_EMBED_MODEL, c.text);
      vectors.push({
        id: `${datasetId}:${resource.id}:chunk:${c.chunk_index}`,
        embedding: emb,
        metadata: {
          doc_id: `${datasetId}:${resource.id}`,
          chunk_id: c.chunk_id,
          source: sourceKey,
          file_url: resource.url,
          citation_label: c.citation_label
        }
      });
    }

    // Upsert to Postgres+pgvector
    if (pgClient) {
      await upsertPgVectors(pgClient, vectors, VECTOR_DIM);
      console.log("Upserted", vectors.length, "vectors into Postgres(pgvector).");
    } else {
      console.log("DATABASE_URL not set; skipping vector upsert.");
    }

    // create Neo4j nodes
    if (neo4jDriver) {
      const source = { source_id: sourceKey, name: SOURCE_CONFIG[sourceKey]?.name || sourceKey, base_url: SOURCE_CONFIG[sourceKey]?.list_url || ""};
      const dataset = {
        dataset_id: datasetId,
        title: datasetMeta?.result?.title || datasetId,
        description: datasetMeta?.result?.notes || ""
      };
      const document = {
        doc_id: `${datasetId}:${resource.id}`,
        title: resource.name || resource.id,
        file_url: resource.url,
        mime: resource.format || "",
        published_at: resource.last_modified || new Date().toISOString()
      };
      const chunkNodes = chunks.map(c => ({
        chunk_id: c.chunk_id,
        chunk_index: c.chunk_index,
        summary: c.summary,
        token_count: c.token_count,
        citation_label: c.citation_label
      }));
      await createNeo4jNodes(neo4jDriver, { source, dataset, document, chunks: chunkNodes });
      console.log("Neo4j nodes created for", document.doc_id);
    } else {
      console.log("Neo4j not configured; skipping node creation.");
    }

    return { ok: true, doc_id: `${datasetId}:${resource.id}`, chunks: chunks.length };
  } catch (err) {
    console.error("Error in processResource:", err);
    return { ok: false, error: String(err) };
  }
}

async function processOpenCanada() {
  console.log("Listing Open Canada datasets...");
  const ids = await listOpenCanadaDatasets();
  console.log("Found", ids.length, "datasets (limited).");
  for (const id of ids) {
    try {
      const pkg = await fetchOpenCanadaPackage(id);
      const resources = extractResourcesFromPackage(pkg);
      for (const r of resources) {
        const ext = (r.format || "").toLowerCase();
        if (["csv","xml","json","text",""].some(e => ext.includes(e) || r.url.endsWith(`.${e}`))) {
          const res = await processResource("open_canada", id, pkg, r);
          console.log("Resource process result:", res);
        } else {
          console.log("Skipping unsupported resource:", r.format, r.url);
        }
      }
    } catch (e) {
      console.error("Error processing dataset", id, e);
    }
  }
}

async function processStatCan() {
  console.log("StatCan ingestion placeholder. Add statcan listing logic here.");
}

// HTTP server to trigger ingestion
import http from "http";
const PORT = process.env.PORT || 3001;

async function runIngest(req) {
  const apiSecret = process.env.API_SECRET;
  if (apiSecret) {
    const key = (req.headers["x-api-key"] || req.headers["X-API-KEY"] || "");
    if (key !== apiSecret) {
      return new Response(JSON.stringify({ ok:false, error:"unauthorized" }), { status: 401, headers: { "content-type":"application/json" }});
    }
  }

  // Run OpenCanada ingestion now
  await processOpenCanada();
  // add processStatCan() when implemented
  return new Response(JSON.stringify({ ok:true, msg:"Ingest completed (OpenCanada partial)" }), { status:200, headers: { "content-type":"application/json" }});
}

const server = http.createServer(async (req, res) => {
  if (req.method === "POST" && (req.url === "/" || req.url === "/trigger" || req.url === "/ingest")) {
    try {
      const resp = await runIngest(req);
      const text = await resp.text();
      res.writeHead(resp.status || 200, { "Content-Type": "application/json" });
      res.end(text);
    } catch (e) {
      console.error("Ingest error:", e);
      res.writeHead(500, { "Content-Type":"application/json" });
      res.end(JSON.stringify({ ok:false, error:String(e) }));
    }
    return;
  }
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type":"application/json" });
    res.end(JSON.stringify({ ok:true }));
    return;
  }
  res.writeHead(404, { "Content-Type":"text/plain" });
  res.end("Not found");
});

server.listen(PORT, () => console.log("Ingest service listening on", PORT));
