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
  upsertPineconeVectors,
  createNeo4jNodes,
  mkdirIfNotExists
} from "helpers.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// --- CONFIG (from env vars) ---
const OPENAI_KEY = process.env.OPENAI_API_KEY;
const OPENAI_EMBED_MODEL = process.env.OPENAI_EMBED_MODEL || "text-embedding-3-small";
const PINECONE_URL = process.env.PINECONE_UPSERT_URL; // full upsert url
const PINECONE_API_KEY = process.env.PINECONE_API_KEY;
const PINECONE_NAMESPACE = process.env.PINECONE_NAMESPACE || "default";
const NEO4J_URI = process.env.NEO4J_URI;
const NEO4J_USER = process.env.NEO4J_USER;
const NEO4J_PASSWORD = process.env.NEO4J_PASSWORD;
const INGEST_BASE_DIR = process.env.INGEST_BASE_DIR || path.join(__dirname, "data");
const INGEST_SOURCES = (process.env.INGEST_SOURCES || "open_canada,statcan").split(",").map(s=>s.trim());
const INGEST_CRON = process.env.INGEST_CRON || "30 3 * * *";
const CHUNK_WORDS = parseInt(process.env.CHUNK_WORDS || "800");
const OVERLAP_WORDS = parseInt(process.env.OVERLAP_WORDS || "128");

// Sample endpoints for listing datasets — you can adjust these or expand
const SOURCE_CONFIG = {
  open_canada: {
    name: "OpenCanada",
    list_url: "https://open.canada.ca/data/en/api/3/action/package_list", // returns list of dataset ids
    package_show_base: "https://open.canada.ca/data/en/api/3/action/package_show?id=" // append id
  },
  statcan: {
    name: "StatCan",
    list_url: "https://www150.statcan.gc.ca/n1/en/dai/data.json", // placeholder - may need correct endpoint
    // For statcan we will use a fallback sample; you or I can add exact endpoints
  }
};

// Neo4j driver
let neo4jDriver = null;
if (NEO4J_URI && NEO4J_USER && NEO4J_PASSWORD) {
  neo4jDriver = neo4j.driver(NEO4J_URI, neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD));
}

// Ensure data dir exists
await mkdirIfNotExists(INGEST_BASE_DIR);

async function listOpenCanadaDatasets() {
  // package_list returns JSON { result: [ids...] }
  const txt = await downloadToString(SOURCE_CONFIG.open_canada.list_url);
  const j = JSON.parse(txt);
  const arr = j.result || [];
  return arr.slice(0, 50); // limit for initial run
}

async function fetchOpenCanadaPackage(packageId) {
  const url = SOURCE_CONFIG.open_canada.package_show_base + encodeURIComponent(packageId);
  const txt = await downloadToString(url);
  return JSON.parse(txt);
}

/** For StatCan we attempt to fetch a sample index; if the endpoint differs, you can change it */
async function listStatCanDatasets() {
  // This is a generic placeholder — many statcan endpoints need different usage.
  // For now, return an empty list to avoid crashing — replace with actual dataset listing API as needed.
  return [];
}

/** Download resources from a package metadata (OpenCanada) - returns array of resource objects */
function extractResourcesFromPackage(pkgJson) {
  const result = pkgJson.result || {};
  const resources = result.resources || [];
  // normalize
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
    console.log("Processing resource:", resource.url);
    const raw = await downloadToString(resource.url);
    const checksum = checksumString(raw);
    const fileNameSafe = `${datasetId}__${resource.id || resource.name}`.replace(/[^\w.-]/g, "_");
    const rawPath = path.join(INGEST_BASE_DIR, `${fileNameSafe}.raw`);
    // check existing checksum file
    const checksumFile = rawPath + ".sha256";
    let previousChecksum = null;
    try {
      previousChecksum = await fs.readFile(checksumFile, "utf-8");
    } catch (e) {}
    if (previousChecksum === checksum) {
      console.log("No change since last ingest, skipping:", resource.url);
      return { skipped: true };
    }
    // save raw
    await fs.writeFile(rawPath, raw, "utf-8");
    await fs.writeFile(checksumFile, checksum, "utf-8");

    // parse raw to text
    const text = parseDocumentText(raw, resource.format, resource.url);

    // chunk
    const chunks = chunkText(text, CHUNK_WORDS, OVERLAP_WORDS).map(c => ({
      chunk_id: `${fileNameSafe}_chunk_${c.chunkIndex}`,
      chunk_index: c.chunkIndex,
      text: c.text,
      summary: c.summary,
      token_count: Math.max(1, Math.floor(c.text.split(/\s+/).length / 0.75)) // rough token estimate
    }));

    // embeddings & upsert to Pinecone
    const vectors = [];
    for (const c of chunks) {
      const embedding = await createOpenAIEmbedding(OPENAI_KEY, OPENAI_EMBED_MODEL, c.text);
      vectors.push({
        id: `${datasetId}:${resource.id}:chunk:${c.chunk_index}`,
        values: embedding,
        metadata: {
          doc_id: `${datasetId}:${resource.id}`,
          chunk_id: c.chunk_id,
          source: sourceKey,
          file_url: resource.url,
          citation_label: `[${datasetId} • ${resource.name} • chunk#${c.chunk_index}]`
        }
      });
    }
    if (vectors.length && PINECONE_URL && PINECONE_API_KEY) {
      await upsertPineconeVectors(PINECONE_URL, PINECONE_API_KEY, vectors, PINECONE_NAMESPACE);
      console.log("Upserted", vectors.length, "vectors to Pinecone");
    } else {
      console.log("Pinecone config missing; vectors not upserted. Set PINECONE_UPSERT_URL and PINECONE_API_KEY to enable.");
    }

    // Neo4j nodes
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
        citation_label: `[${datasetId} • ${resource.name} • chunk#${c.chunk_index}]`
      }));
      await createNeo4jNodes(neo4jDriver, { source, dataset, document, chunks: chunkNodes });
      console.log("Neo4j nodes created for document:", document.doc_id);
    } else {
      console.log("Neo4j not configured; skipping node creation.");
    }

    return { ok: true, doc_id: `${datasetId}:${resource.id}`, chunks: chunks.length };
  } catch (err) {
    console.error("Error processing resource:", resource.url, err);
    return { ok: false, error: String(err) };
  }
}

async function processOpenCanada() {
  console.log("Listing Open Canada datasets...");
  const ids = await listOpenCanadaDatasets();
  console.log("Found", ids.length, "datasets (limited)");
  // for demo we process first N IDs
  for (const id of ids) {
    try {
      const pkg = await fetchOpenCanadaPackage(id);
      const resources = extractResourcesFromPackage(pkg);
      for (const r of resources) {
        // process only CSV/XML/JSON initially to keep runtime reasonable
        if (!r.url) continue;
        const ext = (r.format || "").toLowerCase();
        if (["csv", "xml", "json", "text", ""].some(e => ext.includes(e) || r.url.endsWith(`.${e}`))) {
          const res = await processResource("open_canada", id, pkg, r);
          console.log("Processed resource result:", res);
        } else {
          console.log("Skipping resource of unsupported format:", r.format, r.url);
        }
      }
    } catch (e) {
      console.error("Error processing dataset", id, e);
    }
  }
}

async function processStatCan() {
  console.log("StatCan ingestion not fully implemented in sample. Add statcan endpoint handling.");
  // Add statcan-specific listing + resource extraction here. Placeholder for now.
}

async function runIngest(req, res) {
  // this function can be called via HTTP POST to trigger ingestion
  try {
    // Basic protection via API key header optional
    const apiSecret = process.env.API_SECRET;
    if (apiSecret) {
      const key = (req.headers.get("x-api-key") || req.headers.get("X-API-KEY") || "");
      if (key !== apiSecret) {
        res = res || new Response();
        return new Response(JSON.stringify({ ok:false, error:"unauthorized" }), { status: 401, headers: { "content-type":"application/json" }});
      }
    }
    // For now run open canada ingestion only
    await processOpenCanada();
    // add processStatCan() when endpoint implemented
    return new Response(JSON.stringify({ ok: true, msg: "ingest completed (partial - statcan placeholder)" }), { status: 200, headers: { "content-type":"application/json" }});
  } catch (e) {
    console.error("Ingest failed", e);
    return new Response(JSON.stringify({ ok:false, error:String(e) }), { status: 500, headers: { "content-type":"application/json" }});
  }
}

// If run as HTTP server (Railway expects a web service), create a tiny server:
import http from "http";
const PORT = process.env.PORT || 3001;

const server = http.createServer(async (req, res) => {
  if (req.method === "POST" && (req.url === "/" || req.url === "/trigger" || req.url === "/ingest")) {
    // collect body if any (not necessary)
    const response = await runIngest(req, res);
    res.writeHead(response.status || 200, { "Content-Type": "application/json" });
    res.end(await response.text());
    return;
  }
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, env: { INGEST_CRON } }));
    return;
  }
  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not found");
});

server.listen(PORT, () => console.log("Ingest service listening on", PORT));
