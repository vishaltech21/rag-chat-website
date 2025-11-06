// services/ingest/index.js
import fetch from "node-fetch";

async function main() {
  try {
    const res = await fetch("https://open.canada.ca/data/en/api/3/action/package_list");
    const txt = await res.text();
    console.log("[ingest] fetched size:", txt.length);
    // TODO: parse CSV/XML, persist raw file, create chunks, call embeddings, upsert to vector DB, write to Neo4j
  } catch (err) {
    console.error("[ingest] error:", err);
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main();
}
