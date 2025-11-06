// services/ingest/index.js
// ESM-friendly ingestion skeleton (uses global fetch available in Node 18+)

async function main() {
  try {
    const res = await fetch("https://open.canada.ca/data/en/api/3/action/package_list");
    const txt = await res.text();
    console.log("[ingest] fetched size:", txt.length);
    // TODO: parse CSV/XML, persist raw file, chunk text, call embeddings, upsert to vector DB, write to Neo4j
  } catch (err) {
    console.error("[ingest] error:", err);
    process.exitCode = 1;
  }
}

// call main when this file is executed directly
// (no require.main in ESM; just run main())
main();
