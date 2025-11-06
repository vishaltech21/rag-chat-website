# Ingest Service (Bun / Node)

This service ingests datasets from Open Canada (and StatCan - placeholder), parses files, chunks text, creates OpenAI embeddings, upserts vectors to Pinecone, and creates Neo4j nodes.

## Recommended env vars (Railway Project → Variables)
- OPENAI_API_KEY
- OPENAI_EMBED_MODEL (default: text-embedding-3-small)
- PINECONE_UPSERT_URL (full upsert url e.g. https://<index>.<project>.<region>.pinecone.io/vectors/upsert)
- PINECONE_API_KEY
- PINECONE_NAMESPACE (optional)
- NEO4J_URI (bolt:// or neo4j+s://)
- NEO4J_USER
- NEO4J_PASSWORD
- INGEST_BASE_DIR (optional, default: ./data)
- INGEST_CRON (set to `30 3 * * *` for 09:00 IST daily)
- API_SECRET (optional - protect manual trigger)
- CHUNK_WORDS (optional, default 800)
- OVERLAP_WORDS (optional, default 128)

## How to run locally
1. `cd services/ingest`
2. `bun install` or `npm install`
3. set env vars (e.g. via `.env`) and run:
   `node index.js`
4. Trigger ingest with:
   `curl -X POST http://localhost:3001/trigger -H "x-api-key: <API_SECRET>"`

## Railway setup
- Add this folder as a Railway service (Root Directory = /services/ingest).
- Add env vars in Railway.
- Deploy once; after deploy add a Schedule to call POST `/` (or `/trigger`) daily at `30 3 * * *` (03:30 UTC = 09:00 IST).
- Reduce vCPU/memory after first deploy per project settings.

## Notes & next steps
- StatCan listing handling needs a specific endpoint; add its listing/parsing logic in processStatCan().
- The Pinecone upsert URL format varies: create an index in Pinecone, then use the appropriate upsert URL. Alternatively adapt to Pinecone SDK.
- This example stores raw files locally in `/services/ingest/data`. For S3, extend `saveRawFile()` to call AWS SDK (or presigned upload) if you prefer object storage.
- This script is designed as a minimal starting point — improve error handling, parallelism, backoff, and batching for production.
