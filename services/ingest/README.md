# Ingest Service (Postgres + pgvector)

This version writes OpenAI embeddings to Postgres using pgvector.

## Required env vars
- DATABASE_URL : Postgres connection string (Railway provided)
- OPENAI_API_KEY
- OPENAI_EMBED_MODEL (default: text-embedding-3-small)
- VECTOR_DIM (1536 for text-embedding-3-small)
- NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD (optional, for Neo4j)
- INGEST_BASE_DIR (optional)
- API_SECRET (optional)
- CHUNK_WORDS (optional, default 800)
- OVERLAP_WORDS (optional, default 128)

## Setup steps
1. Deploy PostgreSQL in Railway (or provide DB), note `DATABASE_URL`.
2. Connect to the DB and run `migrations.sql` (enable pgvector and create table).
   - Use Railway SQL console or psql: `psql $DATABASE_URL -f migrations.sql`
3. Add env vars to Railway project.
4. Deploy service to Railway (Root Directory = /services/ingest).
5. Create a schedule in Railway: POST `/` with cron `30 3 * * *` (03:30 UTC = 09:00 IST).
6. Test via:
   `curl -X POST "https://<ingest-service-url>/" -H "x-api-key: <API_SECRET>"`

## Notes
- The ingest currently implements OpenCanada listing. StatCan listing is a placeholder — I can implement it next.
- Embeddings are created sequentially here; consider batching for speed/cost.
- Use pgvector operators for retrieval:
  - Cosine distance: `<=>`
  - Euclidean: `<->`
