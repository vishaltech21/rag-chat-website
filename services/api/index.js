// services/api/index.js
import express from "express";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

app.get("/health", (req, res) => res.json({ ok: true }));

app.post("/ingest-trigger", async (req, res) => {
  const secret = req.headers["x-api-key"] || req.query.api_key;
  if (secret !== process.env.API_SECRET) return res.status(401).json({ error: "unauthorized" });

  // For quick test, call the ingest service URL if provided, otherwise return ok.
  const ingestUrl = process.env.INGEST_SERVICE_URL;
  if (ingestUrl) {
    try {
      const r = await fetch(ingestUrl, { method: "POST" });
      const t = await r.text();
      return res.json({ triggered: true, ingest_status: r.status, ingest_text_length: t.length });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  return res.json({ triggered: true, note: "No INGEST_SERVICE_URL provided; please set env var" });
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`api listening on ${port}`));
