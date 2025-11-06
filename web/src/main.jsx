import React from "react";
import { createRoot } from "react-dom/client";

function App() {
  return (
    <div style={{ padding: 20 }}>
      <h1>RAG Chat Demo</h1>
      <p><a href="/chat">/chat</a> | <a href="/docs">/docs</a> | <a href="/node">/node</a></p>
      <p>Frontend connected — replace with real UI later.</p>
    </div>
  );
}

createRoot(document.getElementById("root")).render(<App />);
