"""Local sentence-embedding model wrapper.

Everything runs on a LOCAL model — no paid embedding API. The default is a Swedish-tuned
768-dim model whose width matches the `cases.embedding vector(768)` column. The corpus
(ingest.py embed), candidate windows (enrich.py) and MCP queries (mcp_server.py) must all use
the same model, or similarity is meaningless.

The heavy `sentence_transformers` import is deferred to `Embedder.load()` so scripts that
don't embed never import torch.

Config (optional dict):
  model            HF model id (default 'KBLab/sentence-bert-swedish-cased', 768d)
  dimension        expected vector width; MUST equal the DB column (default 768)
  batch_size       encode batch size (default 32)
  device           'cpu' | 'cuda' | None (auto)
  passage_prefix   prepended to documents  (e5 models want 'passage: ')
  query_prefix     prepended to queries    (e5 models want 'query: ')
  max_chars        truncate input text before encoding (default 8000)
"""
from __future__ import annotations

import sys

DEFAULT_MODEL = "KBLab/sentence-bert-swedish-cased"
DEFAULT_DIMENSION = 768


class Embedder:
    def __init__(self, config: dict | None = None):
        cfg = config or {}
        self.model_name = cfg.get("model") or DEFAULT_MODEL
        self.dimension = int(cfg.get("dimension") or DEFAULT_DIMENSION)
        self.batch_size = int(cfg.get("batch_size") or 32)
        self.device = cfg.get("device")  # None -> sentence-transformers auto-selects
        self.passage_prefix = cfg.get("passage_prefix") or ""
        self.query_prefix = cfg.get("query_prefix") or ""
        self.max_chars = int(cfg.get("max_chars") or 8000)
        self._model = None

    def load(self) -> None:
        """Load the model once and verify it emits the configured dimension."""
        if self._model is not None:
            return
        from sentence_transformers import SentenceTransformer  # deferred (torch)

        # stderr: the MCP server speaks its protocol over stdout.
        print(f"  loading embedding model '{self.model_name}' (device={self.device or 'auto'})...", file=sys.stderr)
        self._model = SentenceTransformer(self.model_name, device=self.device)
        # method was renamed across sentence-transformers versions
        get_dim = getattr(self._model, "get_embedding_dimension", None) or \
            self._model.get_sentence_embedding_dimension
        got = get_dim()
        if got != self.dimension:
            raise ValueError(
                f"Model '{self.model_name}' emits {got}-dim vectors but config/DB "
                f"expect {self.dimension}. Set embeddings.dimension to {got} AND alter "
                f"the cases.embedding column to vector({got}) (rebuild the HNSW "
                f"index) so they match."
            )

    def _encode(self, texts: list[str], prefix: str) -> list[list[float]]:
        self.load()
        prepared = [prefix + (t or "")[: self.max_chars] for t in texts]
        vecs = self._model.encode(
            prepared,
            batch_size=self.batch_size,
            normalize_embeddings=True,  # unit vectors -> cosine == dot product
            show_progress_bar=False,
            convert_to_numpy=True,
        )
        return [v.tolist() for v in vecs]

    def embed_passages(self, texts: list[str]) -> list[list[float]]:
        """Embed corpus documents (case text)."""
        return self._encode(texts, self.passage_prefix)

    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        """Embed search queries and concept definitions."""
        return self._encode(texts, self.query_prefix)


def to_pgvector(vec: list[float]) -> str:
    """Render a vector in pgvector's text input form: '[v1,v2,...]'."""
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"
