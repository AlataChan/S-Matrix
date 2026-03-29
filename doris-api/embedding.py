"""
Embedding helpers for semantic query history retrieval.
"""
import hashlib
import math
import os
import re
from typing import List, Optional


class EmbeddingService:
    """Small embedding wrapper with a deterministic hashing fallback."""

    DEFAULT_MODEL_NAME = "BAAI/bge-small-zh-v1.5"

    def __init__(
        self,
        provider: Optional[str] = None,
        model_name: Optional[str] = None,
        dimension: Optional[int] = None,
    ):
        self.provider = provider or os.getenv("SMATRIX_EMBEDDING_PROVIDER", "hashing")
        self.model_name = model_name or os.getenv("SMATRIX_EMBEDDING_MODEL", self.DEFAULT_MODEL_NAME)
        self.dimension = int(dimension or os.getenv("SMATRIX_EMBEDDING_DIM", "512"))
        self._model = None

    def embed_text(self, text: str) -> List[float]:
        normalized = self._normalize_text(text)
        if not normalized:
            return [0.0] * self.dimension

        if self.provider == "sentence_transformers":
            try:
                return self._embed_with_sentence_transformers(normalized)
            except Exception:
                return self._embed_with_hashing(normalized)

        return self._embed_with_hashing(normalized)

    def to_doris_array_literal(self, vector: List[float]) -> str:
        return "[" + ", ".join(f"{value:.8f}" for value in vector) + "]"

    def _normalize_text(self, text: str) -> str:
        compact = re.sub(r"\s+", " ", (text or "").strip())
        return compact

    def _embed_with_sentence_transformers(self, text: str) -> List[float]:
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self.model_name)

        vector = self._model.encode(text, normalize_embeddings=True)
        return [float(value) for value in vector.tolist()]

    def _embed_with_hashing(self, text: str) -> List[float]:
        vector = [0.0] * self.dimension
        tokens = self._tokenize(text)

        for token in tokens:
            digest = hashlib.md5(token.encode("utf-8")).hexdigest()
            index = int(digest[:8], 16) % self.dimension
            sign = -1.0 if int(digest[8:10], 16) % 2 else 1.0
            weight = 1.5 if len(token) > 1 else 1.0
            vector[index] += sign * weight

        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0:
            return vector
        return [value / norm for value in vector]

    def _tokenize(self, text: str) -> List[str]:
        compact = re.sub(r"\s+", "", text)
        if len(compact) <= 1:
            return list(compact)

        chars = list(compact)
        bigrams = [compact[index : index + 2] for index in range(len(compact) - 1)]
        return chars + bigrams
