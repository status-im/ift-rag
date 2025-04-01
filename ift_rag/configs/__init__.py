import dagster as dg
from pydantic import Field
from typing import Union, Optional


class ChunkingConfig(dg.Config):
    file_paths: list[str] = Field(description="The files that will be created into chunks")
    model_name: str = Field(default="bert-base-uncased", description="The tokenizer's name as it is in Hugging Face")
    min_tokens: int = Field(default=1000, description="The minimum capacity of tokens in each chunk.")
    max_tokens: int = Field(default=1000, description="The maximum capacity of tokens in each chunk.")



class EmbeddingConfig(dg.Config):
    model_name: str = Field(default="nomic-ai/nomic-embed-text-v1.5", description="The embedding model name as it is in Hugging Face")