import dagster as dg
from pydantic import Field

class EmbeddingConfig(dg.Config):
    file_paths: list[str] = Field(description="The files that will be created into chunks")
    model_name: str = Field(default="nomic-ai/nomic-embed-text-v1.5", description="The embedding model name as it is in Hugging Face")



class NotionBlocksConfig(dg.Config):
    file_paths: list[str] = Field(description="The files that will be converted into markdown")
    skip_block_types: list[str] = Field(default=["image", "child_page"], description="The Notion block types that will be skipped when extracting the Markdown")
