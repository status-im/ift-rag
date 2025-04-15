import dagster as dg
from pydantic import Field
from typing import Optional

class EmbeddingConfig(dg.Config):
    file_paths: list[str] = Field(description="The files that will be created into chunks")
    model_name: str = Field(default="nomic-ai/nomic-embed-text-v1.5", description="The embedding model name as it is in Hugging Face")



class NotionBlocksConfig(dg.Config):
    local_path: Optional[str] = Field(description="The folder where JSON files will be saved", default=None)
    file_paths: list[str] = Field(description="The files that will be converted into markdown", default=[])
    debug: bool = Field(default=False, description="If True the the assets will be in debug mode. If False the assets will be in production mode")
    minio_markdown_path: str = Field(default="documents/markdown/notion/", description="The Minio path where Markdown files will be stored")
    archive_json_path: str = Field(default="archive/notion/json/", description="The Minio path where JSON files will be stored")
    skip_block_types: list[str] = Field(default=["image", "child_page"], description="The Notion block types that will be skipped when extracting the Markdown")



class FileProcessingConfig(dg.Config):
    file_paths: list[str] = Field(description="The files that will be processed by the asset")