import dagster as dg
from ..resources import Notion, MinioResource
from ..configs import NotionBlocksConfig
from ..utils import notion_parser
from llama_index.core import Document
from llama_index.core.llms import MockLLM
from llama_index.core.node_parser import MarkdownElementNodeParser
import pandas as pd
import os

@dg.asset(
    kinds=["Notion"],
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description="Get the Notion page IDs."
)
def notion_page_ids(context: dg.AssetExecutionContext, notion: Notion) -> dg.Output:

    pages = notion.get_pages()
    context.log.info(f"Found: {len(pages)} pages")
    page_info = pd.DataFrame([
        {
            "id": page["id"],
            "title": "".join([chunk["plain_text"] for chunk in page["properties"]["title"]["title"]]),
            "created_time": page["created_time"],
            "last_edited_time": page["last_edited_time"],
            "url": page["url"],
            "archived": page["archived"],
        }
        for page in pages
    ])

    format = "%Y-%m-%dT%H:%M:%S.%fZ"
    page_info = page_info.assign(
        created_time = pd.to_datetime(page_info["created_time"], format=format),
        last_edited_time = pd.to_datetime(page_info["last_edited_time"], format=format),
    )

    metadata = {
        "preview": dg.MarkdownMetadataValue(page_info.head(10).to_markdown(index=False)),
        "pages": len(page_info)
    }

    return dg.Output(page_info, metadata=metadata)



@dg.asset(
    kinds=["Notion"],
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description="Get the Notion page data.",
    ins={
        "page_ids": dg.AssetIn("notion_page_ids")
    }
)
def notion_page_data(page_ids: pd.DataFrame, notion: Notion) -> dg.Output:

    page_data = {
        id: notion.get_all_blocks(id)
        for id in page_ids["id"].to_list()
    }

    metadata = {
        page["title"]: f"{len(page_data[page['id']])} block(s)" 
        for page in page_ids.to_dict("records")
    }

    return dg.Output(page_data, metadata=metadata)



@dg.asset(
    kinds=["Minio"],
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description="Upload the Notion JSON data to Minio.",
)
def notion_page_json(context: dg.AssetExecutionContext, notion_page_data: dict[str, list[dict]], minio: MinioResource) -> dg.MaterializeResult:

    for page_id, page_data in notion_page_data.items():
        file_path = f"notion/json/{page_id}.json"        
        minio.upload(page_data, file_path)



@dg.asset(
    kinds=["Python", "Minio", "LlamaIndex"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description="Convert the Notion page text to Markdown and split it into chunks (🦙 Documents) that will be stored in the vector store",
    deps=["notion_page_json"],
    metadata={
        "minio_folder": "documents/notion/markdown/",
        "🦙Index": "https://github.com/run-llama/llama_index/discussions/13412"
    }
)
def notion_markdown_documents(context: dg.AssetExecutionContext, config: NotionBlocksConfig, minio: MinioResource) -> dg.MaterializeResult:
    
    minio_folder = "documents/notion/markdown/"

    for file_path in config.file_paths:
        
        file_name = os.path.basename(file_path)
        page_id = ".".join(file_name.split(".")[:-1]).replace("_", "-")

        markdown = []
        data: list[dict] = minio.load(file_path)

        for index, document in enumerate(data):

            if document["type"] in config.skip_block_types:
                continue

            block = notion_parser.get_notion_block(document, document["type"])
            markdown.append(block.markdown_text)

            if len(block.children) != 0 and not isinstance(block, notion_parser.Table):
                markdown.append(notion_parser.get_child_markdown(block, ""))

        # https://github.com/run-llama/llama_index/issues/16707
        parser = MarkdownElementNodeParser(llm=MockLLM())

        archive_path = "archive/" + file_path
        params = {
            "text": "".join(markdown),
            "metadata": {
                "path": archive_path,
                "page_id": page_id,
                "parser": parser.class_name(),
                "source": "notion"
            }
        }
        
        page_chunks = parser.get_nodes_from_documents([Document(**params)])
        minio.upload(page_chunks, f"{minio_folder}{page_id}.pkl")
        context.log.info(f"There are {len(page_chunks)} Documents for page {page_id}")

        minio.move(file_path, archive_path)
        context.log.info(f"Moved file {os.path.basename(file_path)} from {os.path.dirname(file_path)} to {os.path.dirname(archive_path)}")

    metadata = {
        "bucket": minio.bucket_name,
        "files": len(config.file_paths)
    }

    return dg.MaterializeResult(metadata=metadata)