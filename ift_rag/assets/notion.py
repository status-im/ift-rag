import dagster as dg
from ..resources import Notion, MinioResource
from ..configs import NotionBlocksConfig
from ..utils import notion_parser
from llama_index.core import Document
import pandas as pd
import traceback
import time
import json
import os

@dg.asset(
    kinds=["Notion"],
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description="Get the Notion page IDs.",
    tags={
        "notion": ""
    }
)
def notion_page_ids(context: dg.AssetExecutionContext, notion: Notion) -> dg.Output:

    pages = notion.get_pages()
    context.log.info(f"Found: {len(pages)} pages")
    page_info = pd.DataFrame([
        {
            "id": page["id"],
            "title": "".join([
                chunk["plain_text"] 
                for chunk in page["properties"].get("title", page["properties"].get("Name", {"title": []}))["title"]
            ]),
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
        "preview": dg.MarkdownMetadataValue(page_info.sample(10).to_markdown(index=False)),
        "pages": len(page_info)
    }

    return dg.Output(page_info, metadata=metadata)



@dg.asset(
    kinds=["Notion"],
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description="Get the Notion page data and upload it locally.",
    ins={
        "page_ids": dg.AssetIn("notion_page_ids")
    },
    tags={
        "notion": ""
    }
)
def notion_page_data(context: dg.AssetExecutionContext, page_ids: pd.DataFrame, config: NotionBlocksConfig, notion: Notion) -> dg.MaterializeResult:
    
    os.makedirs(config.local_path, exist_ok=True)

    metadata = {
        "downloads": 0,
        "failed": 0,
    }

    downloaded_ids = pd.Series(os.listdir(config.local_path)).str.replace(".json", "").to_list()

    query = ~page_ids["id"].isin(downloaded_ids)
    ids = page_ids.loc[query, "id"].to_list()
    context.log.info(f"To do: {len(ids)} IDs")

    for index, id in enumerate(ids):
        try:
            data = notion.get_all_blocks(id)
            context.log.info(f"{index}) Extracted data for {id}")

            file_path = os.path.join(config.local_path, f"{id}.json")
            with open(file_path, "w") as file:
                json.dump(data, file, indent=4)

            context.log.info(f"Created {file_path}")
            metadata["downloads"] += 1
        except Exception as e:
            context.log.warning(f"There was an error with page Notion page ID - {id}\n\n{traceback.format_exc()}")
            metadata["failed"] += 1
    
        time.sleep(1)

    return dg.MaterializeResult(metadata=metadata)



@dg.asset(
    kinds=["Python", "Minio", "LlamaIndex"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description="Convert the Notion page text to Markdown and split it into chunks (🦙 Documents) that will be stored in the vector store",
    deps=["notion_page_data"],
    metadata={
        "json": NotionBlocksConfig().archive_json_path,
        "markdown": NotionBlocksConfig().minio_markdown_path,
        "🦙Index": "https://github.com/run-llama/llama_index/discussions/13412"
    },
    tags={
        "notion": ""
    }
)
def notion_markdown_documents(context: dg.AssetExecutionContext, config: NotionBlocksConfig, minio: MinioResource) -> dg.MaterializeResult:
    
    add_slash = lambda path: str(path) if str(path).endswith("/") else str(path) + "/"

    for file_path in config.file_paths:
        
        file_name = os.path.basename(file_path)
        page_id = ".".join(file_name.split(".")[:-1]).replace("_", "-")

        markdown = []
        
        with open(file_path, "r") as file:
            data: list[dict] = json.load(file)
        
        for index, document in enumerate(data):

            if document["type"] in config.skip_block_types:
                continue

            block = notion_parser.get_notion_block(document, document["type"])
            markdown.append(block.markdown_text)

            if len(block.children) != 0 and not isinstance(block, notion_parser.Table):
                markdown.append(notion_parser.get_child_markdown(block, ""))

        archive_path = add_slash(config.archive_json_path) + file_name
        params = {
            "text": "".join(markdown),
            "metadata": {
                "path": archive_path,
                "page_id": page_id,
                "source": "notion"
            }
        }

        document = Document(**params)

        path = add_slash(config.minio_markdown_path) + f"{page_id}.pkl"
        minio.upload(document, path)
        context.log.info(f"Uploaded {page_id} Markdown to {path}")

        minio.upload(data, archive_path)
        os.remove(file_path)
        context.log.info(f"Archived file {page_id} - {archive_path}")
        

    metadata = {
        "bucket": minio.bucket_name,
        "files": len(config.file_paths)
    }

    return dg.MaterializeResult(metadata=metadata)