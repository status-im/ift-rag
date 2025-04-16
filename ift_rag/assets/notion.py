import dagster as dg
from ..resources import Notion, MinioResource
from ..configs import NotionBlocksConfig
from ..utils import notion_parser
from .. import constants
from llama_index.core import Document
import pandas as pd
import numpy as np
import time
import traceback
import json
import os
import datetime

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
            "last_edited_by": page["last_edited_by"]["id"],
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
        "preview": dg.MarkdownMetadataValue(page_info.head(min(len(page_info), 10)).to_markdown(index=False)),
        "pages": len(page_info)
    }

    return dg.Output(page_info, metadata=metadata)


@dg.asset(
    kinds=["Pandas"],
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description=f"Split the pages into {constants.NOTION_PAGE_DOWNLOADERS} chunks so REST API requests can be done in parallel.",
    ins={
        "page_ids": dg.AssetIn("notion_page_ids")
    },
    tags={
        "notion": ""
    }
)
def notion_page_requests(context: dg.AssetExecutionContext, page_ids: pd.DataFrame, config: NotionBlocksConfig) -> dg.Output:

    os.makedirs(config.local_path, exist_ok=True)

    downloaded_ids = pd.Series(os.listdir(config.local_path)).str.replace(".json", "").to_list()
    query = ~page_ids["id"].isin(downloaded_ids)
    pages = page_ids.loc[query].assign(file_path=None).reset_index(drop=True).copy()

    if len(pages) > 0:
        pages["file_path"] = pages["id"].apply(lambda id: os.path.join(config.local_path, f"{id}.json"))

    chunks = np.array_split(pages, constants.NOTION_PAGE_DOWNLOADERS)
    
    metadata = {
        "total": str(len(page_ids)),
        "uploaded": str((~query).sum()),
        "to_do": str(len(pages)),
        "chunks": str(len(chunks)),
    }

    return dg.Output(chunks, metadata=metadata)



def notion_page_data_factory(number: int):
    @dg.asset(
        kinds=["Notion"],
        owners=["team:Nikolay"],
        group_name="Notion_Extraction",
        description="Get the Notion page data and upload it locally.",
        ins={
            "chunks": dg.AssetIn("notion_page_requests")
        },
        tags={
            "notion": ""
        },
        name=f"notion_page_json_{number}"
    )
    def asset_template(context: dg.AssetExecutionContext, chunks: list[pd.DataFrame], notion: Notion) -> dg.MaterializeResult:
        
        metadata = {
            "downloads": 0,
            "failed": 0,
        }

        pages = chunks[number-1].to_dict("records")
        context.log.info(f"To do: {len(pages)} IDs")

        for index, page in enumerate(pages):
            
            file_path = page.pop("file_path")

            page = {
                key: value if "time" not in key else str(value)
                for key, value in page.items()
            }
            try:
                data = notion.get_all_blocks(page['id'])
                context.log.info(f"{index}) Extracted data for {page['id']}")
                
                json_data = {
                    "metadata": page,
                    "data": data
                }
                with open(file_path, "w") as file:
                    json.dump(json_data, file, indent=4)

                context.log.info(f"Created {file_path}")
                metadata["downloads"] += 1
            except Exception as e:
                context.log.warning(f"There was an error with page Notion page ID - {id}\n\n{traceback.format_exc()}")
                metadata["failed"] += 1
            
            time.sleep(0.5)

        return dg.MaterializeResult(metadata=metadata)
    
    return asset_template



@dg.asset(
    kinds=["Python", "Minio", "LlamaIndex"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Notion_Extraction",
    description="Convert the Notion page text to Markdown and split it into chunks (🦙 Documents) that will be stored in the vector store",
    deps=[f"notion_page_json_{number}" for number in range(1, constants.NOTION_PAGE_DOWNLOADERS+1)],
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
            json_data: dict = json.load(file)
        
        data: list[dict] = json_data.get("data", [])
        metadata: dict = json_data.get("metadata", {})

        metadata = {
            key: datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if "time" in key else value 
            for key, value in metadata.items()
        }

        for index, document in enumerate(data):

            if document["type"] in config.skip_block_types:
                continue

            block = notion_parser.get_notion_block(document, document["type"])
            markdown.append(block.markdown_text)

            if len(block.children) != 0 and not isinstance(block, notion_parser.Table):
                markdown.append(notion_parser.get_child_markdown(block, ""))

        if not markdown:
            context.log.info(f"No Markdown for {file_path}")
            os.remove(file_path)
            continue
        
        archive_path = add_slash(config.archive_json_path) + file_name
        params = {
            "text": "".join(markdown),
            "metadata": {
                "path": archive_path,
                "source": "notion",
                **metadata
            }
        }

        document = Document(**params)

        path = add_slash(config.minio_markdown_path) + f"{page_id}.pkl"
        minio.upload(document, path)
        context.log.info(f"Uploaded {page_id} Markdown to {path}")

        minio.upload(data, archive_path)

        if not config.debug:
            os.remove(file_path)
            context.log.info(f"Archived file {page_id} - {archive_path}")
        

    metadata = {
        "bucket": minio.bucket_name,
        "files": len(config.file_paths)
    }

    return dg.MaterializeResult(metadata=metadata)