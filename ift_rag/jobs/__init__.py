from dagster import define_asset_job, AssetSelection, RunConfig, EnvVar
from ..configs import NotionBlocksConfig
from .. import constants

blog_upload_job = define_asset_job(
    name="blog_upload_job",
    selection=AssetSelection.groups("Codex_Extraction", "Nomos_Extraction", "Waku_Extraction", "Nimbus_Extraction", "Status_Extractio", "Status_Extraction")
)

text_embedding_job = define_asset_job(
    name="text_embedding_job",
    selection=AssetSelection.assets("document_embeddings")
)

html_to_markdown_job = define_asset_job(
    name="html_to_markdown_job",
    selection=AssetSelection.assets("blog_documents")
)

document_chunkation_job = define_asset_job(
    name="document_chunkation_job",
    selection=AssetSelection.assets("document_chunks")
)

notion_json_download_job = define_asset_job(
    name="notion_json_download_job",
    selection= AssetSelection.assets("notion_page_ids", "notion_page_requests", *[f"notion_page_json_{number}" for number in range(1, constants.NOTION_PAGE_DOWNLOADERS+1)]),
    config=RunConfig(ops={
        "notion_page_requests": NotionBlocksConfig(
            local_path=EnvVar("NOTION_JSON_PATH")
        )
    })
)

notion_markdown_creation_job = define_asset_job(
    name="notion_markdown_creation_job",
    selection=AssetSelection.assets("notion_markdown_documents")
)