from dagster import define_asset_job, AssetSelection

logos_projects_upload_job = define_asset_job(
    name="logos_projects_upload_job",
    selection=AssetSelection.groups("Codex_Extraction", "Nomos_Extraction", "Waku_Extraction")
)

documents_preprocessing_job = define_asset_job(
    name="documents_preprocessing_job",
    selection=AssetSelection.assets("document_embeddings")
)

notion_json_upload_job = define_asset_job(
    name="notion_json_upload_job",
    selection= AssetSelection.groups("Notion_Extraction") - AssetSelection.assets("notion_markdown_documents")
)

notion_markdown_creation_job = define_asset_job(
    name="notion_markdown_creation_job",
    selection=AssetSelection.assets("notion_markdown_documents")
)