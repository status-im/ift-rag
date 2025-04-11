from dagster import define_asset_job, AssetSelection

blog_upload_job = define_asset_job(
    name="blog_upload_job",
    selection=AssetSelection.groups("Codex_Extraction", "Nomos_Extraction", "Waku_Extraction", "Nimbus_Extraction", "Status_Extractio")
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

notion_json_upload_job = define_asset_job(
    name="notion_json_upload_job",
    selection= AssetSelection.groups("Notion_Extraction") - AssetSelection.assets("notion_markdown_documents", "blog_documents")
)

notion_markdown_creation_job = define_asset_job(
    name="notion_markdown_creation_job",
    selection=AssetSelection.assets("notion_markdown_documents")
)