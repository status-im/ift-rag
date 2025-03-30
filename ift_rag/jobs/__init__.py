from dagster import define_asset_job, AssetSelection

logos_projects_upload_job = define_asset_job(
    name="logos_projects_upload_job",
    selection=AssetSelection.groups("Codex_Extraction", "Nomos_Extraction", "Waku_Extraction")
)

text_preprocessing_job = define_asset_job(
    name="text_preprocessing_job",
    selection=AssetSelection.assets("custom_data_chunks")
)