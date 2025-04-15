from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules, EnvVar
from .assets import blogs, preprocessing, notion, metadata, github_markdowns
from . import operations
from . import jobs
from . import schedules
from . import checks
from . import sensors
from . import resources

module_assets = load_assets_from_modules([
    blogs.status, blogs.nimbus, preprocessing, notion, metadata
])

module_asset_checks = load_asset_checks_from_modules([
    checks
])

defs = Definitions(
    assets = [
        *module_assets, 
        blogs.common.blog_urls_factory("waku"), blogs.common.blog_urls_factory("codex"), blogs.common.blog_urls_factory("nomos"),
        blogs.common.blog_text_factory("waku"), blogs.common.blog_text_factory("codex"), blogs.common.blog_text_factory("nomos"),
        metadata.metadata_factory("blog"), metadata.metadata_factory("notion"),
        blogs.common.uploaded_blog_metadata_factory("waku"), blogs.common.uploaded_blog_metadata_factory("codex"), blogs.common.uploaded_blog_metadata_factory("nomos"),
        blogs.common.uploaded_blog_metadata_factory("nimbus"), blogs.common.uploaded_blog_metadata_factory("status_app"), blogs.common.uploaded_blog_metadata_factory("status_network"),
        blogs.common.filtered_urls_factory("waku"), blogs.common.filtered_urls_factory("codex"), blogs.common.filtered_urls_factory("nomos"),
        blogs.common.filtered_urls_factory("nimbus"), blogs.common.filtered_urls_factory("status_app"), blogs.common.filtered_urls_factory("status_network"),
        github_markdowns.github_markdown_factory("contributors.free.technology", "develop", ".md"), github_markdowns.github_markdown_factory("infra-docs", "master", ".md")
    ],
    jobs = [
        jobs.blog_upload_job, jobs.text_embedding_job,
        jobs.notion_json_download_job, jobs.notion_markdown_creation_job,
        jobs.html_to_markdown_job, jobs.document_chunkation_job
    ],
    sensors = [
        sensors.minio_file_sensor_factory("text_embeddings_sensor", "document_embeddings", jobs.text_embedding_job, minio_path_prefix="documents/chunks"),
        sensors.minio_file_sensor_factory("html_to_markdown_sensor", "blog_documents", jobs.html_to_markdown_job, minio_path_prefix="html"),
        sensors.minio_file_sensor_factory("document_chunks_sensor", "document_chunks", jobs.document_chunkation_job, minio_path_prefix="documents/markdown"),
        sensors.minio_file_sensor_factory("notion_markdown_sensor", "notion_markdown_documents", jobs.notion_markdown_creation_job, folder_path=EnvVar("NOTION_JSON_PATH").get_value(), debug=True)
    ],
    schedules = [
        # Insert schedules here. Example schedules.your_schedule_name
    ],
    asset_checks = [*module_asset_checks],
    resources={
        "selenium": resources.Selenium(),
        "minio": resources.MinioResource(
            access_key=EnvVar("ACCESS_KEY"), 
            secret_key=EnvVar("SECRET_KEY"), 
            bucket_name="rag"
        ),
        "postgres": resources.Postgres(
            host=EnvVar("POSTGRES_HOST"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            port=EnvVar("POSTGRES_PORT"),
            database=EnvVar("POSTGRES_DATABASE")
        ),
        "notion": resources.Notion(
            api_key=EnvVar("NOTION_SECRET_KEY")
        ),
        "qdrant": resources.Qdrant(),
        "github": resources.GithubResource(token=EnvVar("GITHUB_TOKEN"))
    },
)