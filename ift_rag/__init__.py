from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules, EnvVar
from .assets import blogs, preprocessing, notion
from . import operations
from . import jobs
from . import schedules
from . import checks
from . import sensors
from . import resources

module_assets = load_assets_from_modules([
    blogs.status, blogs.nimbus, preprocessing, notion
])

module_asset_checks = load_asset_checks_from_modules([
    # put asset check modules here
])

defs = Definitions(
    assets = [
        *module_assets, 
        blogs.common.make_blog_urls("waku"), blogs.common.make_blog_urls("codex"), blogs.common.make_blog_urls("nomos"),
        blogs.common.make_blog_text("waku"), blogs.common.make_blog_text("codex"), blogs.common.make_blog_text("nomos"),
    ],
    jobs = [
        jobs.logos_projects_upload_job, jobs.documents_preprocessing_job,
        jobs.notion_json_upload_job, jobs.notion_markdown_creation_job
    ],
    sensors = [
        sensors.notion_markdown_sensor, sensors.documents_preprocessing_sensor
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
        "qdrant": resources.Qdrant()
    }
)