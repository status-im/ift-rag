from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules, EnvVar
from .assets import blogs, preprocessing
from . import operations
from . import jobs
from . import schedules
from . import checks
from . import sensors
from . import resources

module_assets = load_assets_from_modules([
    blogs.status, blogs.nimbus, preprocessing
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
        jobs.logos_projects_upload_job, jobs.text_preprocessing_job
    ],
    sensors = [
        sensors.text_preprocessing_sensor
    ],
    schedules = [
        # Insert schedules here. Example schedules.your_schedule_name
    ],
    asset_checks = [*module_asset_checks],
    resources={
        "selenium": resources.Selenium(),
        "minio": resources.MinioResource(access_key=EnvVar("ACCESS_KEY"), secret_key=EnvVar("SECRET_KEY"), bucket_name="rag")
    }
)