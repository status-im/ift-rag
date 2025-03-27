from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from .assets import status
from . import operations
from . import jobs
from . import schedules
from . import checks
from . import sensors
from . import resources

module_assets = load_assets_from_modules([
    status
])

module_asset_checks = load_asset_checks_from_modules([
    # put asset check modules here
])

defs = Definitions(
    assets = [*module_assets],
    jobs = [
        # Insert jobs here. Example jobs.your_job_name
    ],
    sensors = [
        # Insert sensors here. Example sensors.your_sensor_name
    ],
    schedules = [
        # Insert schedules here. Example schedules.your_schedule_name
    ],
    asset_checks = [*module_asset_checks],
    resources={
        "selenium": resources.Selenium()
    }
)
