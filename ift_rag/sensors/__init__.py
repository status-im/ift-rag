import dagster as dg
import datetime
from .. import jobs
from ..resources import DeltaLake
from pathlib import Path

@dg.sensor(
    job=jobs.text_preprocessing_job
)
def text_preprocessing_sensor(delta_lake: DeltaLake):

    file_paths = [str(path) for path in Path(delta_lake.path).glob("*.pkl")]

    params = {
        "run_key": str(int(datetime.datetime.now().timestamp())),
        "run_config": {
            "ops": {
                "custom_data_chunks": {
                    "config": {
                        "file_paths": file_paths
                    }
                }
            }
        }
    }
    if not file_paths:
        return dg.SkipReason(f"No files to process in {delta_lake.path}")
    
    return dg.RunRequest(**params)