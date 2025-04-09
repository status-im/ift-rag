import dagster as dg
import datetime
from .. import jobs
from ..resources import MinioResource



def minio_file_sensor_factory(sensor_name: str, minio_path_prefix: str, asset_name: str, job: dg.JobDefinition, minimum_interval_seconds: int = 30, auto_run: bool = False):

    @dg.sensor(
        job=job,
        name=sensor_name,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=dg.DefaultSensorStatus.RUNNING if auto_run else dg.DefaultSensorStatus.STOPPED
    )
    def sensor_template(minio: MinioResource):

        file_paths = [minio_object.object_name for minio_object in minio.client.list_objects(minio.bucket_name, recursive=True, prefix=minio_path_prefix)]

        params = {
            "run_key": str(int(datetime.datetime.now().timestamp())),
            "run_config": {
                "ops": {
                    asset_name: {
                        "config": {
                            "file_paths": file_paths
                        }
                    }
                }
            }
        }
        if not file_paths:
            return dg.SkipReason(f"No files to process in {minio.bucket_name}/{minio_path_prefix}")
        
        return dg.RunRequest(**params)
    
    return sensor_template