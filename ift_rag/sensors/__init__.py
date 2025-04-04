import dagster as dg
import datetime
from .. import jobs
from ..resources import MinioResource

@dg.sensor(
    job=jobs.text_preprocessing_job
)
def text_preprocessing_sensor(minio: MinioResource):

    file_paths = [minio_object.object_name for minio_object in minio.client.list_objects("rag", recursive=True, prefix="html")]

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
        return dg.SkipReason(f"No files to process in {minio.bucket_name}/html")
    
    return dg.RunRequest(**params)



@dg.sensor(
    job=jobs.notion_markdown_creation_job
)
def notion_markdown_sensor(minio: MinioResource):

    file_paths = [minio_object.object_name for minio_object in minio.client.list_objects("rag", recursive=True, prefix="notion/json")]

    params = {
        "run_key": str(int(datetime.datetime.now().timestamp())),
        "run_config": {
            "ops": {
                "notion_markdown_data": {
                    "config": {
                        "file_paths": file_paths
                    }
                }
            }
        }
    }
    if not file_paths:
        return dg.SkipReason(f"No files to process in {minio.bucket_name}/html")
    
    return dg.RunRequest(**params)