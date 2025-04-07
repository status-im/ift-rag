import dagster as dg
import datetime
from .. import jobs
from ..resources import MinioResource

@dg.sensor(
    job=jobs.documents_preprocessing_job
)
def documents_preprocessing_sensor(minio: MinioResource):

    file_paths = [minio_object.object_name for minio_object in minio.client.list_objects("rag", recursive=True, prefix="documents")]

    params = {
        "run_key": str(int(datetime.datetime.now().timestamp())),
        "run_config": {
            "ops": {
                "document_embeddings": {
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
                "notion_markdown_documents": {
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