import dagster as dg
import pandas as pd
import json
from .. import constants
from ..configs import EmbeddingConfig
from ..resources import MinioResource, Qdrant, Postgres
from llama_index.core.schema import TextNode


@dg.asset(
    kinds=["Python", "Minio"],
    owners=["team:Nikolay"],
    group_name="Metadata_Upload",
    description="Create a metadata DataFrame that can be filtered and aggregated.",
    ins={
        "info": dg.AssetIn("qdrant_vectors")
    },
    deps=["processed_documents_files"]
)
def documents_metadata(context: dg.AssetExecutionContext, info: dict, minio: MinioResource) -> dg.Output:

    data = []
    file_paths = info.pop("file_paths")
    for file_path in file_paths:
        
        if not minio.exists(file_path):
            continue

        text_nodes: list[TextNode] = minio.load(file_path)

        data += [
            {
                "id": text_node.node_id,
                **{f"chunk_{key}": value for key, value in text_node.metadata.get("chunks", {}).items()},
                "source": text_node.metadata.get("source"),
                "path": text_node.metadata.get("path"),
                "model": text_node.metadata.get("model"),
                "upload_timestamp": text_node.metadata.get("upload_timestamp"),
            }
            for text_node in text_nodes
        ]

    df = pd.DataFrame(data)

    metadata = {
        "preview": dg.MarkdownMetadataValue(df.sample(10).to_markdown(index=False)),
        **df.groupby("source").count()["id"].astype(str).to_dict()
    }
    return dg.Output(df, metadata=metadata)



def metadata_factory(source: str):
    @dg.asset(
        kinds=["Pandas"],
        owners=["team:Nikolay"],
        group_name="Metadata_Upload",
        description=f"Get the necessary metadata for source {source}.",
        ins={
            "data": dg.AssetIn("documents_metadata")
        },
        tags={
            source: ""
        },
        name=f"{source}_documents_metadata"
    )
    def asset_template(data: pd.DataFrame) -> dg.Output:
        
        query = data["source"] == source

        selected = data.loc[query].reset_index(drop=True).copy()

        metadata = {
            "rows": str(query.sum()),
            "preview": dg.MarkdownMetadataValue(selected.sample(min(len(selected), 10)).to_markdown(index=False)),
        }

        return dg.Output(data.loc[query].reset_index(drop=True), metadata=metadata)
    
    return asset_template



@dg.asset(
    kinds=["Minio", "Python"],
    owners=["team:Nikolay"],
    group_name="Metadata_Upload",
    description=f"Get the metadata for source for the blogs.",
    ins={
        "data": dg.AssetIn("blog_documents_metadata")
    },
    tags={
        "blog": ""
    },
)
def blog_file_info(data: pd.DataFrame, minio: MinioResource) -> pd.DataFrame:

    file_paths = data["path"].unique().tolist()
    data = []

    for file_path in file_paths:
        
        text_nodes: list[TextNode] = minio.load(file_path)

        data += [
            {
                "id": text_node.node_id,
                "metadata": {
                    "url": text_node.metadata["url"],
                    "project": text_node.metadata["project"],
                    "title": text_node.metadata["title"]
                }
            }
            for text_node in text_nodes
        ]

    return pd.DataFrame(data)



@dg.asset(
    kinds=["Pandas"],
    owners=["team:Nikolay"],
    group_name="Metadata_Upload",
    description=f"Create the blog metadata for Postgres.",
    tags={
        "blog": ""
    }
)
def blog_metadata(context: dg.AssetExecutionContext, blog_file_info: pd.DataFrame, blog_documents_metadata: pd.DataFrame) -> pd.DataFrame:

    df = blog_documents_metadata.merge(blog_file_info, "left", "id")\
                                .reset_index(drop=True)
    
    return df



@dg.asset(
    kinds=["Pandas", "Postgres"],
    owners=["team:Nikolay"],
    group_name="Metadata_Upload",
    description=f"Get the new blog metadata that has not been uploaded.",
    tags={
        "blog": ""
    },
    metadata=constants.POSTGRES_INFO
)
def new_blog_metadata(context: dg.AssetExecutionContext, blog_metadata: pd.DataFrame, postgres: Postgres) -> dg.Output:
    
    ids = ", ".join(blog_metadata["id"].apply(lambda id: f"'{id}'").to_list())
    
    query = f"""
    SELECT ID 
    FROM {constants.POSTGRES_INFO['schema']}.{constants.POSTGRES_INFO['table_name']} 
    WHERE source = 'blog' 
    AND ID IN ({ids})
    """

    existing = postgres.to_pandas(query)
    
    existing_ids = existing["ID"].to_list() if len(existing) > 0 else []
    
    query = blog_metadata["id"].isin(existing_ids)
    new_data = blog_metadata.loc[~query].reset_index(drop=True).copy()

    metadata = {
        "existing": str(len(existing_ids)),
        "new": len(new_data)
    }

    return dg.Output(new_data, metadata=metadata)



@dg.asset(
    kinds=["Postgres"],
    owners=["team:Nikolay"],
    group_name="Metadata_Upload",
    description=f"Upload the new data to Postgres.",
    tags={
        "blog": ""
    },
    metadata=constants.POSTGRES_INFO
)
def postgres_blog_metadata(context: dg.AssetExecutionContext, new_blog_metadata: pd.DataFrame, postgres: Postgres) -> dg.MaterializeResult:
    
    metadata = {
        "uploaded": len(new_blog_metadata) > 0,
        "rows": len(new_blog_metadata)
    }
    if metadata["uploaded"]:
        postgres.insert(new_blog_metadata, **constants.POSTGRES_INFO, json_columns=["metadata"])
        context.log.info(f"Uploaded {len(new_blog_metadata)} rows to {constants.POSTGRES_INFO['schema']}.{constants.POSTGRES_INFO['table_name']}")

    return dg.MaterializeResult(metadata=metadata)