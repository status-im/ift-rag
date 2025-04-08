import dagster as dg
from ..configs import EmbeddingConfig
from ..resources import MinioResource, Qdrant
from llama_index.core.schema import TextNode
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from qdrant_client import models

@dg.asset(
    kinds=["LlamaIndex", "HuggingFace", "Minio"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Convert the chunkated 🦙 Documents into embeddings that will be stored in the vector store.",
    metadata={
        "embeddings": "https://huggingface.co/models?library=sentence-transformers"
    },
    deps=[
        "codex_blog_documents", "nomos_blog_documents", "waku_blog_documents",
        "nimbus_blog_documents", "status_app_blog_documents", "notion_markdown_documents"
    ]
)
def document_embeddings(context: dg.AssetExecutionContext, config: EmbeddingConfig, minio: MinioResource) -> dg.Output:
    
    embed_model = HuggingFaceEmbedding(
        model_name=config.model_name, 
        trust_remote_code=True
    )
    
    context.log.info(f"Embedding model: {config.model_name}")

    output = {
        "file_paths": config.file_paths,
        "qdrant_collection_name": config.model_name.replace("/", "-"),
        "embedding_size": len(embed_model.get_text_embedding("Get the embedding size for Qdrant"))  
    }

    for file_path in config.file_paths:

        documents: list[TextNode] = minio.load(file_path)
        embeded_documents = []

        for document in documents:
            
            if document.metadata.get("model") == config.model_name and len(document.embedding) > 0:
                continue

            document.embedding = embed_model.get_text_embedding(document.text)
            document.metadata["model"] = config.model_name
            document.metadata["path"] = f"archive/{file_path}"
            embeded_documents.append(document)

        if embeded_documents:
            minio.upload(embeded_documents, file_path)
            context.log.info(f"Added embeddings for {file_path}")

    metadata = {
        "model_name": config.model_name,
        "device": embed_model._device,
        "embedding_size": output["embedding_size"],
        "bucket": minio.bucket_name,
    }

    return dg.Output(output, metadata=metadata)



@dg.asset(
    kinds=["Qdrant"],
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Create a Qdrant collection for the embeddings.",
    ins={
        "info": dg.AssetIn("document_embeddings")
    },
    metadata={
        "vector_search": "https://qdrant.tech/articles/vector-search-resource-optimization/"
    }
)
def qdrant_collection(context: dg.AssetExecutionContext, info: dict, qdrant: Qdrant) -> dg.MaterializeResult:

    collection_name = info["qdrant_collection_name"]
    distance = models.Distance.COSINE

    client = qdrant.client
    context.log.info(collection_name)
    exists = client.collection_exists(collection_name)

    metadata = {
        "collection": collection_name,
        "created": str(not exists)
    }

    if exists:
        context.log.info(f"{collection_name} already exists")
        return dg.MaterializeResult(metadata=metadata)
    
    # https://qdrant.tech/documentation/guides/optimize/#3-high-precision-with-high-speed-search
    params = {
        "collection_name": collection_name,
        "vectors_config": models.VectorParams(size=info["embedding_size"], distance=distance),
        "quantization_config": models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(type=models.ScalarType.INT8, always_ram=True),
        )
    }
    client.create_collection(**params)

    metadata = {
        **metadata,
        "vector_distance": distance.value,
        "vector_size": str(info['embedding_size'])
    }
    context.log.info(f"Created {collection_name}\nVector size: {info['embedding_size']}\nDistance: {distance.value}")
    return dg.MaterializeResult(metadata=metadata)



@dg.asset(
    kinds=["LlamaIndex", "Qdrant", "Minio"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Upload the 🦙 Documents to Qdrant.",
    ins={
        "info": dg.AssetIn("document_embeddings")
    },
    deps=["qdrant_collection"]
)
def qdrant_vectors(context: dg.AssetExecutionContext, info: dict, qdrant: Qdrant) -> dg.MaterializeResult:
    pass



@dg.asset(
    kinds=["Minio"],
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Archive the chunkated files.",
    ins={
        "info": dg.AssetIn("document_embeddings")
    },
    deps=["qdrant_vectors"]
)
def processed_documents_files(context: dg.AssetExecutionContext, info: dict, minio: MinioResource) -> dg.MaterializeResult:

    context.log.info(f"{len(info['file_paths'])} files to archive")

    for source_path in info['file_paths']:
        minio.move(source_path, f"archive/{source_path}")