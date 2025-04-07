import dagster as dg
from ..configs import EmbeddingConfig
from ..resources import MinioResource
from llama_index.core.schema import TextNode
from llama_index.embeddings.huggingface import HuggingFaceEmbedding

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

    embed_model = HuggingFaceEmbedding(model_name=config.model_name, trust_remote_code=True)
    context.log.info(f"Embedding model: {config.model_name}")

    for file_path in config.file_paths:

        documents: list[TextNode] = minio.load(file_path)

        embeded_documents = []
        for document in documents:
            document.embedding = embed_model.get_text_embedding(document.text)
            document.metadata["model"] = config.model_name
            embeded_documents.append(document)

        minio.upload(embeded_documents, file_path)
        context.log.info(f"Added embeddings for {file_path}")

    metadata = {
        "model_name": config.model_name,
        "bucket": minio.bucket_name,
    }

    return dg.Output(config.file_paths, metadata=metadata)




@dg.asset(
    kinds=["Minio"],
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Archive the chunkated files.",
    ins={
        "file_paths": dg.AssetIn("document_embeddings")
    }
)
def processed_documents_files(context: dg.AssetExecutionContext, file_paths: list[str], minio: MinioResource) -> dg.MaterializeResult:

    context.log.info(f"{len(file_paths)} files to archive")

    for source_path in file_paths:
        minio.move(source_path, f"archive/{source_path}")