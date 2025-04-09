import dagster as dg
import pprint
import os
import datetime
from ..configs import EmbeddingConfig, FileProcessingConfig
from ..resources import MinioResource, Qdrant
from llama_index.core.schema import TextNode
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core import Document
from llama_index.core.llms import MockLLM
from llama_index.core.node_parser import MarkdownElementNodeParser
from qdrant_client import models
from html_to_markdown import convert_to_markdown



@dg.asset(
    kinds=["Minio"],
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Convert the HTML blogs to Markdown.",
    deps=[
        "codex_blogs", "nomos_blogs", "waku_blogs",
        "nimbus_blogs", "status_app_blogs"
    ]
)
def blog_documents(config: FileProcessingConfig, minio: MinioResource) -> dg.MaterializeResult:
    
    for file_path in config.file_paths:
        
        document: Document = minio.load(file_path)

        params = {
            "source": document.text,
            "heading_style": "atx",
            "bullets": "-",
            "strong_em_symbol": "*"
        }

        document = Document(text=convert_to_markdown(**params), metadata=document.metadata)
        file_name = os.path.basename(file_path)
        project = os.path.basename(os.path.dirname(file_path))

        minio.upload(document, f"documents/markdown/blogs/{project}/{file_name}")
        minio.move(file_path, f"archive/{file_path}")



@dg.asset(
    kinds=["Minio", "LlamaIndex"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Convert the Markdown 🦙 Documents into chunks.",
    deps=["blog_documents", "notion_markdown_documents"]
)
def document_chunks(context: dg.AssetExecutionContext, config: FileProcessingConfig, minio: MinioResource) -> dg.MaterializeResult:
    
    # https://github.com/run-llama/llama_index/issues/16707
    parser = MarkdownElementNodeParser(llm=MockLLM())

    metadata = {
        "chunks": 0,
        "documents": len(config.file_paths),
    }
    for file_path in config.file_paths:
        
        document: Document = minio.load(file_path)

        chunks = parser.get_nodes_from_documents([document])
        
        path_parts = ["documents", "chunks"]
        source = document.metadata.get("source")
        if source:
            path_parts.append(source)

        project = document.metadata.get("project")
        if project:
            path_parts.append(project)
        
        path_parts.append(os.path.basename(file_path))

        context.log.info(f"Document {path_parts[-1]} has {len(chunks)} chunks")

        minio.upload(chunks, "/".join(path_parts))
        minio.move(file_path, f"archive/{file_path}")
        metadata["chunks"] += len(chunks)

    metadata = {key: str(value) for key, value in metadata.items()}
    return dg.MaterializeResult(metadata=metadata)



@dg.asset(
    kinds=["LlamaIndex", "HuggingFace", "Minio"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Convert the chunkated 🦙 Documents into embeddings that will be stored in the vector store.",
    metadata={
        "embeddings": "https://huggingface.co/models?library=sentence-transformers"
    },
    deps=["document_chunks"]
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
        total_chunks = len(documents)

        embeded_documents = []
        
        for index, document in enumerate(documents):
            
            embedding_exists = document.metadata.get("model") == config.model_name and len(document.embedding) > 0
            is_empty = document.text == "None"
            if embedding_exists or is_empty:
                continue

            document.embedding = embed_model.get_text_embedding(document.text)
            document.metadata["model"] = config.model_name
            document.metadata["path"] = f"archive/{file_path}"
            document.metadata["chunks"] = {
                "index": index,
                "total": total_chunks
            }
            document.metadata["upload_timestamp"] = datetime.datetime.now()
            embeded_documents.append(document)

        if not embeded_documents:
            context.log.debug(f"Embeddings for {file_path} exist")
            continue
        
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

    pp = pprint.PrettyPrinter(indent=2, sort_dicts=False)
    context.log.info(f"Collection parameters:\n{pp.pformat(params)}")
    
    metadata = {
        **metadata,
        "vector_distance": distance.value,
        "vector_size": str(info['embedding_size'])
    }
    context.log.info(f"Created {collection_name}\nVector size: {info['embedding_size']}\nDistance: {distance.value}")
    return dg.MaterializeResult(metadata=metadata)



@dg.asset(
    kinds=["Qdrant", "Minio"],
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Upload the 🦙 Chunks to Qdrant.",
    ins={
        "info": dg.AssetIn("document_embeddings")
    },
    deps=["qdrant_collection"]
)
def qdrant_vectors(context: dg.AssetExecutionContext, info: dict, qdrant: Qdrant, minio: MinioResource) -> dg.Output:

    metadata = {
        "batch_size": str(qdrant.batch_size),
        "batches": 1
    }    
    storage = []

    collection_name = info["qdrant_collection_name"]
    batch_size = qdrant.batch_size

    for file_path in info["file_paths"]:

        text_nodes: list[TextNode] = minio.load(file_path)        
        context.log.debug(f"Loaded {file_path}")

        points = [
            models.PointStruct(
                id=text_node.node_id, 
                vector=text_node.embedding, 
                payload={
                    **text_node.metadata,
                    "text": text_node.text
                }
            )
            for text_node in text_nodes
        ]
        context.log.debug(f"File contains {len(points)} Qdrant Points")

        storage += points
        if len(storage) < batch_size:
            continue
        
        batch = storage[:batch_size]

        qdrant.client.upsert(collection_name, batch)
        context.log.info(f"Batch {metadata['batches']}: Uploaded {len(batch)} vectors to {collection_name}")
        storage = storage[batch_size:]
        metadata['batches'] += 1


    if not storage:
        metadata["batches"] = str(metadata["batches"])
        return dg.Output(info, metadata=metadata)
    
    qdrant.client.upsert(collection_name, storage)
    metadata['batches'] += 1
    context.log.info(f"Uploaded remaining {len(storage)} vectors to {collection_name}")
    
    metadata["batches"] = str(metadata["batches"])
    return dg.Output(info, metadata=metadata)



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