import dagster as dg
import pandas as pd
from ..configs import ChunkingConfig, EmbeddingConfig
from ..resources import MinioResource
from semantic_text_splitter import TextSplitter
from tokenizers import Tokenizer
from llama_index.core import Document
from llama_index.embeddings.huggingface import HuggingFaceEmbedding

@dg.asset(
    kinds=["LlamaIndex", "HuggingFace", "Minio"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Convert the text data into text chunks (🦙 Documents) that will be stored in the vector store. To chunk the data we use semantic chunking where a pre-trained tokenizer determines the chunk size.",
    metadata={
        "tokenizers": "https://huggingface.co/transformers/v3.3.1/pretrained_models.html",
        "🦙Index": "https://docs.llamaindex.ai/en/stable/module_guides/loading/documents_and_nodes/",
        "semantic_chunking": "https://github.com/FullStackRetrieval-com/RetrievalTutorials/blob/main/tutorials/LevelsOfTextSplitting/5_Levels_Of_Text_Splitting.ipynb"
    },
    deps = [
        "codex_blog_text", "nomos_blog_text", "waku_blog_text",
        "nimbus_blog_text", "status_app_blog_text"
    ]
)
def custom_data_chunks(context: dg.AssetExecutionContext, config: ChunkingConfig, minio: MinioResource) -> dg.Output:

    tokenizer = Tokenizer.from_pretrained(config.model_name)
    context.log.info(f"Loaded Pre Trained tokenizer - {config.model_name}")

    splitter = TextSplitter.from_huggingface_tokenizer(tokenizer, (config.min_tokens, config.max_tokens))
    context.log.info(f"Instantiated HuggingFace Tokenizer with token range {config.min_tokens}-{config.max_tokens}")
    
    chunkated_data = []
    for file_path in config.file_paths:

        data: dict = minio.load(file_path)
        context.log.info(f"Loaded {file_path}")

        text: str = data.pop("text")
                
        chunkated_data += [
            Document(text=chunk, metadata={
                "bucket": minio.bucket_name,
                "location": f"archive/{file_path}",
                "models": {
                    "chunks": config.model_name,
                },
                "min_tokens": config.min_tokens,
                "max_tokens": config.max_tokens,
                **data
            }) 
            for chunk in splitter.chunks(text)
        ]
    
    metadata = {
        "bucket": minio.bucket_name,
        "files": len(config.file_paths),
        "chunks": len(chunkated_data),
        "model_name": config.model_name,
        "min_size": str(config.min_tokens),
        "max_size": str(config.max_tokens)
    }
    
    output = (chunkated_data, config.file_paths)
    return dg.Output(output, metadata=metadata)



@dg.asset(
    kinds=["Minio"],
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Archive the chunkated files.",
    deps=["data_chunk_embeddings"]
)
def processed_rag_files(context: dg.AssetExecutionContext, custom_data_chunks: tuple[list[Document], list[str]], minio: MinioResource) -> dg.MaterializeResult:

    _, file_paths = custom_data_chunks

    context.log.info(f"{len(file_paths)} files to archive")

    for source_path in file_paths:
        minio.move(source_path, f"archive/{source_path}")



@dg.asset(
    kinds=["LlamaIndex", "HuggingFace"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Convert the chunkated 🦙 Documents into embeddings that will be stored in the vector store.",
    metadata={
        "embeddings": "https://huggingface.co/models?library=sentence-transformers"
    }
)
def data_chunk_embeddings(context: dg.AssetExecutionContext, custom_data_chunks: tuple[list[Document], list[str]], config: EmbeddingConfig) -> dg.Output:

    documents, _ = custom_data_chunks

    embed_model = HuggingFaceEmbedding(model_name=config.model_name)

    updated_documents = []
    for document in documents:

        document.embedding = embed_model.get_text_embedding(document.text)
        document.metadata["models"]["embedding"] = config.model_name
        updated_documents.append(document)

    metadata = {
        "model_name": config.model_name,
        "documents": len(updated_documents),
    }

    return dg.Output(documents, metadata=metadata)