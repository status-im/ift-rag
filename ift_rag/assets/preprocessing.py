import dagster as dg
import pandas as pd
import datetime
from ..configs import ChunkingConfig
from ..resources import DeltaLake
from semantic_text_splitter import TextSplitter
from tokenizers import Tokenizer
from llama_index.core import Document

@dg.asset(
    kinds=["Python", "HuggingFace", "LlamaIndex"], # 🦙 is not allowed :/
    owners=["team:Nikolay"],
    group_name="Data_Retreiver",
    description="Convert the text data into text chunks (🦙 Documents) that will be stored in the vector store. To chunk the data we use semantic chunking where a pre-trained tokenizer determines the chunk size.",
    metadata={
        "tokenizers": "https://huggingface.co/transformers/v3.3.1/pretrained_models.html",
        "🦙Index": "https://docs.llamaindex.ai/en/stable/module_guides/loading/documents_and_nodes/",
        "semantic_chunking": "https://github.com/FullStackRetrieval-com/RetrievalTutorials/blob/main/tutorials/LevelsOfTextSplitting/5_Levels_Of_Text_Splitting.ipynb"
    }
)
def custom_data_chunks(context: dg.AssetExecutionContext, config: ChunkingConfig, delta_lake: DeltaLake) -> dg.Output:

    tokenizer = Tokenizer.from_pretrained(config.model_name)
    context.log.info(f"Loaded Pre Trained tokenizer - {config.model_name}")

    splitter = TextSplitter.from_huggingface_tokenizer(tokenizer, (config.min_tokens, config.max_tokens))
    context.log.info(f"Instantiated HuggingFace Tokenizer with token range {config.min_tokens}-{config.max_tokens}")
    
    chunkated_data = []
    for file_path in config.file_paths:

        data: dict = delta_lake.load(file_path)
        context.log.info(f"Loaded {file_path}")

        text: str = data.pop("text")
                
        chunkated_data += [
            Document(text=chunk, metadata={
                "model_name": config.model_name,
                "min_tokens": config.min_tokens,
                "max_tokens": config.max_tokens,
                **data
            }) 
            for chunk in splitter.chunks(text)
        ]
    
    metadata = {
        "chunks": len(chunkated_data),
        "model_name": config.model_name,
        "min_size": str(config.min_tokens),
        "max_size": str(config.max_tokens)
    }
    
    return dg.Output(chunkated_data, metadata=metadata)