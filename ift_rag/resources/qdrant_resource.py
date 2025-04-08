import dagster as dg
from pydantic import Field
from typing import Optional
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

class Qdrant(dg.ConfigurableResource):

    host: str = Field(default="localhost", description="The Qdrant host name")
    port: int = Field(default=6333, description="The Qdrant's port number")
    __client: Optional[QdrantClient] = None

    @property
    def client(self) -> QdrantClient:
        if not self.__client:
            self.__client = QdrantClient(host=self.host, port=self.port,)
        return self.__client