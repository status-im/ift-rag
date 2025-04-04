import psycopg2
import dagster as dg
import pandas as pd
from pydantic import Field
from typing import Optional

class Postgres(dg.ConfigurableResource):

    host: str = Field(description="The PostgreSQL host name")
    user: str = Field(description="The PostgreSQL username")
    password: str = Field(description="The PostgreSQL username's password")
    port: str = Field(description="The PostgreSQL host's port")
    database: str = Field(description="The PostgreSQL database")
    
    __conn: Optional[psycopg2.extensions.connection] = None
    __cursor: Optional[psycopg2.extensions.cursor] = None
    
    @property
    def connection(self) -> psycopg2.extensions.connection:
        params = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "port": self.port if isinstance(self.port, int) else int(self.port),
            "database": self.database
        }
        if not self.__conn:
            self.__conn = psycopg2.connect(**params)

        return self.__conn



    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        
        if not self.__cursor:
            self.__cursor = self.connection.cursor()
        
        return self.__cursor
    


    def execute(self, query: str):
        """
        Execute queries such as INSERT, UPDATE, DELETE etc.

        Parameters:
            -  `query` - the PostgreSQL query
        """
        self.cursor.execute(query)
        self.connection.commit()



    def to_pandas(self, query: str, batch_size: int = 50_000) -> pd.DataFrame:
        """
        Create a DataFrame from the given query

        Parameters:
            - `query` - the PostgreSQL query
            - `batch_size` - how many rows will be fetched at once
        
        Output:
            - DataFrame for the executed query
        """
        self.cursor.execute(query)
        columns = [column.name.upper() for column in self.cursor.description]
        chunks = []

        while True:

            rows = self.cursor.fetchmany(batch_size)

            if not rows:
                break
            
            chunks.append(pd.DataFrame(rows, columns=columns))

        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=columns)