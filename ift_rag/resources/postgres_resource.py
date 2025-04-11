import psycopg2
import dagster as dg
import pandas as pd
from pydantic import Field
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB

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



    def insert(self, data: pd.DataFrame, table_name: str, schema: str, json_columns: Optional[list] = None):
        
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        engine = create_engine(self.url)

        data.columns = [column.lower() for column in data.columns]

        params = {
            "name": table_name,
            "con": engine,
            "schema": schema,
            "if_exists": "append",
            "index": False
        }
        if json_columns:
            params["dtype"] = {
                json_column: JSONB
                for json_column in json_columns
            }
        
        data.to_sql(**params)


    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
