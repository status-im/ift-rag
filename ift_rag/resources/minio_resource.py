import dagster as dg
import os
import shutil
import re
import json
import platform
import pickle
import datetime
import random
from minio import Minio
from minio.commonconfig import CopySource
from typing import Optional, Any

class MinioResource(dg.ConfigurableResource):

    endpoint: str = "localhost"    
    port: Optional[int] = 9000
    bucket_name: str
    access_key: str
    secret_key: str

    __client: Optional[Minio] = None
    __symbol_mappings: dict[str, str] = {
        "\n": "_",
        "\t": "_",
        "|": "_",
        "/": "_",
        "#": "",
        "\\": "_",
        "-": "_",
        "(": "_",
        ")": "_",
        "'": "",
        ".": "",
        "!": "",
        "?": "",
        "'": "",
        "\"": "",
        ",": "_",
        ":": "",
        "£": "GBP",
        "$": "USD",
        "%": "PCT",
        "^": "",
        "&": "AND",
        "*": "",
        "+": "",
        "-": "_",
        "=": "",
        " ": "_",
    }

    __file_modes: dict[str, str] = {
        "json": "",
        "pkl": "b"
    }

    @property
    def client(self) -> Minio:

        if not self.__client:
            url = f"{self.endpoint}:{self.port}" if self.port else self.endpoint
            self.__client = Minio(url, access_key = self.access_key, secret_key = self.secret_key, secure = False)
            
        return self.__client



    def __get_file_open_mode(self, file_extension: str, mode: str = "w") -> str:
         """
         Get the file's `open` mode.

         Parameters:
            - `file_extension` - the file's extension

        Output:
            - the `open` mode
         """
         file_mode = self.__file_modes.get(file_extension)
         
         if isinstance(file_mode, type(None)):
            raise Exception(f"File extension \".{file_extension}\" was not recognized")
         
         return mode + file_mode



    def upload(self, data: Any, file_path: str):
        """
        Upload a pikle file to Minio bucket

        Parameters:
            - `data` - the Python data that will be saved as a pickle file
            - `file_path` - the Minio location that the file will be stored
        """

        file_name = os.path.basename(file_path).lower()
        
        file_extension = file_name.split(".")[-1]
        file_mode = self.__get_file_open_mode(file_extension)

        file_name = ".".join(file_name.split(".")[:-1])
        processed_file_name = file_name

        for old, new in self.__symbol_mappings.items():
            processed_file_name = processed_file_name.replace(old, new)

        processed_file_name = re.sub(r'_+', '_', processed_file_name)
        file_path = file_path.replace(file_name, processed_file_name)
        
        
        local_file_path = os.path.join(
            os.path.dirname(__file__), 
            f"{str(datetime.datetime.now().timestamp()).replace('.', '')}-{random.randrange(0, int(datetime.datetime.now().timestamp()))}",
            processed_file_name
        )
        if not local_file_path.endswith(file_extension):
            local_file_path += f".{file_extension}"

        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        with open(local_file_path , file_mode) as file:
            
            if local_file_path.endswith(".pkl"):
                pickle.dump(data, file)
            
            elif local_file_path.endswith(".json"):
                json.dump(data, file, indent=4)                
        
        if platform.system() == "Windows":
            file_path = file_path.replace("\\", "/")
    
        self.client.fput_object(self.bucket_name, file_path, local_file_path)
        
        os.remove(local_file_path)
        shutil.rmtree(os.path.dirname(local_file_path))



    def load(self, file_path: str) -> Any:
        """
        Load a pikle file from a Minio bucket

        Parameters:
            - `file_path` - the Minio location that the file will be stored

        Output:
            - the Python data that will be saved as a pickle file
        """
        file_name = os.path.basename(file_path)
        local_file_path = os.path.join(os.path.dirname(__file__), str(datetime.datetime.now().timestamp()).replace(".", ""), file_name)
        
        file_extension = file_name.split(".")[-1]
        file_mode = self.__get_file_open_mode(file_extension, "r")
        
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        self.client.fget_object(self.bucket_name, file_path, local_file_path)

        data = None
        with open(local_file_path, file_mode) as file:
            
            if local_file_path.endswith(".pkl"):
                data = pickle.load(file)
            
            elif local_file_path.endswith(".json"):
                data = json.load(file)
            
            else:
                raise Exception(f"File extension \".{local_file_path.split('.')[-1]}\" was not recognized")
            
        os.remove(local_file_path)
        os.rmdir(os.path.dirname(local_file_path))

        return data    



    def copy(self, source: str, destination: str):
        """
        Copy the source file to the given destination

        Parameters:
            - `source` - the source file path in Minio
            - `destination` - the destination where the `source` will be copied to
        """
        copy_source = CopySource(self.bucket_name, source)
        self.client.copy_object(self.bucket_name, destination, copy_source)

    

    def move(self, source: str, destination: str):
        """
        Move the source file to the given destination

        Parameters:
            - `source` - the source file path in Minio
            - `destination` - the destination where the `source` will be moved to
        """
        self.copy(source, destination)
        self.client.remove_object(self.bucket_name, source)



    def exists(self, file_path: str) -> bool:
        """
        Check if the given file exists.

        Parameters:
            - `file_path` - the Minio file location

        Output:
            - if True the file was found in Minio if False the file does not exist
        """
        exists = True
        try:
            self.client.stat_object(self.bucket_name, file_path)
        except:
            exists = False

        return exists