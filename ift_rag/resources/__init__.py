import dagster as dg
import time
import os
import re
import platform
import pickle
import datetime
from minio import Minio
from minio.commonconfig import CopySource
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from typing import Optional, Any

class Selenium(dg.ConfigurableResource):

    timeout: int = 10
    headless: bool = True
    __driver: Optional[webdriver.Chrome] = None

    @property
    def driver(self) -> webdriver.Chrome:

        if self.__driver:
            return self.__driver

        
        options = Options() if self.headless else None
        if options:
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")

        
        self.__driver = webdriver.Chrome(options=options)

        return self.__driver
    

    def wait(self, by_literal: By, by_attribute_name: str):
        """
        Wait until the given `by_literal` and `by_path` are rendered on the screen

        Parameters:
            - `by_literal` - the Selenium By literal such as ID, XPATH, LINK_TEXT, PARTIAL_LINK_TEXT, NAME, TAG_NAME, CLASS_NAME, CSS_SELECTOR
            - `by_path` - the Selenium literal's attribute name
        """
        element = WebDriverWait(self.driver, self.timeout).until(
            EC.presence_of_element_located(
                (by_literal, by_attribute_name)
            )
        )


    def scroll_down(self):
        """
        Scroll down to the very bottom of the page.
        """
        get_height = lambda: self.driver.execute_script("return document.body.scrollHeight")
        scroll = lambda: self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        last_height: int = get_height()

        while True:
            
            scroll()
            time.sleep(1)

            new_height: int = get_height()
            if new_height == last_height:
                break

            last_height = new_height


    def get_xpath(self, tag_name: str, attribute_name: str, values: str) -> str:
        return f"//{tag_name}[" + " and ".join([f"contains(@{attribute_name}, '{name}')" for name in values.split(" ")]) + "]"
    


class MinioResource(dg.ConfigurableResource):

    endpoint: str = "localhost"    
    port: Optional[int] = 9000
    bucket_name: str
    access_key: str
    secret_key: str

    __client: Optional[Minio] = None
    __symbol_mappings: dict[str, str] = {
        " ": "_",
        "-": "_",
        "(": "",
        ")": "",
        "'": "",
        ".": "",
        "!": "",
        "?": "",
        "'": "",
        ",": " ",
        ":": "",
        "\"": "",
        "£": "GBP",
        "$": "USD",
        "%": "PCT",
        "^": "",
        "&": "AND",
        "*": "",
        "-": "_",
        "+": "",
        "-": "",
        "=": ""
    }

    @property
    def client(self) -> Minio:

        if not self.__client:
            url = f"{self.endpoint}:{self.port}" if self.port else self.endpoint
            self.__client = Minio(url, access_key = self.access_key, secret_key = self.secret_key, secure = False)
            
        return self.__client



    def upload(self, data: Any, file_path: str):
        """
        Upload a pikle file to Minio bucket

        Parameters:
            - `data` - the Python data that will be saved as a pickle file
            - `file_path` - the Minio location that the file will be stored
        """

        file_name = os.path.basename(file_path).lower()

        for old, new in self.__symbol_mappings.items():
            file_name = file_name.replace(old, new)

        file_name = re.sub(r'_+', '_', file_name)
        local_file_path = os.path.join(
            os.path.dirname(__file__), 
            str(datetime.datetime.now().timestamp()).replace(".", ""), 
            file_name
        )
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        with open(local_file_path , "wb") as file:
            pickle.dump(data, file)

        if platform.system() == "Windows":
            file_path = file_path.replace("\\", "/")

        self.client.fput_object(self.bucket_name, file_path, local_file_path)
        
        os.remove(local_file_path)
        os.rmdir(os.path.dirname(local_file_path))



    def load(self, file_path: str) -> Any:
        """
        Load a pikle file from a Minio bucket

        Parameters:
            - `file_path` - the Minio location that the file will be stored

        Output:
            - the Python data that will be saved as a pickle file
        """
        local_file_path = os.path.join(os.path.dirname(__file__), str(datetime.datetime.now().timestamp()).replace(".", ""), os.path.basename(file_path))
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        self.client.fget_object(self.bucket_name, file_path, local_file_path)

        with open(local_file_path, 'rb') as file:
            data = pickle.load(file)

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