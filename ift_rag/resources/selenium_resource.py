import dagster as dg
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from typing import Optional

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