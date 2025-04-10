import dagster as dg
import pandas as pd
import datetime
import requests
from selenium.webdriver.common.by import By
from ...resources import Selenium, MinioResource
from ... import constants
from bs4 import BeautifulSoup
from llama_index.core.node_parser import HTMLNodeParser
from llama_index.core import Document

@dg.asset(
    metadata={
        "url": constants.URL["status"]["blog"]
    },
    kinds=["Selenium", "Python"],
    group_name="Status_Extraction",
    owners=["team:Nikolay"],
    description="Extract the Status Blog URLs.",
    tags={
        "blog": "",
        "scrape": "",
        "portfolio": "Status"
    }
)
def status_app_blog_urls(context: dg.AssetExecutionContext, selenium: Selenium) -> dg.Output:
    
    url = constants.URL["status"]["blog"]
    selenium.driver.get(url)
    
    selenium.wait(By.CSS_SELECTOR, ".text-40.font-bold.xl\\:text-64")
    context.log.info(f"Loaded {url}")

    selenium.scroll_down()
    context.log.info(f"Requested all blogs")

    to_datetime = lambda text: datetime.datetime.strptime(text, "on %b %d, %Y")

    # The heading article
    xpath = "//*[contains(concat(' ', normalize-space(@class), ' '), ' mb-[44px] ') and contains(concat(' ', normalize-space(@class), ' '), ' 2xl:mb-12 ')]"
    element = selenium.driver.find_element(By.XPATH, xpath)
    html = BeautifulSoup(element.get_attribute("innerHTML"), "html.parser")

    posts = [{
        "tag": html.find("span", class_="flex-1 whitespace-nowrap").text,
        "title": html.find("span", class_="text-27 font-semibold xl:text-40 xl:font-bold").text,
        "ref_date": to_datetime(html.find("span", class_="font-sans text-15 font-regular text-neutral-50 undefined").text),
        "url": element.find_element(By.TAG_NAME, "a").get_attribute("href"),
        "author": html.find("span", class_="font-sans text-15 font-semibold").text
    }]
    
    # Remaining articles
    xpath = '//div[@class="grid auto-rows-[1fr] grid-cols-[repeat(auto-fill,minmax(350px,1fr))] gap-5"]'
    links = selenium.driver.find_element(By.XPATH, xpath).find_elements(By.TAG_NAME, "a")

    context.log.info(f"Found {len(links)} blogs")
    
    for blog_card in links:
    
        url = blog_card.get_attribute("href")
        blog_card = BeautifulSoup(blog_card.get_attribute("innerHTML"), "html.parser")

        lambda_document = {
            "tag": lambda: blog_card.find("span", class_="flex-1 whitespace-nowrap").text,
            "title": lambda: blog_card.find("span", class_="font-sans text-19 font-semibold").text,
            "ref_date": lambda: to_datetime(blog_card.find("span", class_="font-sans text-15 font-regular text-neutral-50 undefined").text),
            "url": lambda: url,
            "author": lambda: blog_card.find("span", class_="font-sans text-15 font-semibold").text
        }
        
        row = {}
        for key, func in lambda_document.items():
            
            try:
                row[key] = func()
            except:
                row[key] = None

        posts.append(row)
            
    posts = pd.DataFrame(posts)
    selenium.driver.quit()

    metadata = {
        "blogs": len(posts),
        "start_date": posts["ref_date"].min().strftime("%Y-%m-%d"),
        "end_date": posts["ref_date"].max().strftime("%Y-%m-%d"),
        "preview": dg.MarkdownMetadataValue(posts.head().to_markdown(index=False))
    }

    return dg.Output(posts, metadata=metadata)



@dg.asset(
    kinds=["LlamaIndex", "Python", "Minio"], # 🦙 is not allowed :/
    group_name="Status_Extraction",
    owners=["team:Nikolay"],
    description="Extract the HTML text of the Staus Blog pages.",
    tags={
        "blog": "",
        "scrape": "",
        "portfolio": "Status"
    },
    metadata={
        "🦙Index": "https://docs.llamaindex.ai/en/stable/module_guides/loading/documents_and_nodes/",
    },
    ins={
        "info": dg.AssetIn("status_app_blog_urls")
    }
)
def status_app_blogs(context: dg.AssetExecutionContext, info: pd.DataFrame, minio: MinioResource) -> dg.MaterializeResult:
    
    for row in info.to_dict(orient="records"):
        
        url: str = row["url"]
        response = requests.get(row["url"])
        context.log.debug(f"Fetched data for {row['url']}")

        html = BeautifulSoup(response.text, "html.parser")
        html_text = str(html.find("div", class_="root-content container-blog py-6"))

        chunks_metadata = {
            **row,
            "project": "status",
            "source": "blog"
        }
        
        document = Document(text=html_text, metadata=chunks_metadata)

        file_name = ("_".join(url.split("/")[-2:]) + ".pkl").replace("_.", ".")
        minio.upload(document, f"html/status/{file_name}")

    metadata = {
        "bucket": minio.bucket_name,
        "documents": len(info),
    }
    return dg.MaterializeResult(metadata=metadata)