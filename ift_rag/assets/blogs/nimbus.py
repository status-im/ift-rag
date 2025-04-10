import dagster as dg
import pandas as pd
import datetime
import requests
from selenium.webdriver.common.by import By
from ...resources import Selenium, MinioResource
from ... import constants
from bs4 import BeautifulSoup
from llama_index.core import Document

@dg.asset(
    metadata={
        "url": constants.URL["nimbus"]["blog"]
    },
    kinds=["BeautifulSoup", "Python"],
    group_name="Nimbus_Extraction",
    owners=["team:Nikolay"],
    description="Extract the Nimbus Blog topics.",
    tags={
        "blog": "",
        "scrape": "",
        "portfolio": "Nimbus"
    }
)
def nimbus_blog_topics(context: dg.AssetExecutionContext) -> dg.Output:

    url = constants.URL["nimbus"]["blog"]
    response = requests.get(url)
    context.log.info(f"Fetched {url}")

    html = BeautifulSoup(response.text, "html.parser")

    topics = pd.DataFrame([
        {
            "tag": a_tag.h3.text,
            "articles": int(a_tag.span.text.strip().split(" ")[0]),
            "url": url + a_tag["href"][1:]
        }
        for a_tag in html.find("aside").find("div", class_="gh-topic").find_all("a")
    ])

    metadata = {
        "preview": dg.MarkdownMetadataValue(topics.to_markdown(index=False))
    }

    return dg.Output(topics, metadata=metadata)



@dg.asset(
    kinds=["Selenium", "Python"],
    group_name="Nimbus_Extraction",
    owners=["team:Nikolay"],
    description="Extract the Nimbus Blog urls for every topic.",
    tags={
        "blog": "",
        "scrape": "",
        "portfolio": "Nimbus"
    },
    ins={
        "info": dg.AssetIn("nimbus_blog_topics")
    }
)
def nimbus_blog_urls(context: dg.AssetExecutionContext, info: pd.DataFrame, selenium: Selenium) -> dg.Output:

    article_tags_data = []

    for row in info.to_dict("records"):
        
        selenium.driver.get(row["url"])
        context.log.info(f"Loaded {row['url']}")

        xpath = "//button[text()='Load more articles']"
        count = 0
        while True:

            try:
                button = selenium.driver.find_element(By.XPATH, xpath)
                button.click()
                
                context.log.info(f"Clicked \"Load more articles\" {count} time{'s' if count > 1 else ''}")
                selenium.wait(By.XPATH, xpath)

                selenium.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                count += 1
            except:
                context.log.info(f"Button not found. Terminating loop")
                break
    
        article_tags_data += [
           {
                "title": article.find_element(By.TAG_NAME, "h2").text,
                "ref_date": datetime.datetime.strptime(article.find_element(By.TAG_NAME, "time").text, "%b %d, %Y"),
                "url": article.find_element(By.TAG_NAME, "a").get_attribute("href"),
                "tag": row["tag"]
            }
            for article in selenium.driver.find_elements(By.TAG_NAME, "article")
        ]


    posts = pd.DataFrame(article_tags_data)

    metadata = {
        "blogs": len(posts),
        "start_date": posts["ref_date"].min().strftime("%Y-%m-%d"),
        "end_date": posts["ref_date"].max().strftime("%Y-%m-%d"),
        "preview": dg.MarkdownMetadataValue(posts.head().to_markdown(index=False))
    }

    return dg.Output(posts, metadata=metadata)



@dg.asset(
    kinds=["LlamaIndex", "Python", "Minio"], # 🦙 is not allowed :/
    group_name="Nimbus_Extraction",
    owners=["team:Nikolay"],
    description="Extract the Nimbus Blog text for every topic.",
    tags={
        "blog": "",
        "scrape": "",
        "portfolio": "Nimbus"
    },
    metadata={
        "🦙Index": "https://docs.llamaindex.ai/en/stable/module_guides/loading/documents_and_nodes/",
    },
    ins={
        "info": dg.AssetIn("nimbus_new_urls")
    }
)
def nimbus_blogs(context: dg.AssetExecutionContext, info: pd.DataFrame, minio: MinioResource) -> dg.Output:

    for row in info.to_dict("records"):
        
        url: str = row["url"]
        response = requests.get(url)
        context.log.debug(f"Fetched data for {url}")
        
        html = BeautifulSoup(response.text, "html.parser")
        html_text = str(html.find("div", class_="gh-content gh-canvas"))
                    
        chunks_metadata = {
            **row,
            "project": "nimbus",
            "source": "blog"
        }

        document = Document(text=html_text, metadata=chunks_metadata)
        
        file_name = ("_".join(url.split("/")[-2:]) + ".pkl").replace("_.", ".")
        minio.upload(document, f"html/nimbus/{file_name}")
    
    metadata = {
        "bucket": minio.bucket_name,
        "documents": len(info),
    }
    return dg.MaterializeResult(metadata=metadata)