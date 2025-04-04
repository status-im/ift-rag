import dagster as dg
import pandas as pd
import datetime
import requests
import os
from selenium.webdriver.common.by import By
from ...resources import Selenium, MinioResource
from ... import constants
from bs4 import BeautifulSoup
from llama_index.core import Document
from llama_index.core.node_parser import HTMLNodeParser


def make_blog_urls(project_name: str):

    @dg.asset(
        metadata={
            "url": constants.URL[project_name]["blog"]
        },
        kinds=["Selenium", "Python"],
        group_name=f"{project_name.title()}_Extraction",
        owners=["team:Nikolay"],
        description=f"Extract the {project_name.title()} Blog URLs.",
        tags={
            "blog": "",
            "scrape": "",
            "portfolio": project_name.title()
        },
        name=f"{project_name.lower()}_blog_urls"
    )
    def asset_template(context: dg.AssetExecutionContext, selenium: Selenium) -> dg.Output:

        url = constants.URL[project_name.lower()]["blog"]
        selenium.driver.get(url)

        selenium.wait(By.CLASS_NAME, "site-description")
        context.log.info(f"Loaded {url}")

        selenium.scroll_down()
        context.log.info(f"Requested all blogs")

        posts = pd.DataFrame([
            {
                "title": article.find_element(By.TAG_NAME, "h2").text,
                "ref_date": datetime.datetime.strptime(article.find_element(By.TAG_NAME, "time").get_attribute("datetime"), "%Y-%m-%d"),
                "url": article.find_element(By.TAG_NAME, "a").get_attribute("href"),
            } 
            for article in selenium.driver.find_elements(By.TAG_NAME, "article")
        ])

        selenium.driver.quit()

        metadata = {
            "blogs": len(posts),
            "start_date": posts["ref_date"].min().strftime("%Y-%m-%d"),
            "end_date": posts["ref_date"].max().strftime("%Y-%m-%d"),
            "preview": dg.MarkdownMetadataValue(posts.head().to_markdown(index=False))
        }

        return dg.Output(posts, metadata=metadata)
    
    return asset_template



def make_blog_text(project_name: str):

    @dg.asset(
        kinds=["BeautifulSoup", "Python", "Minio"],
        group_name=f"{project_name.title()}_Extraction",
        owners=["team:Nikolay"],
        description=f"Extract the HTML text of the {project_name.title()} Blog pages.",
        metadata={
            "🦙Index": "https://docs.llamaindex.ai/en/stable/module_guides/loading/documents_and_nodes/",
        },
        tags={
            "blog": "",
            "scrape": "",
            "portfolio": project_name.title()
        },
        ins={
            "info": dg.AssetIn(f"{project_name.lower()}_blog_urls")
        },
        name=f"{project_name.lower()}_blog_text"
    )
    def asset_template(context: dg.AssetExecutionContext, info: pd.DataFrame, minio: MinioResource) -> dg.MaterializeResult:
        
        for row in info.to_dict(orient="records"):
            
            response = requests.get(row["url"])
            context.log.info(f"Fetched data for {row['url']}")

            html = BeautifulSoup(response.text, "html.parser")
            html_text = "".join(list(map(str, html.find("section", class_="gh-content gh-canvas").children))).strip()
            
            row["text"] = " ".join([
                text_node.text.replace("\n", " ").strip()
                for text_node in HTMLNodeParser().get_nodes_from_documents([Document(text=html_text)]) 
                if not str(text_node.metadata["tag"]).startswith("h")
            ])

            file_name = str(row["title"]).lower() + ".pkl"
            minio.upload(row, f"html/{project_name.lower()}/{file_name}")

        metadata = {
            "bucket": minio.bucket_name,
            "uploaded": len(info)
        }
        return dg.MaterializeResult(metadata=metadata)
    
    return asset_template