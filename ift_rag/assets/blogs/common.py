import dagster as dg
import pandas as pd
import datetime
import requests
import os
from selenium.webdriver.common.by import By
from ...resources import Selenium, MinioResource, Postgres
from ... import constants
from bs4 import BeautifulSoup
from llama_index.core import Document


def uploaded_blog_metadata_factory(project_name: str):

    metadata_name = project_name.title() if "_" not in project_name else project_name.split("_")[0].title()

    @dg.asset(
        metadata=constants.POSTGRES_INFO,
        kinds=["Postgres"],
        group_name=f"{metadata_name}_Extraction",
        owners=["team:Nikolay"],
        description=f"Get the {metadata_name} Blog URLs that have already been uploaded to Qdrant.",
        tags={
            "blog": "",
            "portfolio": metadata_name
        },
        name=f"{project_name.lower()}_uploaded_metadata"
    )
    def asset_template(context: dg.AssetExecutionContext, postgres: Postgres) -> dg.Output:
        
        table_path = f"{constants.POSTGRES_INFO['schema']}.{constants.POSTGRES_INFO['table_name']}"
        query = f"""
        select distinct
            metadata->>'url' as url
        from {table_path}
        where source = 'blog'
        and metadata->>'project' = '{project_name}'
        """

        context.log.info(f"Executing:\n{query}")

        urls = []
        try:
            urls = postgres.to_pandas(query)["URL"].to_list()
        except:
            context.log.warning(f"No URLs found for {project_name} in {table_path} or {table_path} does not exist")

        metadata = {
            "urls": len(urls),
            "query": query
        }

        return dg.Output(urls, metadata=metadata)

    return asset_template




def filtered_urls_factory(project_name: str):

    metadata_name = project_name.title() if "_" not in project_name else project_name.split("_")[0].title()

    @dg.asset(
        metadata=constants.POSTGRES_INFO,
        kinds=["Pandas"],
        group_name=f"{metadata_name}_Extraction",
        owners=["team:Nikolay"],
        description=f"Filter the {metadata_name} Blog URLs that have not been uploaded to Qdrant and Postgres.",
        tags={
            "blog": "",
            "portfolio": metadata_name
        },
        ins={
            "blog_info": dg.AssetIn(f"{project_name.lower()}_blog_urls"),
            "uploaded": dg.AssetIn(f"{project_name.lower()}_uploaded_metadata")
        },
        name=f"{project_name.lower()}_new_urls"
    )
    def asset_template(blog_info: pd.DataFrame, uploaded: list) -> dg.Output:
        
        query = blog_info["url"].isin(uploaded)

        metadata = {
            "new": str((~query).sum()),
            "found": str(query.sum())
        }
        return dg.Output(blog_info.loc[~query], metadata=metadata)

    return asset_template



def blog_urls_factory(project_name: str):

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



def blog_text_factory(project_name: str):

    @dg.asset(
        kinds=["LlamaIndex", "Python", "Minio"], # 🦙 is not allowed :/
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
            "info": dg.AssetIn(f"{project_name.lower()}_new_urls")
        },
        name=f"{project_name.lower()}_blogs"
    )
    def asset_template(context: dg.AssetExecutionContext, info: pd.DataFrame, minio: MinioResource) -> dg.MaterializeResult:
        
        for row in info.to_dict(orient="records"):
            
            url: str = row["url"]
            response = requests.get(url)
            context.log.debug(f"Fetched data for {url}")

            html = BeautifulSoup(response.text, "html.parser")
            html_text = str(html.find("section", class_="gh-content gh-canvas"))
            
            chunks_metadata = {
                **row,
                "project": project_name.lower(), 
                "source": "blog"
            }

            document = Document(text=html_text, metadata=chunks_metadata)
            
            file_name = ("_".join(url.split("/")[-2:]) + ".pkl").replace("_.", ".")
            minio.upload(document, f"html/{project_name.lower()}/{file_name}")

        metadata = {
            "bucket": minio.bucket_name,
            "documents": len(info)
        }
        return dg.MaterializeResult(metadata=metadata)
    
    return asset_template