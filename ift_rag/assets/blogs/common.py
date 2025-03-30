import dagster as dg
import pandas as pd
import datetime
import requests
import os
from selenium.webdriver.common.by import By
from ...resources import Selenium, DeltaLake
from ... import constants
from bs4 import BeautifulSoup



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
        kinds=["BeautifulSoup", "Python"],
        group_name=f"{project_name.title()}_Extraction",
        owners=["team:Nikolay"],
        description=f"Extract the HTML text of the {project_name.title()} Blog pages.",
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
    def asset_template(context: dg.AssetExecutionContext, info: pd.DataFrame, delta_lake: DeltaLake) -> dg.Output:
        
        data = []
        for row in info.to_dict(orient="records"):
            
            response = requests.get(row["url"])
            context.log.info(f"Fetched data for {row['url']}")

            html = BeautifulSoup(response.text, "html.parser")

            row["text"] = html.find("section", class_="gh-content gh-canvas").text
            
            file_name = str(row["title"]).lower()

            for old, new in constants.SYMBOL_MAPPING.items():
                file_name = file_name.replace(old, new)
            
            file_name += ".pkl"
            delta_lake.upload(file_name, row)

            data.append(row)

        data = pd.DataFrame(data)

        metadata = {
            "preview": dg.MarkdownMetadataValue(data.assign(text = data["text"].apply(lambda text: text[:200])).head(5).to_markdown(index=False)),
            "articles": len(data)
        }
        return dg.Output(data, metadata=metadata)
    

    return asset_template