import dagster as dg
import pandas as pd
import datetime
import requests
from selenium.webdriver.common.by import By
from ...resources import Selenium
from ... import constants
from bs4 import BeautifulSoup

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

    # The heading article
    xpath = "//*[contains(concat(' ', normalize-space(@class), ' '), ' mb-[44px] ') and contains(concat(' ', normalize-space(@class), ' '), ' 2xl:mb-12 ')]"
    element = selenium.driver.find_element(By.XPATH, xpath)

    # Remainign articles
    xpath = '//div[@class="grid auto-rows-[1fr] grid-cols-[repeat(auto-fill,minmax(350px,1fr))] gap-5"]'
    links = selenium.driver.find_element(By.XPATH, xpath).find_elements(By.TAG_NAME, "a")
    links = [element] + links

    context.log.info(f"Found {len(links)} blogs")
    
    posts = []

    for blog_card in links:
    
        url = blog_card.get_attribute("href")
        blog_card = BeautifulSoup(blog_card.get_attribute("innerHTML"), "html.parser")

        lambda_document = {
            "tag": lambda: blog_card.find("span", class_="flex-1 whitespace-nowrap").text,
            "title": lambda: blog_card.find("span", class_="font-sans text-19 font-semibold").text,
            "ref_date": lambda: datetime.datetime.strptime(blog_card.find("span", class_="font-sans text-15 font-regular text-neutral-50 undefined").text, "on %b %d, %Y"),
            "url": lambda: url,
            "author": lambda: blog_card.find("span", class_="font-sans text-15 font-semibold").text
        }
        
        document = {}
        for key, func in lambda_document.items():
            
            try:
                document[key] = func()
            except:
                document[key] = None

        posts.append(document)
    
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
    kinds=["BeautifulSoup", "Python"],
    group_name="Status_Extraction",
    owners=["team:Nikolay"],
    description="Extract the HTML text of the Staus Blog pages.",
    tags={
        "blog": "",
        "scrape": "",
        "portfolio": "Status"
    },
    ins={
        "info": dg.AssetIn("status_app_blog_urls")
    }
)
def status_app_blog_text(context: dg.AssetExecutionContext, info: pd.DataFrame) -> list[dict]:
    
    data = []
    for row in info.to_dict(orient="records"):
        
        response = requests.get(row["url"])
        context.log.info(f"Fetched data for {row['url']}")

        html = BeautifulSoup(response.text, "html.parser")

        row["text"] = html.find("div", class_="root-content container-blog py-6").text
        data.append(row)

    return data