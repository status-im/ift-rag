import dagster as dg
import logging
from notion_client import Client
from typing import Optional

class Notion(dg.ConfigurableResource):

    api_key: str
    __client: Optional[Client] = None

    @property
    def client(self) -> Client:
        if not self.__client:
            self.__client = Client(
                auth=self.api_key, 
                log_level=logging.DEBUG,
                logger=dg.get_dagster_logger()
            )

        return self.__client

    
    def get_pages(self) -> list[dict]:
        """
        Get all of the pages that the `api_key` has access to

        Output:
            - the raw page data
        """
        response: dict = self.client.search(
            query="",
            filter={"property": "object", "value": "page"}
        )
        
        return response.get("results", [])



    def get_all_blocks(self, block_id: str) -> list[dict]:
        """
        Recursively retrieve all blocks (and nested child blocks) for a given block_id.
        
        Parameters:
            - `block_id` - the block / page ID

        Output:
            - The blocks
        """

        def get_children(block_id) -> list[dict]:
            """
            Get all direct child blocks for the given `block_id`.

            Parameters:
                - `block_id` - the block / page ID
            
            Output:
                - the children of the `block_id`
            """
            children = []
            next_cursor = None
            while True:
                response: dict = self.client.blocks.children.list(
                    block_id=block_id,
                    start_cursor=next_cursor
                )
                children.extend(response.get("results", []))
                if response.get("has_more"):
                    next_cursor = response.get("next_cursor")
                else:
                    break
            
            return children
        
        blocks = get_children(block_id)

        for block in blocks:
            
            if block.get("has_children"):
                block["children"] = self.get_all_blocks(block["id"])

        return blocks