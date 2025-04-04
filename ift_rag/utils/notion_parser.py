import datetime
import pandas as pd
from typing import Union, Optional

class Block:

    def __init__(self, document: dict, indents: int = 0):
        
        self.id: str = document["id"]
        self.type: str = document["type"]
        self.parent_type: str = document["parent"]["type"]
        self.parent_id: str = document["parent"][document["parent"]["type"]]
        self.indents = "".join(["\t" for i in range(indents)]) 

        format = "%Y-%m-%dT%H:%M:%S.%fZ"

        self.last_edited_time = datetime.datetime.strptime(document["last_edited_time"], format)
        self.created_time = datetime.datetime.strptime(document["created_time"], format)

        self.rich_text: list[dict] = document[self.type].get("rich_text", [])
        
        self.markdown_text = "".join([
            self.set_annotations(rich_text["plain_text"], rich_text["annotations"], rich_text["href"])
            for rich_text in self.rich_text
        ]) + "\n"
        
        self.children: list[dict] = document.get("children", [])

        
    @staticmethod
    def set_annotations(plain_text: str, annotations: dict[str, Union[str, bool]], href: Optional[str] = None) -> str:
        """
        Create the annotations for a single text chunk

        Parameters:
            - `plain_text` - the current text chunk
            - `annotations` - the Notion "annotations"
            - `href` - the Notion url (if any)

        Output:
            - the Markdown text
        """
        markdown_text = plain_text

        is_href =  isinstance(href, str) and len(href) > 0

        if is_href:
            markdown_text = f"[{markdown_text.strip()}]({href})"

        if annotations["bold"]:
            markdown_text = f"**{markdown_text.strip()}**"

        if annotations["italic"]:
            markdown_text = f"*{markdown_text.strip()}*"

        if annotations["underline"]:
            markdown_text = f"<ins>{markdown_text.strip()}</ins>"

        if annotations["strikethrough"]:
            markdown_text = f"~~{markdown_text.strip()}~~"

        return markdown_text



class BulletPoint(Block):

    def __init__(self, document: dict, indents: int = 0):
        super().__init__(document, indents=indents)
        self.markdown_text = self.indents + f"- {self.markdown_text}"
        


class Header(Block):

    def __init__(self, document: dict, indents: int = 0):
        super().__init__(document, indents=indents)
        self.markdown_text = self.indents + "".join(["#" for i in range(int(self.type.split("_")[-1]))]) + f" {self.markdown_text}"
        


class Table(Block):

    def __init__(self, document: dict, indents: int = 0):
        super().__init__(document, indents=indents)
        self.markdown_text = self.indents + self.__create_table_markdown()

    def __create_table_markdown(self) -> str:
        """
        Create a table markdown from Children table rows

        Parameters:
            - `children` - the child documents of the table. Children should not be nested.

        Output:
            - the markdown of the Notion table
        """
        records = [
            [
                "".join([
                    self.set_annotations(cell_value["plain_text"], cell_value["annotations"], cell_value["href"]) 
                    for cell_value in cell
                ])
                for cell in child["table_row"]["cells"]
            ]
            for child in self.children
        ]

        return "\n" + pd.DataFrame.from_records(records[1:], columns=records[0]).to_markdown(index=False) + "\n"


class Code(Block):

    def __init__(self, document: dict, indents: int = 0):
        super().__init__(document, indents)
        self.language = document["code"]["language"]
        
        self.markdown_text = f"\n```{self.language}\n" + "\n".join([
            rich_text["plain_text"] 
            for rich_text in self.rich_text
        ]) + "\n```\n"


class Quote(Block):

    def __init__(self, document: dict, indents: int = 0):
        super().__init__(document, indents)
        self.markdown_text = f"{self.indents}> {self.markdown_text}"


class Divider(Block):

    def __init__(self, document: dict, indents: int = 0):
        super().__init__(document, indents)
        self.markdown_text = "---\n"

# ============================================================================================================================================================
# ============================================================================================================================================================
# ============================================================================================================================================================

def get_notion_block(document: dict, text_type: str, indents: int = 0) -> Union[Block, Header, Table, BulletPoint, Code]:
    """
    Get the correct Notion Block type based on the given Notion JSON document.

    Parameters:
        - `document` - the current JSON result from Notion
        - `text_type` - the JSON "type" key value
        - `indents` - how many `\t` to put in front of the markdown

    Output:
        - the correct Block type class for the given JSON resu.t
    """
    if text_type.startswith("heading"):
        return Header(document, indents)
    
    if text_type == "table":
        return Table(document, indents)
    
    if text_type in ["to_do", "bulleted_list_item", "numbered_list_item", "toggle"]:
        return BulletPoint(document, indents)

    if text_type == "code":
        return Code(document, indents)
    
    if text_type == "quote":
        return Quote(document, indents)
    
    if text_type == "divider":
        return Divider(document, indents)

    return Block(document, indents)



def get_child_markdown(parent: Union[Block, Header, Table, BulletPoint], markdown_text: str = "") -> str:
    """
    Create a recursive Markdown for the child elements inside the current block.

    Parameters:
        - `parent` - the current block
        - `markdown_text`

    Output:
        - the Markdown for the child elements of the parent
    """
    if len(parent.children) == 0:
        return ""

    for child in parent.children:
        
        indents = len(parent.indents) + 1 if parent.type != "column" else 0
        child_block = get_notion_block(child, child["type"], indents)
        
        markdown_text += child_block.markdown_text
        if child_block.type != "table":
            markdown_text += get_child_markdown(child_block, "")

    return markdown_text