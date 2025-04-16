import dagster as dg
from github import Github, GitTree
from typing import Optional
from pydantic import Field

class GithubResource(dg.ConfigurableResource):

    token: str = Field(description="A GitHub classic token")
    organization: str = Field(description="The GitHub organization / username", default="status-im")
    __client: Optional[Github] = None

    @property
    def client(self) -> Github:
        
        if not self.__client:
            self.__client = Github(self.token)

        return self.__client
    


    def get_files(self, repository_name: str, branch_name: str, *extensions) -> GitTree.GitTree:
        """
        Get a list of all the files in the given repository and branch

        Parameters:
            - `repository_name` - the GitHub repository name
            - `branch_name` - the GitHub branch name of the repository
            - `*extensions` - the file extensions that will be checked

        Output:
            - list of all GitHub files
        """
        repository = self.client.get_repo(f"{self.organization}/{repository_name}")
        git_tree = repository.get_git_tree(branch_name, True)

        if extensions:
            return [item for item in git_tree.tree if item.path.endswith(extensions)]
        
        return git_tree.tree
    


    