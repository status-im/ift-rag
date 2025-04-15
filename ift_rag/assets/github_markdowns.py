import dagster as dg
from llama_index.core import Document
from ..resources import GithubResource

def github_markdown_factory(repository_name: str, branch_name: str, *file_extensions):

    @dg.asset(
        kinds=["Github", "LlamaIndex"], # 🦙 is not allowed :/
        owners=["team:Nikolay"],
        group_name="GitHub_Extraction",
        description="Convert the Notion page text to Markdown and split it into chunks (🦙 Documents) that will be stored in the vector store",
        tags={
            "github": "",
        },
        metadata={
            "branch": branch_name,
            "file_extensions": ", ".join(list(file_extensions))
        },
        name=f"{repository_name.replace('-', '_').replace('.', '_')}_markdown_documents"
    )
    def asset_template(context: dg.AssetExecutionContext, github: GithubResource) -> dg.Output:
        
        repo_path = f"{github.organization}/{repository_name}"
        context.log.info(f"Fetching repository: {repo_path}")
        repo = github.client.get_repo(repo_path)
        context.log.info(f"Fetched repository {repo_path}")
        
        github_files = github.get_files(repository_name, branch_name, *file_extensions)
        context.log.info(f"Fetched {len(github_files)} files with extensions {file_extensions}")
        documents = []

        for item in github_files:

            commit = list(repo.get_commits(path=item.path))[0]

            params = {
                "text": repo.get_contents(item.path, branch_name).decoded_content.decode("utf-8"),
                "metadata": {
                    "path": item.path,
                    "url": f"https://github.com/{github.organization}/{repository_name}/blob/{branch_name}/{item.path}",
                    "branch": {
                        "last_modified_time": item.last_modified_datetime,
                        "name": branch_name
                    },
                    "source": "github",
                    "repository_name": repository_name,
                    "commit": {
                        "url": commit.html_url,
                        "author": commit.author.login,
                        "committer": commit.committer.login,
                        "last_modified_time": commit.last_modified_datetime
                    }
                }
            }

            documents.append(Document(**params))
            context.log.debug(f"Created 🦙 for {item.path}")

        metadata = {
            "files": str(len(github_files)),
            "organization": github.organization
        }
        return dg.Output(documents, metadata=metadata)


    return asset_template