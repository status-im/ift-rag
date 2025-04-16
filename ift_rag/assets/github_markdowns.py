import dagster as dg
from llama_index.core import Document
from ..resources import GithubResource, MinioResource

def github_markdown_factory(repository_name: str, branch_name: str, *file_extensions):

    to_asset = lambda value: str(value).replace('-', '_').replace('.', '_').replace("/", "_")
    
    @dg.asset(
        kinds=["Github", "LlamaIndex", "Minio"], # 🦙 is not allowed :/
        owners=["team:Nikolay"],
        group_name="GitHub_Extraction",
        description=f"Convert GitHub repository {repository_name} files into 🦙 documents",
        tags={
            "github": "",
        },
        metadata={
            "branch": branch_name,
            "file_extensions": ", ".join(list(file_extensions))
        },
        name=f"{to_asset(repository_name)}_markdown_documents"
    )
    def asset_template(context: dg.AssetExecutionContext, github: GithubResource, minio: MinioResource) -> dg.MaterializeResult:
        
        repo_path = f"{github.organization}/{repository_name}"
        context.log.info(f"Fetching repository: {repo_path}")
        repo = github.client.get_repo(repo_path)
        context.log.info(f"Fetched repository {repo_path}")
        
        github_files = github.get_files(repository_name, branch_name, *file_extensions)
        context.log.info(f"Fetched {len(github_files)} files with extensions {file_extensions}")
        
        for item in github_files:

            commit = list(repo.get_commits(path=item.path))[0]

            params = {
                "text": repo.get_contents(item.path, branch_name).decoded_content.decode("utf-8"),
                "metadata": {
                    "path": item.path,
                    "parent": None if not str(item.path).endswith(tuple(file_extensions)) else str(item.path).split("/")[0].title(),
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

            document = Document(**params)
            document.node_id
            path = f"documents/markdown/github/{to_asset(repository_name)}/{document.node_id}.pkl"
            minio.upload(document, path)
            context.log.debug(f"Created 🦙 for {item.path} in Minio {path}")

        metadata = {
            "files": str(len(github_files)),
            "url": dg.MetadataValue.url(f"https://github.com/{github.organization}/{repository_name}"),
        }
        return dg.MaterializeResult(metadata=metadata)

    return asset_template