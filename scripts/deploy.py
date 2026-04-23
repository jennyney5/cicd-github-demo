"""Deploy Fabric workspace items to the target environment using fabric-cicd."""

import os
import sys
from pathlib import Path

from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items


def main():
    workspace_id = os.environ.get("FABRIC_WORKSPACE_ID")
    environment = os.environ.get("FABRIC_ENVIRONMENT", "PROD")
    remove_orphans = os.environ.get("REMOVE_ORPHAN_ITEMS", "false").lower() == "true"

    if not workspace_id:
        print("ERROR: FABRIC_WORKSPACE_ID environment variable is required")
        sys.exit(1)

    # Repo root is one level up from this script
    repo_dir = Path(__file__).resolve().parent.parent

    # Determine auth credential
    # In GitHub Actions: uses DefaultAzureCredential (OIDC via azure/login)
    # Locally: uses AzureCliCredential (az login)
    if os.environ.get("GITHUB_ACTIONS"):
        from azure.identity import DefaultAzureCredential
        token_credential = DefaultAzureCredential()
    else:
        from azure.identity import AzureCliCredential
        token_credential = AzureCliCredential()

    print(f"Deploying to workspace: {workspace_id}")
    print(f"Environment: {environment}")
    print(f"Repository directory: {repo_dir}")

    item_types = [
        "Notebook",
        "DataPipeline",
        "SemanticModel",
        "Report",
        "Lakehouse",
        "VariableLibrary",
    ]

    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        repository_directory=str(repo_dir),
        environment=environment,
        item_type_in_scope=item_types,
        token_credential=token_credential,
    )

    publish_all_items(workspace)
    print("Deployment complete.")

    if remove_orphans:
        print("Removing orphan items...")
        unpublish_all_orphan_items(workspace)
        print("Orphan removal complete.")


if __name__ == "__main__":
    main()
