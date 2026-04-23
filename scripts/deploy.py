"""Deploy Fabric workspace items to the target environment using fabric-cicd."""

import json
import os
import sys
from pathlib import Path

import requests
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

FABRIC_API = "https://api.fabric.microsoft.com/v1"


def get_token(credential):
    """Get a bearer token for the Fabric API."""
    return credential.get_token("https://api.fabric.microsoft.com/.default").token


def set_active_value_set(workspace_id, token, value_set_name):
    """Set the active value set on all Variable Libraries in the workspace."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # List variable libraries
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/variableLibraries", headers=headers
    )
    resp.raise_for_status()
    libraries = resp.json().get("value", [])

    for lib in libraries:
        lib_id = lib["id"]
        lib_name = lib["displayName"]
        print(f"Setting active value set '{value_set_name}' on '{lib_name}'...")
        patch_resp = requests.patch(
            f"{FABRIC_API}/workspaces/{workspace_id}/variableLibraries/{lib_id}",
            headers=headers,
            json={"properties": {"activeValueSetName": value_set_name}},
        )
        if patch_resp.ok:
            print(f"  OK: '{lib_name}' active value set = '{value_set_name}'")
        else:
            print(f"  WARN: {patch_resp.status_code} - {patch_resp.text}")


def main():
    workspace_id = os.environ.get("FABRIC_WORKSPACE_ID")
    environment = os.environ.get("FABRIC_ENVIRONMENT", "PROD")
    active_value_set = os.environ.get("ACTIVE_VALUE_SET", "prod")
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

    # Set active value set on Variable Libraries
    if active_value_set:
        print(f"\nSetting active value set to '{active_value_set}'...")
        token = get_token(token_credential)
        set_active_value_set(workspace_id, token, active_value_set)

    if remove_orphans:
        print("Removing orphan items...")
        unpublish_all_orphan_items(workspace)
        print("Orphan removal complete.")


if __name__ == "__main__":
    main()
