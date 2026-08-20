#!/usr/bin/env python3
"""Temporary: print the latest pl_medallion_orchestration job instance status."""
import os

from fabric_client import FabricClient
from utils import load_valuesets

REPO_ROOT_PARENT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main() -> None:
    branch = os.environ.get("BRANCH", "dev")
    from pathlib import Path
    config = load_valuesets(Path(REPO_ROOT_PARENT), branch)
    workspace_id = config["workspace_id"]

    client = FabricClient(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    items = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id)}
    pipeline_id = items.get("pl_medallion_orchestration")
    print(f"Workspace: {workspace_id}")
    print(f"Pipeline item id: {pipeline_id}")

    import requests
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances",
        headers=client._headers(),
        timeout=30,
    )
    resp.raise_for_status()
    instances = resp.json().get("value", [])
    instances.sort(key=lambda x: x.get("startTimeUtc", ""), reverse=True)
    for inst in instances[:3]:
        print("---")
        print(f"  id: {inst.get('id')}")
        print(f"  status: {inst.get('status')}")
        print(f"  startTimeUtc: {inst.get('startTimeUtc')}")
        print(f"  endTimeUtc: {inst.get('endTimeUtc')}")
        fr = inst.get("failureReason")
        if fr:
            print(f"  failureReason: {fr}")


if __name__ == "__main__":
    main()
