#!/usr/bin/env python3
"""
Debug script: downloads the report definition from Fabric and prints the report.json.
Run locally with env vars set:
  AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, FABRIC_WORKSPACE_ID_DEV
"""
import base64
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fabric_client import FabricClient
from utils import load_valuesets

REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    tenant_id = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]

    branch = os.getenv("BRANCH", "dev")
    config = load_valuesets(REPO_ROOT, branch)
    workspace_id = config["workspace_id"]

    client = FabricClient(tenant_id, client_id, client_secret)
    items = {i["displayName"]: i for i in client.get_workspace_items(workspace_id)}

    report_name = "rpt_crypto_dashboard"
    if report_name not in items:
        print(f"Report '{report_name}' not found in workspace {workspace_id}")
        print("Available items:", list(items.keys()))
        sys.exit(1)

    report_id = items[report_name]["id"]
    print(f"Report ID: {report_id}")
    print("Fetching definition...")

    raw = client.get_item_definition_raw(workspace_id, report_id, fmt="PBIR-Legacy")
    print(f"\nRAW response (first 2000 chars):\n{raw[:2000]}\n")

    import json as _json
    data = _json.loads(raw)
    parts = (
        data.get("parts")
        or data.get("definition", {}).get("parts")
        or []
    )

    for part in parts:
        path = part["path"]
        payload = base64.b64decode(part["payload"]).decode("utf-8", errors="replace")
        print(f"\n{'='*60}")
        print(f"PATH: {path}")
        print(f"{'='*60}")
        if path.endswith(".json"):
            try:
                parsed = json.loads(payload)
                print(json.dumps(parsed, indent=2)[:3000])
                if len(payload) > 3000:
                    print(f"... [truncated, total {len(payload)} chars]")
            except json.JSONDecodeError:
                print(payload[:1000])
        else:
            print(payload[:500])


if __name__ == "__main__":
    main()
