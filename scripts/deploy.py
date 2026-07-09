#!/usr/bin/env python3
"""
Deploy Fabric artifacts directly to the target workspace via REST API.

How it works:
  - Reads config/valueSets/{branch}.json to determine the target workspace.
  - DEV values in the repo are treated as the source for GUID replacement.
  - Before deploying, replaces DEV OneLake URL with the target environment URL
    inside .tmdl files (DirectLake Semantic Model connection string).
  - Supports two deploy modes:
      selective (default) — only artifacts changed since HEAD~1
      full               — all artifacts (first deploy or workflow_dispatch)

Environment variables expected (from GitHub Actions secrets):
  AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

Optional:
  DEPLOY_MODE   — "selective" | "full" (default: selective)
  GITHUB_REF_NAME — branch name injected automatically by GitHub Actions
"""
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fabric_client import FabricClient
from utils import (
    get_changed_items,
    get_display_name,
    get_item_type,
    load_valuesets,
    read_item_parts,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

ARTIFACT_DIRS = [
    REPO_ROOT / "notebooks",
    REPO_ROOT / "pipelines",
    REPO_ROOT / "semantic models",
    REPO_ROOT / "report",
]


def _current_branch() -> str:
    env_branch = os.getenv("GITHUB_REF_NAME") or os.getenv("BRANCH_NAME")
    if env_branch:
        return env_branch
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    return result.stdout.strip()


def _all_artifacts() -> list[Path]:
    items = []
    for artifact_dir in ARTIFACT_DIRS:
        if not artifact_dir.exists():
            continue
        for entry in sorted(artifact_dir.iterdir()):
            if entry.is_dir() and get_item_type(entry.name):
                items.append(entry)
    return items


def main() -> None:
    mode = os.getenv("DEPLOY_MODE", "selective")
    branch = _current_branch()

    print(f"=== Fabric Deploy ===")
    print(f"Branch : {branch}")
    print(f"Mode   : {mode}")

    target_config = load_valuesets(REPO_ROOT, branch)
    dev_config = load_valuesets(REPO_ROOT, "dev")

    workspace_id = target_config["workspace_id"]
    print(f"Target workspace: {workspace_id}\n")

    # Build GUID replacement map: swap DEV OneLake URL → target OneLake URL
    # Only applies when deploying to an environment other than DEV itself
    replacements: dict[str, str] = {}
    dev_url = dev_config.get("onelake_url", "")
    target_url = target_config.get("onelake_url", "")
    if dev_url and target_url and dev_url != target_url:
        replacements[dev_url] = target_url

    # Determine which artifacts to deploy
    if mode == "full":
        items = _all_artifacts()
    else:
        items = get_changed_items(REPO_ROOT)
        if not items:
            print("No artifact changes detected — nothing to deploy.")
            return

    # Filter out lakehouses (managed separately, not via item definition API)
    deployable = [p for p in items if get_item_type(p.name) and not p.name.endswith(".Lakehouse")]

    if not deployable:
        print("No deployable artifacts in changeset (lakehouses are excluded).")
        return

    print(f"Artifacts to deploy ({len(deployable)}):")
    for p in deployable:
        print(f"  {p.name}")
    print()

    client = FabricClient(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    existing = {
        item["displayName"]: item["id"]
        for item in client.get_workspace_items(workspace_id)
    }

    success, failed = 0, []

    for item_path in deployable:
        item_type = get_item_type(item_path.name)
        display_name = get_display_name(item_path)

        try:
            parts = read_item_parts(item_path, replacements or None)

            if display_name in existing:
                print(f"  Updating  [{item_type}] {display_name}")
                client.update_item_definition(workspace_id, existing[display_name], parts)
            else:
                print(f"  Creating  [{item_type}] {display_name}")
                client.create_item(workspace_id, display_name, item_type, parts)

            print(f"  OK: {display_name}\n")
            success += 1

        except Exception as exc:
            print(f"  FAILED: {display_name} — {exc}\n")
            failed.append(display_name)

    print(f"=== Result: {success} deployed, {len(failed)} failed ===")
    if failed:
        print(f"Failed: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
