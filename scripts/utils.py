import base64
import json
import subprocess
from pathlib import Path

ITEM_TYPE_MAP = {
    ".Notebook": "Notebook",
    ".DataPipeline": "DataPipeline",
    ".SemanticModel": "SemanticModel",
    ".Report": "Report",
}

ARTIFACT_SUFFIXES = tuple(ITEM_TYPE_MAP.keys())

# Only .tmdl files carry environment-specific GUIDs (DirectLake expressions)
_REPLACEABLE_EXTENSIONS = {".tmdl"}


def get_item_type(folder_name: str) -> str | None:
    for suffix, item_type in ITEM_TYPE_MAP.items():
        if folder_name.endswith(suffix):
            return item_type
    return None


def get_display_name(item_path: Path) -> str:
    platform_file = item_path / ".platform"
    if platform_file.exists():
        data = json.loads(platform_file.read_text(encoding="utf-8"))
        return data["metadata"]["displayName"]
    # Fallback: strip the type suffix from the folder name
    name = item_path.name
    for suffix in ARTIFACT_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def get_changed_items(repo_root: Path, depth: int = 1) -> list[Path]:
    result = subprocess.run(
        ["git", "diff", f"HEAD~{depth}", "--name-only"],
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    items: set[Path] = set()
    for f in result.stdout.strip().splitlines():
        parts = Path(f).parts
        for i, part in enumerate(parts):
            if any(part.endswith(s) for s in ARTIFACT_SUFFIXES):
                items.add(repo_root / Path(*parts[: i + 1]))
                break
    return list(items)


def read_item_parts(item_path: Path, replacements: dict[str, str] | None = None) -> list[dict]:
    parts = []
    for file_path in sorted(item_path.rglob("*")):
        if file_path.is_dir():
            continue
        relative = file_path.relative_to(item_path).as_posix()
        raw = file_path.read_bytes()
        if replacements and file_path.suffix in _REPLACEABLE_EXTENSIONS:
            text = raw.decode("utf-8")
            for old, new in replacements.items():
                text = text.replace(old, new)
            raw = text.encode("utf-8")
        parts.append({
            "path": relative,
            "payload": base64.b64encode(raw).decode("ascii"),
            "payloadType": "InlineBase64",
        })
    return parts


def load_valuesets(repo_root: Path, branch: str) -> dict:
    path = repo_root / "config" / "valueSets" / f"{branch}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"No valueSet found for branch '{branch}'. Expected: {path}"
        )
    return json.loads(path.read_text(encoding="utf-8"))
