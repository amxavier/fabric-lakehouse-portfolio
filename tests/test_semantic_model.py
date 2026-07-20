import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SM_PATH = REPO_ROOT / "semantic models" / "sm_crypto_medallion.SemanticModel"
DEFINITION_PATH = SM_PATH / "definition"

EXPECTED_MEASURES = [
    "Total Market Cap (USD)",
    "Total Volume 24h (USD)",
    "Avg Price vs ATH (%)",
    "Top Coin",
    "Avg Price Change 7d (%)",
    "Large Cap Dominance (%)",
]

EXPECTED_TABLES = ["dim_coin", "dim_date", "fact_prices", "measure"]

# Repository always stores the DEV OneLake workspace GUID; replaced at deploy time.
DEV_WORKSPACE_GUID = "dc072922-4ffb-4424-868c-28087b02ecba"


def test_semantic_model_folder_exists():
    assert SM_PATH.is_dir(), f"SemanticModel folder not found: {SM_PATH}"


def test_platform_file_is_valid():
    platform = SM_PATH / ".platform"
    assert platform.exists(), ".platform file missing"
    data = json.loads(platform.read_text(encoding="utf-8"))
    assert data["metadata"]["displayName"] == "sm_crypto_medallion"
    assert data["metadata"]["type"] == "SemanticModel"


def test_required_tmdl_files_exist():
    required = ["model.tmdl", "expressions.tmdl", "relationships.tmdl", "database.tmdl"]
    for name in required:
        assert (DEFINITION_PATH / name).exists(), f"Missing required TMDL file: {name}"


def test_all_tables_present():
    tables_dir = DEFINITION_PATH / "tables"
    assert tables_dir.is_dir(), "tables/ directory missing inside definition/"
    present = {f.stem for f in tables_dir.glob("*.tmdl")}
    for table in EXPECTED_TABLES:
        assert table in present, f"Missing table TMDL: {table}.tmdl"


def test_all_measures_defined():
    content = (DEFINITION_PATH / "tables" / "measure.tmdl").read_text(encoding="utf-8")
    for measure in EXPECTED_MEASURES:
        assert f"measure '{measure}'" in content, f"Missing DAX measure: {measure}"


def test_expressions_tmdl_contains_dev_workspace_guid():
    # DEV GUID must be present in the repo so the deploy script can replace it per environment.
    content = (DEFINITION_PATH / "expressions.tmdl").read_text(encoding="utf-8")
    assert DEV_WORKSPACE_GUID in content, (
        f"DEV workspace GUID not found in expressions.tmdl — "
        "the deploy script requires it to perform environment substitution"
    )
