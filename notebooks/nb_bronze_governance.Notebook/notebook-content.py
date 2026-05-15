# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4b77bc3c-8482-4301-8454-b7b97ada8da8",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "4b77bc3c-8482-4301-8454-b7b97ada8da8"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_bronze_governance
#
# **Layer:** Bronze — Governance Catalog
# **Destination:** Delta tables in the default lakehouse
# **Purpose:** descobrir artefatos Fabric e criar tabelas de governança Bronze.

# MARKDOWN ********************

# ### Import configuration and helpers

# CELL ********************

import os
import sys
import json
from datetime import datetime
import pandas as pd
from pathlib import Path

# Add the repository tools path so Fabric helper modules can be imported
repo_root = Path("..").resolve()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# Attempt to import the repository Fabric helper package if available
try:
    from tools.fabric_governance.fabric_governance.auth import FabricAuth
    from tools.fabric_governance.fabric_governance.api import FabricClient
    has_fabric_client = True
except Exception:
    FabricAuth = None
    FabricClient = None
    has_fabric_client = False

secrets_path = os.environ.get("GIT_SECRETS_PATH")
workspace_id = os.environ.get("FABRIC_WORKSPACE_ID_DEV") or os.environ.get("FABRIC_WORKSPACE_ID")

print("Fabric client available:", has_fabric_client)
print("Workspace ID:", workspace_id)

# MARKDOWN ********************

# ### Discover Fabric artifacts

# CELL ********************

pipelines = []
items = []
lakehouses = []

if has_fabric_client and workspace_id:
    auth = FabricAuth.from_env_or_file(secrets_path)
    client = FabricClient(auth.get_access_token())

    try:
        lp = client.list_data_pipelines(workspace_id)
        pipelines = lp.get("value") or lp.get("items") or lp or []
    except Exception as e:
        print("list_data_pipelines failed:", e)

    try:
        lh = client.list_lakehouses(workspace_id)
        lakehouses = lh.get("value") or lh or []
    except Exception as e:
        print("list_lakehouses failed:", e)

    try:
        it = client.list_items(workspace_id)
        items = it.get("value") or it or []
    except Exception as e:
        print("list_items failed:", e)
else:
    print("Fabric client not available or workspace ID missing. Repositório local apenas.")

print(f"Found: {len(pipelines)} pipelines, {len(lakehouses)} lakehouses, {len(items)} items")

# MARKDOWN ********************

# ### Normalize artifact catalog

# CELL ********************

rows_pipes = []
for p in pipelines:
    rows_pipes.append({
        "id": p.get("id"),
        "name": p.get("displayName") or p.get("name") or p.get("id"),
        "type": p.get("type") or "DataPipeline",
        "workspace_id": workspace_id,
        "raw": json.dumps(p, ensure_ascii=False),
        "last_updated": p.get("lastModifiedDateTime") or p.get("modifiedDate"),
        "discovered_at": datetime.utcnow().isoformat(),
    })

rows_lh = []
for l in lakehouses:
    rows_lh.append({
        "id": l.get("id"),
        "name": l.get("displayName") or l.get("name") or l.get("id"),
        "type": "Lakehouse",
        "workspace_id": workspace_id,
        "raw": json.dumps(l, ensure_ascii=False),
        "last_updated": l.get("lastModifiedDateTime") or l.get("modifiedDate"),
        "discovered_at": datetime.utcnow().isoformat(),
    })

rows_items = []
for it in items:
    rows_items.append({
        "id": it.get("id"),
        "name": it.get("displayName") or it.get("name") or it.get("id"),
        "type": it.get("type") or "Item",
        "workspace_id": workspace_id,
        "raw": json.dumps(it, ensure_ascii=False),
        "last_updated": it.get("lastModifiedDateTime") or it.get("modifiedDate"),
        "discovered_at": datetime.utcnow().isoformat(),
    })

print("Prepared catalog records.")

# MARKDOWN ********************

# ### Create Bronze governance tables

# CELL ********************

def create_table_ddl(table_name, cols):
    cols_ddl = ",\n        ".join([f"{name} {dtype}" for name, dtype in cols])
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n        {cols_ddl}\n    ) USING DELTA"


tables = {
    "fabric_governance_pipelines": [
        ("id", "STRING"),
        ("name", "STRING"),
        ("type", "STRING"),
        ("workspace_id", "STRING"),
        ("raw", "STRING"),
        ("last_updated", "TIMESTAMP"),
        ("discovered_at", "TIMESTAMP"),
    ],
    "fabric_governance_dfgen2": [
        ("id", "STRING"),
        ("name", "STRING"),
        ("type", "STRING"),
        ("workspace_id", "STRING"),
        ("raw", "STRING"),
        ("last_updated", "TIMESTAMP"),
        ("discovered_at", "TIMESTAMP"),
    ],
    "fabric_governance_workspaces": [
        ("id", "STRING"),
        ("name", "STRING"),
        ("tenant_id", "STRING"),
        ("raw", "STRING"),
        ("discovered_at", "TIMESTAMP"),
    ],
}

try:
    spark
    using_spark = True
except NameError:
    using_spark = False

for table_name, cols in tables.items():
    ddl = create_table_ddl(table_name, cols)
    print(f"Creating table: {table_name}")
    print(ddl)
    if using_spark:
        try:
            spark.sql(ddl)
            print(f"Created table {table_name}")
        except Exception as e:
            print(f"Failed to create table {table_name}: {e}")
    else:
        out_dir = Path("tools/fabric-governance/sql").resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{table_name}.sql").write_text(ddl, encoding="utf-8")
        print(f"Wrote DDL to {out_dir / f'{table_name}.sql'}")

# MARKDOWN ********************

# ### Validate created tables

# CELL ********************

if using_spark:
    for table_name in tables.keys():
        exists = spark.catalog.tableExists(table_name)
        print(f"{table_name}: tableExists={exists}")

    print("List of current tables in default database:")
    spark.sql("SHOW TABLES").show(truncate=False)
else:
    print("Spark is not available in the current execution environment.")
