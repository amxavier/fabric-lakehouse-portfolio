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

# # nb_bronze_governance_fabric
# 
# **Layer:** Bronze — Governance Catalog  
# **Destination:** Delta tables no lakehouse `lh_bronze_governance_fabric`  
# **Source:** Fabric Admin REST API + Power BI Admin REST API (Service Principal com Fabric/PBI Admin)  
# **Purpose:** inventário completo do tenant — Fabric items (notebooks, lakehouses, warehouses, semantic models, pipelines, dataflows gen2, reports, dashboards, eventstreams, KQL DBs) + Power BI legado (datasets v1, dashboards, reports).
# 
# ## Padrão
# 
# - **Naming:** `dim_*` (entidades) e `fact_*` (relações / métricas / cleanup).
# - **Auth:** Service Principal admin via OAuth client_credentials — mais poderoso que sempy, sem dependência de identidade interativa.
# - **Idempotente:** `CREATE TABLE IF NOT EXISTS` + `MERGE INTO` (upsert por chave natural).
# - **Soft-delete** via flag `is_active` quando objeto some da API.
# - **Particionamento Delta** por `env` + dimensão de alta cardinalidade.
# - **Parametrizado** por `env` (DEV/PRD) e credenciais via secrets externos.
# - **`raw_json`** preservado em STRING para auditoria e reprocessamento Silver.
# - **`scan_errors`** persistido em `fact_governance_scan_errors` (não silencia falhas).
# 
# ## Auditoria de objetos
# 
# Cada tabela `dim_*` carrega 4 colunas extraídas da API quando disponíveis (null caso contrário):
# `created_date, created_by, modified_date, modified_by`.
# 
# > **Nota:** `last_access_ts` (último acesso) **não** é retornado pelos endpoints admin de listagem.  
# > Será derivado no Silver cruzando com `/admin/activityevents` (audit log, retenção 30d).
# 
# ## Tabelas geradas
# 
# | Tabela | Conteúdo |
# |---|---|
# | `dim_workspace_fabric` | Todos os workspaces do tenant |
# | `dim_capacity_fabric` | Capacities (Fabric/PBI Premium/PPU) |
# | `dim_item_fabric` | Catálogo geral de items Fabric (notebooks, lakehouses, warehouses, semantic models, etc.) |
# | `dim_pipeline_fabric` | Data Pipelines |
# | `dim_dataflow_gen2_fabric` | Dataflows Gen2 |
# | `dim_dataset_pbi` | Power BI datasets v1 (legado) |
# | `fact_report_fabric` | Reports (Fabric + PBI legado) com link para dataset |
# | `dim_dashboard_pbi` | Power BI dashboards (legado) |
# | `fact_governance_scan_errors` | Erros do scan por workspace/objeto |


# MARKDOWN ********************

# ## 1. Parâmetros

# CELL ********************

# PARAMETERS CELL — substituído em runtime via Fabric Data Pipeline / notebookutils

env = 'DEV'                     # DEV | PRD
tenant_id = ''                  # injetar via pipeline parameter / variável de workspace
client_id = ''                  # SP application (client) id
client_secret = ''              # SP secret — injetar via Key Vault ou git-secrets, nunca commitar

# Filtros opcionais (vazio = tudo)
workspace_filter = []           # lista de workspace_ids para limitar; vazio = todos do tenant

# Lakehouse / database alvo (default lakehouse já está anexado ao notebook)
target_database = 'lh_bronze_governance_fabric'

# Comportamento
soft_delete_enabled = True      # marca is_active=false em objetos que sumiram da API
run_optimize = True             # OPTIMIZE + VACUUM ao final
vacuum_retention_hours = 168    # 7 dias

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Imports e run metadata

# CELL ********************

import json
import time
import hashlib
import requests
import uuid
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType,
    ArrayType, MapType
)
from delta.tables import DeltaTable

RUN_ID = str(uuid.uuid4())
RUN_TS_UTC = datetime.now(timezone.utc)

print(f'env={env} | run_id={RUN_ID}')
print(f'run_timestamp_utc={RUN_TS_UTC.isoformat()}')
print(f'target_database={target_database}')
print(f'workspace_filter={workspace_filter if workspace_filter else "<all>"}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Autenticação (Service Principal)
# 
# OAuth `client_credentials` com dois scopes:
# - `https://api.fabric.microsoft.com/.default` — endpoints Fabric `/v1/admin/*`
# - `https://analysis.windows.net/powerbi/api/.default` — endpoints PBI legacy `/v1.0/myorg/admin/*`
# 
# Cache de token por scope (renova ~5min antes da expiração).

# CELL ********************

_TOKEN_CACHE = {}

def get_token(scope: str) -> str:
    """Obtém access token via client_credentials. Cacheia por scope até 5min antes da expiração."""
    cached = _TOKEN_CACHE.get(scope)
    now = time.time()
    if cached and cached['exp'] > now:
        return cached['token']

    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scope,
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    payload = r.json()
    token = payload['access_token']
    _TOKEN_CACHE[scope] = {
        'token': token,
        'exp': now + int(payload.get('expires_in', 3000)) - 300,
    }
    return token

FABRIC_SCOPE = 'https://api.fabric.microsoft.com/.default'
PBI_SCOPE = 'https://analysis.windows.net/powerbi/api/.default'

# Valida que credenciais foram preenchidas
assert tenant_id and client_id and client_secret, 'tenant_id/client_id/client_secret precisam ser injetados'
_ = get_token(FABRIC_SCOPE)
_ = get_token(PBI_SCOPE)
print('Tokens OK')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. HTTP helpers — paginação, retry e `safe_call`
# 
# - `_request`: retry exponencial em 429/5xx, refresh de token em 401.
# - `get_paged`: itera `continuationUri` (Fabric) e `@odata.nextLink` (PBI).
# - `safe_call`: executa qualquer fn e retorna `(ok, value)` — usado para não quebrar o loop por workspace problemático.

# CELL ********************

FABRIC_BASE = 'https://api.fabric.microsoft.com/v1'
PBI_BASE = 'https://api.powerbi.com/v1.0/myorg'

def _request(method: str, url: str, scope: str, **kwargs) -> requests.Response:
    """HTTP request com retry exponencial em 429/5xx e refresh de token em 401."""
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        headers = kwargs.pop('headers', {})
        headers['Authorization'] = f'Bearer {get_token(scope)}'
        headers.setdefault('Content-Type', 'application/json')
        resp = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        if resp.status_code == 401:
            _TOKEN_CACHE.pop(scope, None)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            retry_after = int(resp.headers.get('Retry-After', 2 ** attempt))
            print(f'  {resp.status_code} on {url} — retrying in {retry_after}s (attempt {attempt})')
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp

def get_paged(url: str, scope: str, value_key: str = 'value') -> list:
    """Itera continuationUri / @odata.nextLink. Retorna lista de items."""
    items = []
    next_url = url
    while next_url:
        resp = _request('GET', next_url, scope)
        body = resp.json()
        items.extend(body.get(value_key, []) or [])
        next_url = body.get('continuationUri') or body.get('@odata.nextLink')
    return items

def safe_call(fn, *args, **kwargs):
    """Executa fn(*args, **kwargs) e retorna (ok, value_or_error_str).
    Padrão adotado para tolerar falhas por workspace sem abortar o scan."""
    try:
        return True, fn(*args, **kwargs)
    except Exception as e:
        return False, f'{type(e).__name__}: {str(e)}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Schemas e DDL Bronze
# 
# **Toda tabela carrega 3 blocos de colunas:**
# 1. **Específicas** do tipo (chave natural + atributos)
# 2. **Auditoria** do objeto (created/modified — vindos da API)
# 3. **Controle** da ingestão (env, raw_json, hash, run_id, soft-delete)

# CELL ********************

# Bloco de AUDITORIA — metadados do próprio objeto (vindos da API)
AUDIT_COLS = [
    ('created_date',    'TIMESTAMP'),
    ('created_by',      'STRING'),
    ('modified_date',   'TIMESTAMP'),
    ('modified_by',     'STRING'),
]

# Bloco de CONTROLE — metadados da própria ingestão
CONTROL_COLS = [
    ('env',                'STRING'),
    ('raw_json',           'STRING'),
    ('source_endpoint',    'STRING'),
    ('ingestion_ts',       'TIMESTAMP'),
    ('last_seen_ts',       'TIMESTAMP'),
    ('is_active',          'BOOLEAN'),
    ('row_hash',           'STRING'),
    ('run_id',             'STRING'),
    ('run_timestamp_utc',  'STRING'),
]

# Definição por tabela: chave_natural, particionamento, colunas_específicas
TABLES = {
    'dim_workspace_fabric': {
        'key': ['workspace_id'],
        'partition_by': ['env'],
        'cols': [
            ('workspace_id',            'STRING'),
            ('workspace_name',          'STRING'),
            ('workspace_type',          'STRING'),
            ('capacity_id',             'STRING'),
            ('state',                   'STRING'),
            ('is_on_dedicated_capacity', 'BOOLEAN'),
        ],
    },
    'dim_capacity_fabric': {
        'key': ['capacity_id'],
        'partition_by': ['env'],
        'cols': [
            ('capacity_id',   'STRING'),
            ('capacity_name', 'STRING'),
            ('sku',           'STRING'),
            ('region',        'STRING'),
            ('state',         'STRING'),
            ('admins',        'STRING'),  # JSON string array
        ],
    },
    'dim_item_fabric': {
        # catálogo geral de items Fabric (notebook, lakehouse, warehouse, semantic model, etc.)
        'key': ['workspace_id', 'item_id'],
        'partition_by': ['env', 'item_type'],
        'cols': [
            ('item_id',         'STRING'),
            ('item_name',       'STRING'),
            ('item_type',       'STRING'),
            ('description',     'STRING'),
            ('workspace_id',    'STRING'),
            ('workspace_name',  'STRING'),
        ],
    },
    'dim_pipeline_fabric': {
        'key': ['workspace_id', 'pipeline_id'],
        'partition_by': ['env'],
        'cols': [
            ('pipeline_id',    'STRING'),
            ('pipeline_name',  'STRING'),
            ('description',    'STRING'),
            ('workspace_id',   'STRING'),
            ('workspace_name', 'STRING'),
        ],
    },
    'dim_dataflow_gen2_fabric': {
        'key': ['workspace_id', 'dataflow_id'],
        'partition_by': ['env'],
        'cols': [
            ('dataflow_id',    'STRING'),
            ('dataflow_name',  'STRING'),
            ('description',    'STRING'),
            ('workspace_id',   'STRING'),
            ('workspace_name', 'STRING'),
        ],
    },
    'dim_dataset_pbi': {
        # datasets v1 (PBI legado) via admin
        'key': ['workspace_id', 'dataset_id'],
        'partition_by': ['env'],
        'cols': [
            ('dataset_id',                              'STRING'),
            ('dataset_name',                            'STRING'),
            ('configured_by',                           'STRING'),
            ('is_refreshable',                          'BOOLEAN'),
            ('is_effective_identity_required',          'BOOLEAN'),
            ('is_effective_identity_roles_required',    'BOOLEAN'),
            ('workspace_id',                            'STRING'),
            ('workspace_name',                          'STRING'),
        ],
    },
    'fact_report_fabric': {
        # reports Fabric + PBI legado (compartilham endpoint admin)
        'key': ['workspace_id', 'report_id'],
        'partition_by': ['env'],
        'cols': [
            ('report_id',           'STRING'),
            ('report_name',         'STRING'),
            ('report_type',         'STRING'),
            ('dataset_id',          'STRING'),
            ('dataset_workspace_id','STRING'),
            ('dataset_link_type',   'STRING'),  # DIRECT | UNKNOWN
            ('web_url',             'STRING'),
            ('embed_url',           'STRING'),
            ('workspace_id',        'STRING'),
            ('workspace_name',      'STRING'),
        ],
    },
    'dim_dashboard_pbi': {
        'key': ['workspace_id', 'dashboard_id'],
        'partition_by': ['env'],
        'cols': [
            ('dashboard_id',     'STRING'),
            ('dashboard_name',   'STRING'),
            ('is_read_only',     'BOOLEAN'),
            ('web_url',          'STRING'),
            ('workspace_id',     'STRING'),
            ('workspace_name',   'STRING'),
        ],
    },
}

# Tabela de erros — schema próprio, sem AUDIT_COLS/CONTROL_COLS padrão
SCAN_ERRORS_TABLE = 'fact_governance_scan_errors'
SCAN_ERRORS_SCHEMA = StructType([
    StructField('workspace_id',      StringType(),    True),
    StructField('workspace_name',    StringType(),    True),
    StructField('object_type',       StringType(),    True),
    StructField('endpoint',          StringType(),    True),
    StructField('error_message',     StringType(),    True),
    StructField('env',               StringType(),    True),
    StructField('run_id',            StringType(),    True),
    StructField('run_timestamp_utc', StringType(),    True),
    StructField('ingestion_ts',      TimestampType(), True),
])

def build_ddl(table_name: str, spec: dict) -> str:
    all_cols = spec['cols'] + AUDIT_COLS + CONTROL_COLS
    cols_ddl = ',\n    '.join([f'{n} {t}' for n, t in all_cols])
    parts = spec.get('partition_by') or []
    part_clause = f"\nPARTITIONED BY ({', '.join(parts)})" if parts else ''
    return (
        f'CREATE TABLE IF NOT EXISTS {table_name} (\n    {cols_ddl}\n) '
        f'USING DELTA{part_clause}'
    )

spark.sql(f'CREATE DATABASE IF NOT EXISTS {target_database}')
spark.sql(f'USE {target_database}')

for tname, spec in TABLES.items():
    ddl = build_ddl(tname, spec)
    spark.sql(ddl)
    print(f'  ✓ {tname}')

# DDL fixa para scan_errors
spark.sql(f'''
    CREATE TABLE IF NOT EXISTS {SCAN_ERRORS_TABLE} (
        workspace_id STRING, workspace_name STRING,
        object_type STRING, endpoint STRING, error_message STRING,
        env STRING, run_id STRING, run_timestamp_utc STRING, ingestion_ts TIMESTAMP
    ) USING DELTA PARTITIONED BY (env)
''')
print(f'  ✓ {SCAN_ERRORS_TABLE}')

print(f'DDL aplicada para {len(TABLES)+1} tabelas')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Helpers genéricos
# 
# - `_parse_ts` — converte ISO 8601 da API em datetime.
# - `_principal` — extrai UPN/displayName de campos que podem vir como string ou objeto.
# - `_audit` — extrai os 4 campos de auditoria com fallbacks para variações de nome.
# - `_hash_row` — SHA-256 do payload para detectar mudanças no MERGE.
# - `_wrap` — adiciona bloco de controle (env, raw_json, run_id, hash, ...).
# - `complex_to_json` — converte ArrayType/StructType/MapType em JSON string (Delta-safe).
# - `df_from_rows` — constrói DataFrame Spark respeitando o schema da tabela alvo.

# CELL ********************

def _hash_row(d: dict) -> str:
    payload = json.dumps(d, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()

def _parse_ts(val):
    """Converte string ISO da API em datetime (ou None)."""
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    try:
        s = str(val).replace('Z', '+00:00')
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _principal(v):
    """Extrai displayName/UPN de campo createdBy/modifiedBy (dict ou string)."""
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return (
            v.get('userPrincipalName')
            or v.get('displayName')
            or v.get('email')
            or v.get('id')
        )
    return str(v)

def _audit(obj: dict) -> dict:
    """Extrai os 4 campos de auditoria de um payload da API, tolerando variações de nome."""
    created_date = (
        obj.get('createdDate')
        or obj.get('createdDateTime')
        or obj.get('createdOn')
    )
    modified_date = (
        obj.get('modifiedDate')
        or obj.get('modifiedDateTime')
        or obj.get('lastUpdate')
        or obj.get('lastUpdatedDate')
        or obj.get('updatedDate')
    )
    created_by = (
        _principal(obj.get('createdBy'))
        or obj.get('createdById')
        or obj.get('configuredBy')   # PBI datasets
    )
    modified_by = (
        _principal(obj.get('modifiedBy'))
        or obj.get('modifiedById')
    )
    return {
        'created_date':  _parse_ts(created_date),
        'created_by':    created_by,
        'modified_date': _parse_ts(modified_date),
        'modified_by':   modified_by,
    }

def _wrap(row: dict, endpoint: str) -> dict:
    """Adiciona bloco de controle ao row dict."""
    row['env'] = env
    row['raw_json'] = json.dumps(row.get('_raw', {}), default=str, ensure_ascii=False)
    row['source_endpoint'] = endpoint
    row['ingestion_ts'] = RUN_TS_UTC
    row['last_seen_ts'] = RUN_TS_UTC
    row['is_active'] = True
    hash_input = {k: v for k, v in row.items()
                  if k not in ('ingestion_ts','last_seen_ts','run_id','row_hash','run_timestamp_utc')}
    row['row_hash'] = _hash_row(hash_input)
    row['run_id'] = RUN_ID
    row['run_timestamp_utc'] = RUN_TS_UTC.isoformat()
    row.pop('_raw', None)
    return row

def complex_to_json(df_spark: DataFrame) -> DataFrame:
    """Converte colunas Array/Struct/Map em JSON string (evita NullType em Delta)."""
    complex_cols = [
        f.name for f in df_spark.schema.fields
        if isinstance(f.dataType, (ArrayType, StructType, MapType))
    ]
    for c in complex_cols:
        df_spark = df_spark.withColumn(c, F.to_json(F.col(c)))
    return df_spark

def df_from_rows(rows: list, table_name: str) -> DataFrame:
    """Constrói DataFrame Spark com schema exato da tabela Bronze alvo."""
    spec = TABLES[table_name]
    schema_cols = spec['cols'] + AUDIT_COLS + CONTROL_COLS
    col_names = [c[0] for c in schema_cols]
    type_map = {
        'STRING': StringType(),
        'TIMESTAMP': TimestampType(),
        'BOOLEAN': BooleanType(),
    }
    schema = StructType([StructField(n, type_map[t], True) for n, t in schema_cols])

    if not rows:
        return spark.createDataFrame([], schema)

    normalized = [{c: r.get(c) for c in col_names} for r in rows]
    return spark.createDataFrame([Row(**r) for r in normalized], schema=schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Extractors — endpoints admin
# 
# Endpoints usados (SP com Fabric/PBI Admin):
# - `/v1/admin/workspaces` (Fabric)
# - `/v1/admin/workspaces/{id}/items` (Fabric)
# - `/v1.0/myorg/admin/capacities` (PBI)
# - `/v1.0/myorg/admin/datasets` (PBI legado)
# - `/v1.0/myorg/admin/reports` (PBI legado, cobre reports Fabric também)
# - `/v1.0/myorg/admin/dashboards` (PBI legado)

# CELL ********************

# Acumulador global de erros do scan
SCAN_ERRORS = []

def record_error(workspace_id, workspace_name, object_type, endpoint, error_msg):
    SCAN_ERRORS.append({
        'workspace_id':      workspace_id,
        'workspace_name':    workspace_name,
        'object_type':       object_type,
        'endpoint':          endpoint,
        'error_message':     str(error_msg)[:2000],
        'env':               env,
        'run_id':            RUN_ID,
        'run_timestamp_utc': RUN_TS_UTC.isoformat(),
        'ingestion_ts':      RUN_TS_UTC,
    })

def list_workspaces() -> list:
    url = f'{FABRIC_BASE}/admin/workspaces?type=Workspace'
    return get_paged(url, FABRIC_SCOPE)

def list_capacities() -> list:
    url = f'{PBI_BASE}/admin/capacities'
    return get_paged(url, PBI_SCOPE)

def list_items(workspace_id: str) -> list:
    url = f'{FABRIC_BASE}/admin/workspaces/{workspace_id}/items'
    return get_paged(url, FABRIC_SCOPE)

def list_pbi_datasets() -> list:
    url = f'{PBI_BASE}/admin/datasets'
    return get_paged(url, PBI_SCOPE)

def list_pbi_reports() -> list:
    url = f'{PBI_BASE}/admin/reports'
    return get_paged(url, PBI_SCOPE)

def list_pbi_dashboards() -> list:
    url = f'{PBI_BASE}/admin/dashboards'
    return get_paged(url, PBI_SCOPE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Normalização — payload da API → row Bronze

# CELL ********************

def norm_workspace(w: dict) -> dict:
    row = {
        'workspace_id':   w.get('id'),
        'workspace_name': w.get('name'),
        'workspace_type': w.get('type'),
        'capacity_id':    w.get('capacityId'),
        'state':          w.get('state'),
        'is_on_dedicated_capacity': w.get('isOnDedicatedCapacity'),
        '_raw': w,
    }
    row.update(_audit(w))
    return _wrap(row, '/v1/admin/workspaces')

def norm_capacity(c: dict) -> dict:
    admins = c.get('admins') or []
    row = {
        'capacity_id':   c.get('id'),
        'capacity_name': c.get('displayName') or c.get('name'),
        'sku':           c.get('sku'),
        'region':        c.get('region'),
        'state':         c.get('state'),
        'admins':        json.dumps(admins, ensure_ascii=False),
        '_raw': c,
    }
    row.update(_audit(c))
    return _wrap(row, '/v1.0/myorg/admin/capacities')

def norm_item(it: dict, ws: dict) -> dict:
    row = {
        'item_id':        it.get('id'),
        'item_name':      it.get('displayName') or it.get('name'),
        'item_type':      it.get('type'),
        'description':    it.get('description'),
        'workspace_id':   ws.get('id'),
        'workspace_name': ws.get('name'),
        '_raw': it,
    }
    row.update(_audit(it))
    return _wrap(row, '/v1/admin/workspaces/{id}/items')

def norm_pipeline(it: dict, ws: dict) -> dict:
    row = {
        'pipeline_id':    it.get('id'),
        'pipeline_name':  it.get('displayName') or it.get('name'),
        'description':    it.get('description'),
        'workspace_id':   ws.get('id'),
        'workspace_name': ws.get('name'),
        '_raw': it,
    }
    row.update(_audit(it))
    return _wrap(row, '/v1/admin/workspaces/{id}/items?type=DataPipeline')

def norm_dfgen2(it: dict, ws: dict) -> dict:
    row = {
        'dataflow_id':    it.get('id'),
        'dataflow_name':  it.get('displayName') or it.get('name'),
        'description':    it.get('description'),
        'workspace_id':   ws.get('id'),
        'workspace_name': ws.get('name'),
        '_raw': it,
    }
    row.update(_audit(it))
    return _wrap(row, '/v1/admin/workspaces/{id}/items?type=Dataflow')

def norm_pbi_dataset(d: dict, ws_lookup: dict) -> dict:
    ws_id = d.get('workspaceId')
    row = {
        'dataset_id':     d.get('id'),
        'dataset_name':   d.get('name'),
        'configured_by':  d.get('configuredBy'),
        'is_refreshable': d.get('isRefreshable'),
        'is_effective_identity_required':       d.get('isEffectiveIdentityRequired'),
        'is_effective_identity_roles_required': d.get('isEffectiveIdentityRolesRequired'),
        'workspace_id':   ws_id,
        'workspace_name': ws_lookup.get(ws_id),
        '_raw': d,
    }
    row.update(_audit(d))
    return _wrap(row, '/v1.0/myorg/admin/datasets')

def norm_pbi_report(r: dict, ws_lookup: dict) -> dict:
    ws_id = r.get('workspaceId')
    ds_id = r.get('datasetId')
    row = {
        'report_id':            r.get('id'),
        'report_name':          r.get('name'),
        'report_type':          r.get('reportType'),
        'dataset_id':           ds_id,
        'dataset_workspace_id': r.get('datasetWorkspaceId'),
        'dataset_link_type':    'DIRECT' if ds_id else 'UNKNOWN',
        'web_url':              r.get('webUrl'),
        'embed_url':            r.get('embedUrl'),
        'workspace_id':         ws_id,
        'workspace_name':       ws_lookup.get(ws_id),
        '_raw': r,
    }
    row.update(_audit(r))
    return _wrap(row, '/v1.0/myorg/admin/reports')

def norm_pbi_dashboard(d: dict, ws_lookup: dict) -> dict:
    ws_id = d.get('workspaceId')
    row = {
        'dashboard_id':   d.get('id'),
        'dashboard_name': d.get('displayName') or d.get('name'),
        'is_read_only':   d.get('isReadOnly'),
        'web_url':        d.get('webUrl'),
        'workspace_id':   ws_id,
        'workspace_name': ws_lookup.get(ws_id),
        '_raw': d,
    }
    row.update(_audit(d))
    return _wrap(row, '/v1.0/myorg/admin/dashboards')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. MERGE upsert + soft-delete
# 
# 1. **Upsert** por chave natural — atualiza tudo se mudou, insere se novo.
# 2. **Soft-delete**: linhas existentes não vistas neste `run_id` recebem `is_active=false`.

# CELL ********************

def merge_upsert(df: DataFrame, table_name: str) -> dict:
    spec = TABLES[table_name]
    key_cols = spec['key']
    full_table = f'{target_database}.{table_name}'

    target = DeltaTable.forName(spark, full_table)
    cond = ' AND '.join([f't.{k} = s.{k}' for k in key_cols])

    # Filtra apenas linhas com chave preenchida
    df_valid = df
    for k in key_cols:
        df_valid = df_valid.filter(F.col(k).isNotNull())

    update_set = {c: f's.{c}' for c, _ in spec['cols'] + AUDIT_COLS + CONTROL_COLS}
    (
        target.alias('t')
        .merge(df_valid.alias('s'), cond)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    if soft_delete_enabled:
        spark.sql(f'''
            MERGE INTO {full_table} t
            USING (SELECT * FROM {full_table} WHERE run_id <> '{RUN_ID}' AND is_active = true) stale
            ON {' AND '.join([f't.{k} = stale.{k}' for k in key_cols])}
            WHEN MATCHED THEN UPDATE SET t.is_active = false
        ''')

    cnt = spark.table(full_table).count()
    active = spark.table(full_table).filter('is_active = true').count()
    return {'table': table_name, 'total_rows': cnt, 'active_rows': active}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Execução — coleta, normaliza, prepara DataFrames

# CELL ********************

print('=== 1/4 Workspaces ===')
ok, ws_raw = safe_call(list_workspaces)
if not ok:
    raise RuntimeError(f'Falha crítica ao listar workspaces: {ws_raw}')

if workspace_filter:
    ws_raw = [w for w in ws_raw if w.get('id') in workspace_filter]
print(f'  workspaces fetched: {len(ws_raw)}')

ws_lookup = {w.get('id'): w.get('name') for w in ws_raw}
df_ws = df_from_rows([norm_workspace(w) for w in ws_raw], 'dim_workspace_fabric')
df_ws.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('=== 2/4 Capacities ===')
ok, cap_raw = safe_call(list_capacities)
if not ok:
    record_error(None, None, 'capacities', '/admin/capacities', cap_raw)
    cap_raw = []
print(f'  capacities fetched: {len(cap_raw)}')
df_cap = df_from_rows([norm_capacity(c) for c in cap_raw], 'dim_capacity_fabric')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('=== 3/4 Fabric Items (por workspace) ===')
items_all, pipelines_all, dfgen2_all = [], [], []

for i, ws in enumerate(ws_raw, 1):
    ws_id = ws.get('id')
    ws_name = ws.get('name')
    if not ws_id:
        continue

    ok, items = safe_call(list_items, ws_id)
    if not ok:
        record_error(ws_id, ws_name, 'items', '/admin/workspaces/{id}/items', items)
        continue

    if i % 20 == 0 or i == len(ws_raw):
        print(f'  [{i}/{len(ws_raw)}] {ws_name}: {len(items)} items')

    for it in items:
        t = (it.get('type') or '').lower()
        items_all.append(norm_item(it, ws))
        if t == 'datapipeline':
            pipelines_all.append(norm_pipeline(it, ws))
        elif t in ('dataflow', 'dataflowgen2'):
            dfgen2_all.append(norm_dfgen2(it, ws))

print(f'  total items={len(items_all)} | pipelines={len(pipelines_all)} | dfgen2={len(dfgen2_all)}')

df_items     = df_from_rows(items_all,     'dim_item_fabric')
df_pipelines = df_from_rows(pipelines_all, 'dim_pipeline_fabric')
df_dfgen2    = df_from_rows(dfgen2_all,    'dim_dataflow_gen2_fabric')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('=== 4/4 Power BI legado (admin) ===')

ok, ds_raw = safe_call(list_pbi_datasets)
if not ok:
    record_error(None, None, 'pbi_datasets', '/admin/datasets', ds_raw); ds_raw = []

ok, rep_raw = safe_call(list_pbi_reports)
if not ok:
    record_error(None, None, 'pbi_reports', '/admin/reports', rep_raw); rep_raw = []

ok, dash_raw = safe_call(list_pbi_dashboards)
if not ok:
    record_error(None, None, 'pbi_dashboards', '/admin/dashboards', dash_raw); dash_raw = []

print(f'  datasets={len(ds_raw)} | reports={len(rep_raw)} | dashboards={len(dash_raw)}')

df_ds   = df_from_rows([norm_pbi_dataset(d, ws_lookup)   for d in ds_raw],   'dim_dataset_pbi')
df_rep  = df_from_rows([norm_pbi_report(r, ws_lookup)    for r in rep_raw],  'fact_report_fabric')
df_dash = df_from_rows([norm_pbi_dashboard(d, ws_lookup) for d in dash_raw], 'dim_dashboard_pbi')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Persistência — MERGE em cada Bronze

# CELL ********************

to_merge = [
    (df_ws,        'dim_workspace_fabric'),
    (df_cap,       'dim_capacity_fabric'),
    (df_items,     'dim_item_fabric'),
    (df_pipelines, 'dim_pipeline_fabric'),
    (df_dfgen2,    'dim_dataflow_gen2_fabric'),
    (df_ds,        'dim_dataset_pbi'),
    (df_rep,       'fact_report_fabric'),
    (df_dash,      'dim_dashboard_pbi'),
]

results = []
for df, tname in to_merge:
    # complex_to_json não é necessário aqui pois df_from_rows já produz schema flat (STRING/TIMESTAMP/BOOLEAN),
    # mas mantemos a chamada como defesa para casos futuros.
    df = complex_to_json(df)
    src_count = df.count()
    print(f'-- merging {tname} ({src_count} rows in source)')
    res = merge_upsert(df, tname)
    print(f'   {res}')
    results.append(res)

# Persiste scan_errors (append, mantém histórico)
if SCAN_ERRORS:
    err_df = spark.createDataFrame([Row(**e) for e in SCAN_ERRORS], schema=SCAN_ERRORS_SCHEMA)
    err_df.write.format('delta').mode('append').saveAsTable(f'{target_database}.{SCAN_ERRORS_TABLE}')
    print(f'  ⚠ {len(SCAN_ERRORS)} scan errors persisted to {SCAN_ERRORS_TABLE}')
else:
    print('  ✓ scan sem erros')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. OPTIMIZE + VACUUM

# CELL ********************

if run_optimize:
    all_tables = [t for _, t in to_merge] + [SCAN_ERRORS_TABLE]
    for tname in all_tables:
        full = f'{target_database}.{tname}'
        print(f'OPTIMIZE {full}')
        spark.sql(f'OPTIMIZE {full}')
        spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')
        spark.sql(f'VACUUM {full} RETAIN {vacuum_retention_hours} HOURS')
else:
    print('OPTIMIZE/VACUUM pulado (run_optimize=False)')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Validação e exit

# CELL ********************

summary_rows = []
for _, tname in to_merge:
    full = f'{target_database}.{tname}'
    total = spark.table(full).count()
    active = spark.table(full).filter('is_active = true').count()
    this_run = spark.table(full).filter(f"run_id = '{RUN_ID}'").count()
    summary_rows.append((tname, total, active, this_run))

summary_df = spark.createDataFrame(summary_rows, ['table', 'total', 'active', 'this_run'])
summary_df.show(truncate=False)

errors_count = len(SCAN_ERRORS)
exit_payload = {
    'run_id': RUN_ID,
    'run_timestamp_utc': RUN_TS_UTC.isoformat(),
    'env': env,
    'scan_errors': errors_count,
    'tables': [dict(zip(['table','total','active','this_run'], r)) for r in summary_rows],
}
print(json.dumps(exit_payload, indent=2, default=str))

# Em pipeline Fabric:
# notebookutils.notebook.exit(json.dumps(exit_payload))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
