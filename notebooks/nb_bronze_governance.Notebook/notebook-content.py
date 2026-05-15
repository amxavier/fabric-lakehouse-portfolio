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
# **Purpose:** criar tabelas de governança Bronze no workspace DEV.

# MARKDOWN ********************

# ### Create Bronze governance tables

# CELL ********************

from datetime import datetime

print('Starting governance Bronze notebook')
print('Default lakehouse target is the active Spark session')

# Define the Bronze governance table schema

tables = {
    'fabric_governance_pipelines': [
        ('id', 'STRING'),
        ('name', 'STRING'),
        ('type', 'STRING'),
        ('workspace_id', 'STRING'),
        ('raw', 'STRING'),
        ('last_updated', 'TIMESTAMP'),
        ('discovered_at', 'TIMESTAMP'),
    ],
    'fabric_governance_dfgen2': [
        ('id', 'STRING'),
        ('name', 'STRING'),
        ('type', 'STRING'),
        ('workspace_id', 'STRING'),
        ('raw', 'STRING'),
        ('last_updated', 'TIMESTAMP'),
        ('discovered_at', 'TIMESTAMP'),
    ],
    'fabric_governance_workspaces': [
        ('id', 'STRING'),
        ('name', 'STRING'),
        ('tenant_id', 'STRING'),
        ('raw', 'STRING'),
        ('discovered_at', 'TIMESTAMP'),
    ],
}


def create_table_ddl(table_name, cols):
    cols_ddl = ',\n        '.join([f'{name} {dtype}' for name, dtype in cols])
    return f'CREATE TABLE IF NOT EXISTS {table_name} (\n        {cols_ddl}\n    ) USING DELTA'


for table_name, cols in tables.items():
    ddl = create_table_ddl(table_name, cols)
    print(f'Executing DDL for {table_name}')
    print(ddl)
    spark.sql(ddl)
    print(f'Table created or already exists: {table_name}')


# Validate created tables

# CELL ********************

for table_name in tables.keys():
    exists = spark.catalog.tableExists(table_name)
    print(f'{table_name}: exists={exists}')

print('Current default database tables:')
spark.sql('SHOW TABLES').show(truncate=False)
