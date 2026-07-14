# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## NB_FMD_LOAD_DEMO_DATA
# 
# Creates the table the demo entity points at.
# 
# `load_demo_data = True` registers `[in].[customer]` in `integration.LandingzoneEntity`,
# `BronzeLayerEntity` and `SilverLayerEntity`, but nothing ever creates the table itself.
# A fresh deployment therefore loads nothing and reports success.
# 
# `NB_SETUP_FMD` cannot do it: it is a pure Python notebook with no Spark session, and it
# downloads only `src/` and `config/`, not `demodata/`. This notebook has Spark, so the
# setup invokes it once when `load_demo_data` is True.

# PARAMETERS CELL ********************

data_workspace_guid = ''
landingzone_lakehouse_guid = ''
lakehouse_schema_enabled = True
repo_owner = 'edkreuk'
repo_name = 'FMD_FRAMEWORK'
branch = 'main'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import requests
from io import StringIO

url = (f'https://raw.githubusercontent.com/{repo_owner}/{repo_name}/'
       f'{branch}/demodata/customer.csv')
response = requests.get(url, timeout=120)
response.raise_for_status()

# dtype=str: the landing zone is raw data. Bronze does the typing, and letting pandas
# guess here is what makes a column change type between runs.
pdf = pd.read_csv(StringIO(response.text), dtype=str).fillna('')
print(f'demodata/customer.csv: {len(pdf)} rows, {len(pdf.columns)} columns')

df = spark.createDataFrame(pdf)

# The demo entity is registered as SourceSchema 'in', SourceName 'customer'.
table_path = 'in/customer' if lakehouse_schema_enabled else 'in_customer'
target = (f'abfss://{data_workspace_guid}@onelake.dfs.fabric.microsoft.com/'
          f'{landingzone_lakehouse_guid}/Tables/{table_path}')

df.write.format('delta').mode('overwrite').save(target)
print(f'Demo table written to {target}')

# The SQL analytics endpoint syncs asynchronously, so INFORMATION_SCHEMA can still be
# empty for a while after this. OneLake is the source of truth, not SQL.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
