# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#pip install pyapacheatlas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import os
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, lower
import json
import pyodbc
import struct
from azure.identity import ClientSecretCredential
# pyapacheatlas

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess
from pyapacheatlas.core.typedef import AtlasAttributeDef, EntityTypeDef
from pyapacheatlas.core.util import AtlasException, GuidTracker
from pyapacheatlas.core import EntityTypeDef, RelationshipTypeDef

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

BrzTableName01= 'Purchasing_PurchaseOrders'
SlvTableName01= 'Purchasing_PurchaseOrders'
LdzFileName01='fmd/wwi/2026/01/19/Purchasing_PurchaseOrderLines_202601191731.parquet'

BrzTableSchema01= 'wwi'
SlvTableSchema01= 'wwi'

LdzWorkspaceName='INTEGRATION DATA (D)'
BrzWorkspaceName='INTEGRATION DATA (D)'
SlvWorkspaceName='INTEGRATION DATA (D)'

LdzLakehouseName='LH_DATA_LANDINGZONE'
BrzLakehouseName='LH_BRONZE_LAYER'
SlvLakehouseName='LH_SILVER_LAYER'

LdzWorkspace=''
BrzWorkspace=''
SlvWorkspace=''

LdzLakehouse=''
BrzLakehouse=''
SlvLakehouse=''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config = {
    "Ldz": {
        "workspace_name": LdzWorkspaceName,
        "workspace_id": LdzWorkspace,
        "lakehouse_name": '',
        "lakehouse_id": ''
    },
    "Brz": {
        "workspace_name": BrzWorkspaceName,
        "workspace_id": BrzWorkspace,
        "lakehouse_name": BrzLakehouseName,
        "lakehouse_id": BrzLakehouse
    },
    "Slv": {
        "workspace_name": SlvWorkspaceName,
        "workspace_id": SlvWorkspace,
        "lakehouse_name": SlvLakehouseName,
        "lakehouse_id": SlvLakehouse
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------------------------------------------------------------
# Configuration / Parameters
# Supply via environment variables or by directly editing defaults below.
# --------------------------------------------------------------------------------------
default_settings=notebookutils.variableLibrary.getLibrary("VAR_FMD")

tenant_id="tenantid"
client_id="sp-fabric-purview-deployment-appid"
key_vault =default_settings.key_vault_uri_name
secret_name='sp-fabric-purview-deployment-secret'
PurviewAccount_name="ededeunpview01"

LdzLayerName = 'DataLandingzone'
BrzLayerName = BrzLakehouseName.removeprefix("LH_").removesuffix("_LAYER")
SlvLayerName = SlvLakehouseName.removeprefix("LH_").removesuffix("_LAYER")

qualified_name_Ldz01=f"https://app.fabric.microsoft.com/groups/{LdzWorkspace}/lakehouses/{LdzLakehouse}/files/{LdzFileName01}"
qualified_name_Brz01=f"https://app.fabric.microsoft.com/groups/{BrzWorkspace}/lakehouses/{BrzLakehouse}/tables/{BrzTableSchema01.lower()}%252F{BrzTableName01.lower()}"
qualified_name_Slv01=f"https://app.fabric.microsoft.com/groups/{SlvWorkspace}/lakehouses/{SlvLakehouse}/tables/{SlvTableSchema01.lower()}%252F{SlvTableName01.lower()}"

#print("qualified_name_Brz01:" + qualified_name_Brz01)
#print("qualified_name_Slv01:" + qualified_name_Slv01)
ldzprocesstype='Notebook'
processtype='Notebook'   #NotebookName, Shortcut, MLV
LdzTypeName01='fabric_lakehouse_path' 
BrzTypeName01 = 'fabric_lakehouse_table'
SlvTypeName01 = 'fabric_lakehouse_table'

driver = '{ODBC Driver 18 for SQL Server}'

if LdzFileName01 == '/':
   LoadLanding=False
else:
   LoadLanding=True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_key_vault = f'https://{key_vault}.vault.azure.net/'
purview_tenant_id = notebookutils.credentials.getSecret(notebook_key_vault, tenant_id) 
purview_client_id  = notebookutils.credentials.getSecret(notebook_key_vault, client_id) 
purview__client_secret  = notebookutils.credentials.getSecret(notebook_key_vault, secret_name) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query_tables=(
    f"SELECT t.name "  
    f",s.name as TableSchema "  
    f",t.object_id "  
    f",t.modify_date "  
    f",t.type "  
    f",t.type_desc "  
    f"FROM sys.tables t "
    f"JOIN sys.schemas s ON t.schema_id = s.schema_id "  
    f"where t.name = ('" )
query_tables2="') " 
query_columns_template1=(
    f"SELECT c.name AS column_name "  
    f",c.column_id "  
    f",SCHEMA_NAME(t.schema_id) AS type_schema "  
    f",t.name AS type_name "  
    f",t.is_user_defined "  
    f",t.is_assembly_type "  
    f",c.max_length "  
    f",c.precision "  
    f",c.scale "  
    f"FROM sys.columns AS c "
    f"JOIN sys.types AS t ON c.user_type_id=t.user_type_id "  
    f"WHERE c.object_id = OBJECT_ID('"
)
query_columns_template2="') ORDER BY c.column_id" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# record template for table columns metadata
df_columns_datarow={"WorkspaceName": "dummy_column1", "WorkspaceID": "dummy_id", \
        "LakehouseName": "", \
        "LakehouseID": "", \
        "TableName": "", \
        "TableSchema": "", \
        "ColumnName": "", \
        "ColumnType": "", \
        "ColumnID":""}

schema_df_columns = StructType([ \
    StructField("WorkspaceName",StringType(),True), \
    StructField("WorkspaceID",StringType(),True), \
    StructField("LakehouseName",StringType(),True), \
    StructField("LakehouseID",StringType(),True), \
    StructField("TableName",StringType(),True), \
    StructField("TableSchema",StringType(),True), \
    StructField("ColumnName", StringType(), True), \
    StructField("ColumnType", StringType(), True), \
    StructField("ColumnID", StringType(), True) \
  ])
 
df_columns = spark.createDataFrame(data=[],schema=schema_df_columns)
#df_columns.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sqlclient = ClientSecretCredential(
        tenant_id=purview_tenant_id,
        client_id=purview_client_id,
        client_secret=purview__client_secret
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

oauth = ServicePrincipalAuthentication(
        tenant_id=purview_tenant_id,
        client_id=purview_client_id,
        client_secret=purview__client_secret
    )
client = PurviewClient(
        account_name=PurviewAccount_name,
        authentication=oauth
    )
created_GUIDs=[] # used to keep track of newly created Purview items, in case we want to move such items to a specific collection  

guid = GuidTracker()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

process_type = EntityTypeDef(
                        name =  "FMD Lineage_Extractor_Process",
                        serviceType =  "FMD Lineage_Extractor_Process",
                        
                        attributeDefs = [
                          AtlasAttributeDef(name = "schedule",
                                              defaultValue = "adHoc"),
                          AtlasAttributeDef(name = "createTime"),
                          AtlasAttributeDef("columnMapping"),
                          AtlasAttributeDef(name="dataLayer", isOptional=True),
                        ],
                        superTypes = ["Process"]
                    )

# Upload the new asset type definition
typedef_results = client.upload_typedefs(entityDefs=[process_type], force_update=True)
#print(typedef_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get token for Fabric authentication
fabric_token = notebookutils.credentials.getToken('https://analysis.windows.net/powerbi/api')

# Get token for Azure SQL authentication
token = notebookutils.credentials.getToken('https://analysis.windows.net/powerbi/api').encode("UTF-16-LE")

access_token = sqlclient.get_token("https://database.windows.net/.default").token

# Azure SQL ODBC expects the token as UTF-16-LE bytes
token_bytes = access_token.encode("UTF-16-LE")

token_struct = struct.pack(
    f"<I{len(token_bytes)}s",
    len(token_bytes),
    token_bytes
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_fabric_session(fabric_token: str):
    fabric_headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {fabric_token}'
    }
    fabric_session = requests.Session()
    fabric_session.headers.update(fabric_headers)
    return fabric_session

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_session = create_fabric_session(fabric_token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resolved_lakehouses = {}
rows = []

for environment, settings in config.items():
    workspace_id = settings["workspace_id"]
    workspace_name = settings["workspace_name"]
    expected_lakehouse_name = settings["lakehouse_name"]

    items_response = fabric_session.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    )
    items_response.raise_for_status()

    artifacts_list = items_response.json().get("value", [])

    for item in artifacts_list:
        if item["type"] == "Lakehouse" and item["displayName"] == expected_lakehouse_name:
            item_name = item["displayName"]
            item_type = item["type"]
            item_id = item["id"]

            print(f'\n{item_name} in {workspace_name} is of type {item_type} ' f'and has guid {item_id}'  )

            lakehouse_response = fabric_session.get(
                f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{item_id}"
            )
            lakehouse_response.raise_for_status()

            lakehouse_json = lakehouse_response.json()

            connstring = lakehouse_json["properties"]["sqlEndpointProperties"]["connectionString"]

            print(f"SQLconn_string for {item_name} is {connstring}")

            conn = None

            try:
                conn = pyodbc.connect(
                    f"DRIVER={driver};"
                    f"SERVER={connstring};"
                    f"PORT=1433;"
                    f"DATABASE={item_name};",
                    attrs_before={1256: token_struct},
                    timeout=12
                )

                conn.timeout = 10
                #make sure we get the correct tables from the correct lakehouse
 
                table_mapping = {
                        config["Brz"]["lakehouse_name"]: {
                            "schema": BrzTableSchema01.lower(),
                            "table": BrzTableName01
                        },
                        config["Slv"]["lakehouse_name"]: {
                            "schema": SlvTableSchema01.lower(),
                            "table": SlvTableName01
                        }
                    }

                table_info = table_mapping[item_name]

                query_columns = (
                    f"{query_columns_template1}"
                    f"{table_info['schema']}.{table_info['table']}"
                    f"{query_columns_template2}"
                )

                #print(query_columns)

                with conn.cursor() as cursor:
                    # Warm-up
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

                    cursor.execute(query_columns)
                    column_list = cursor.fetchall()

                    for column in column_list:
                        new_dict = df_columns_datarow.copy()

                        new_dict.update({
                            "WorkspaceName": workspace_name,
                            "WorkspaceID": workspace_id,
                            "LakehouseName": item_name,
                            "LakehouseID": item_id,
                            "TableSchema": BrzTableSchema01,
                            "TableName": BrzTableName01,
                            "ColumnName": column[0],
                            "ColumnID": column[1],
                            "ColumnType": column[3]
                        })

                        rows.append(new_dict)

            finally:
                if conn is not None:
                    conn.close()
df_columns = spark.createDataFrame(data=rows, schema=schema_df_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Found Table columns for {BrzTableName01}:")

#display(df_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Split Brz and Slv
Brz = df_columns.filter(col("LakehouseName") == BrzLakehouseName)
Slv = df_columns.filter(col("LakehouseName") == SlvLakehouseName)

full_join = Brz.alias("b").join(
    Slv.alias("s"),
    on=[
        col("b.TableSchema") == col("s.TableSchema"),
        col("b.TableName") == col("s.TableName"),
        col("b.ColumnName") == col("s.ColumnName")
    ],
    how="full_outer"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows = full_join.select(
    col("b.ColumnName").alias("Brz"),
    col("s.ColumnName").alias("Slv")
).collect()


column_mapping = [{
    "ColumnMapping": [
        {
            "Source": row["Brz"] if row["Brz"] else "*",
            "Sink": row["Slv"] if row["Slv"] else "*"
        }
        for row in rows
    ],
    "DatasetMapping": {
        "Source": qualified_name_Brz01,
        "Sink": qualified_name_Slv01
    }
}]
column_mapping

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if LoadLanding:
    try:
        existing = client.get_entity(
            qualifiedName=f"{qualified_name_Ldz01}",
            typeName=f"{LdzTypeName01}",
        )

        guid = existing["entities"][0]["guid"]

        print(f"✅ Existing entity found")
        print(f"   Name: {existing['entities'][0]['attributes']['name']}")
        print(f"   QualifiedName: {qualified_name_Ldz01}")
        print(f"   GUID: {guid}")

        Ldz_table = AtlasEntity(
            name=f"{LdzFileName01}",
            typeName=f"{LdzTypeName01}",
            qualified_name=f"{qualified_name_Ldz01}",
            guid=guid
        )

    except Exception:
        print(f"🆕 No existing entity found → creating new one")
        print(f"   QualifiedName: {qualified_name_Ldz01}")

        Ldz_table = AtlasEntity(
            name=f"{LdzFileName01}",
            typeName=f"{LdzTypeName01}",
            qualified_name=f"{qualified_name_Ldz01}",
            guid="-1"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    existing = client.get_entity(
        qualifiedName=f"{qualified_name_Brz01}",
        typeName=f"{BrzTypeName01}",
    )

    guid = existing["entities"][0]["guid"]

    print(f"✅ Existing entity found")
    print(f"   Name: {existing['entities'][0]['attributes']['name']}")
    print(f"   QualifiedName: {qualified_name_Brz01}")
    print(f"   GUID: {guid}")

    Brz_table = AtlasEntity(
        name=f"{BrzTableName01}",
        typeName=f"{BrzTypeName01}",
        qualified_name=f"{qualified_name_Brz01}",
        guid=guid
    )

except Exception:
    print(f"🆕 No existing entity found → creating new one")
    print(f"   QualifiedName: {qualified_name_Brz01}")

    Brz_table = AtlasEntity(
        name=f"{BrzTableName01}",
        typeName=f"{BrzTypeName01}",
        qualified_name=f"{qualified_name_Brz01}",
        guid="-1"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    existing = client.get_entity(
        qualifiedName=f"{qualified_name_Slv01}",
        typeName=f"{SlvTypeName01}",
    )

    guid = existing["entities"][0]["guid"]

    print(f"✅ Existing entity found")
    print(f"   Name: {existing['entities'][0]['attributes']['name']}")
    print(f"   QualifiedName: {qualified_name_Slv01}")
    print(f"   GUID: {guid}")

    Slv_table = AtlasEntity(
        name=f"{SlvTableName01}",
        typeName=f"{SlvTypeName01}",
        qualified_name=f"{qualified_name_Slv01}",
        guid=guid
    )

except Exception:
    print(f"🆕 No existing entity found → creating new one")
    print(f"   QualifiedName: {qualified_name_Slv01}")

    Slv_table = AtlasEntity(
        name=f"{SlvTableName01}",
        typeName=f"{SlvTypeName01}",
        qualified_name=f"{qualified_name_Slv01}",
        guid="-1"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if LoadLanding:
    try:
        process_qn = f"process://{LdzWorkspaceName}/{LdzLayerName}/{LdzFileName01}->{BrzLayerName}/{BrzTableSchema01}/{BrzTableName01}"

        try:
            existing = client.get_entity(
                qualifiedName=process_qn,
                typeName="FMD Lineage_Extractor_Process"
            )
            process_guid = existing["entities"][0]["guid"]
        except:
            process_guid = "-1"  # new process

        process = AtlasProcess(
            name=f"{ldzprocesstype} Lineage {LdzFileName01} ({LdzLayerName} to {BrzLayerName})",
            typeName="FMD Lineage_Extractor_Process",
            description="Lineage Created by FMD Purview Accelerator",
            qualified_name=process_qn,
            labels= [LdzLayerName],
            inputs=[Ldz_table],
            outputs=[Brz_table],
            guid=process_guid,
            attributes={
            "columnMapping": json.dumps(column_mapping),
            "userDescription": f'Fabric Notebook Mapping contained within workspace {LdzWorkspaceName}.{LdzLayerName}.{LdzFileName01}->{BrzWorkspaceName}.{BrzLayerName}.{BrzTableName01}'}
        
    ) 
        results = client.upload_entities(
            batch=[Ldz_table, Brz_table, process]  # ✅ order matters
        )

        print(json.dumps(results, indent=2))

        sink_guid = results["guidAssignments"].get(str(Brz_table.guid))
        print(f'Search for "{Brz_table.name}" or use guid {sink_guid}')

    except Exception as e:
        print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    process_qn = f"process://{BrzWorkspaceName}/{BrzLayerName}/{BrzTableSchema01}/{BrzTableName01}->{SlvLayerName}/{SlvTableSchema01}/{SlvTableName01}"

    try:
        existing = client.get_entity(
            qualifiedName=process_qn,
            typeName="FMD Lineage_Extractor_Process"
        )
        process_guid = existing["entities"][0]["guid"]
    except:
        process_guid = "-1"  # new process

    process = AtlasProcess(
        name=f"{processtype} Lineage {BrzTableName01} ({BrzLayerName} to {SlvLayerName})",
        typeName="FMD Lineage_Extractor_Process",
        description="Lineage Created by FMD Purview Accelerator",
        qualified_name=process_qn,
        labels= [BrzLayerName],
        inputs=[Brz_table],
        outputs=[Slv_table],
        guid=process_guid,
        attributes={
        "columnMapping": json.dumps(column_mapping),
         "userDescription": f'Fabric Notebook Mapping contained within workspace {BrzWorkspaceName}.{BrzLayerName}.{BrzTableName01}->{SlvWorkspaceName}.{SlvLayerName}.{SlvTableName01}'}
       
) 
    results = client.upload_entities(
        batch=[Brz_table, Slv_table, process]  # ✅ order matters
    )

    print(json.dumps(results, indent=2))

    sink_guid = results["guidAssignments"].get(str(Slv_table.guid))
    print(f'Search for "{Slv_table.name}" or use guid {sink_guid}')

except Exception as e:
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
