# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # NB_FMD_FABRIC_PURVIEW_LINEAGE_TABLE_COLUMN_EXTRACTOR
# Extracts table and column-level lineage from Microsoft Fabric Lakehouses and registers it in Microsoft Purview. Supports creating lineage processes with column mapping for data governance and asset tracking.
# 
# ## Prerequisites
# Before running this notebook, ensure the following are in place:
# 
# ### Service Principal
# A dedicated **service principal (App Registration)** is required to authenticate against Microsoft Purview.
# 
# - 1 | Create a service principal in Azure Active Directory (App Registration) |
# - 2 | Store the `tenant_id`, `client_id`, and `client_secret` in **Azure Key Vault** using the secret names configured in the parameters cell |
# 
# ### Microsoft Fabric — Workspace Access
# The service principal must be added as a **Viewer** to each data workspace that contains the source and target Lakehouses and the configuration database
# 
# > Navigate to each Fabric workspace → **Manage access** → Add the service principal as **Viewer**.
# 
# ### Microsoft Purview — Role Assignment
# The service principal must have one of the following roles assigned in Microsoft Purview to register lineage and manage data assets:
# 
# - **Data Curator** — grants read/write access to data assets and lineage
# - **Data Source Admin** — grants full control over data source registration and scanning
# 
# > Navigate to **Microsoft Purview** → **Data Map** → **Collections** → select your collection → **Role assignments** → add the service principal to the desired role.
# 
# ## Parameters
# 
# - tenant_id="**tenantid**"                              #or your own secret name
# - client_id="**sp-fabric-purview-deployment-appid**"    #or your own secret name
# - key_vault = defined in variable library VAR_FMD
# - secret_name="**sp-fabric-purview-deployment-secret**" #or your own secret name
# - PurviewAccount_name=defined in variable library VAR_FMD
# - process_type="**ShortCut, Notebook, MLV**" what kind of lineage are you creating
# 
# ## Remarks
# - Columns without mapping will be marked in Purview with a * 
# 
# - All Lineage items can be found in the **Microsoft Purview** → **Unified Catalog** → **Data assets** → **Browse by source type** → **Search results** → **FMD Fabric to Purview Lineage Extractor Process**
# 
# 
# ## Limitations
# - Only one source and one target can be defined (one to one relation)
# - Only Lakehouses are supported


# CELL ********************

pip install pyapacheatlas

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

# CELL ********************

# --------------------------------------------------------------------------------------
# Configuration / Parameters
# Supply via environment variables or by directly editing defaults below.
# --------------------------------------------------------------------------------------

config_settings=notebookutils.variableLibrary.getLibrary("VAR_CONFIG_FMD")
default_settings=notebookutils.variableLibrary.getLibrary("VAR_FMD")


tenant_id="tenantid"
client_id="sp-fabric-purview-deployment-appid"
key_vault =default_settings.key_vault_uri_name
secret_name='sp-fabric-purview-deployment-secret'
PurviewAccount_name=default_settings.purview_account_name

LdzLayerName = 'DataLandingzone'
BrzLayerName = 'Bronze'
SlvLayerName = 'Silver'

ldzprocesstype='Copy Activity'
processtype='Notebook'   #NotebookName, Shortcut, MLV
fabric_path_type_name='fabric_lakehouse_path' 
fabric_table_type_name = 'fabric_lakehouse_table'

driver = '{ODBC Driver 18 for SQL Server}'
process_type_name = "FMD Fabric to Purview Lineage Extractor Process"



connstring_sql=config_settings.fmd_fabric_db_connection
database=config_settings.fmd_fabric_db_name


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parameters from Data Pipeline

# PARAMETERS CELL ********************

SourceWorkspaceId="3e43b742-3e41-4ec0-a414-03adf83c08e7"

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

def get_or_create_entity(
    entity_name: str,
    type_name: str,
    qualified_name: str,
    temp_guid: str = "-100"
):
    """
    Returns an AtlasEntity.
    If the entity already exists, the existing GUID is reused.
    Otherwise a new AtlasEntity is returned with a temporary negative GUID.
    """

    try:
        existing = client.get_entity(
            qualifiedName=qualified_name,
            typeName=type_name,
        )

        entity_guid = existing["entities"][0]["guid"]

        print("✅ Existing entity found")
        print(f"   Name: {existing['entities'][0]['attributes']['name']}")
        print(f"   QualifiedName: {qualified_name}")
        print(f"   GUID: {entity_guid}")

        return AtlasEntity(
            name=entity_name,
            typeName=type_name,
            qualified_name=qualified_name,
            guid=entity_guid
        )

    except Exception:
        print("🆕 No existing entity found → creating new one")
        print(f"   QualifiedName: {qualified_name}")

        return AtlasEntity(
            name=entity_name,
            typeName=type_name,
            qualified_name=qualified_name,
            guid=temp_guid
        )

def create_lineage_process(
    input_entity: str,
    output_entity: str,
    process_type_name: str,
    process_name: str,
    process_qn: str,
    temp_guid: str = "-100",
    labels=None,
    column_mapping=None
):
    """
    Create or update a Purview lineage process.
    """
    try:
        try:
            existing = client.get_entity(
                qualifiedName=process_qn,
                typeName="FMD Fabric to Purview Lineage Extractor Process"
            )
            process_guid = existing["entities"][0]["guid"]
        except:
            process_guid = "-1"  # new process
        labels = labels or []
        process_guid=temp_guid
        process = AtlasProcess(
        name=process_name,
        typeName=process_type_name,
        description="Lineage Created by FMD Fabric Purview Accelerator",
        qualified_name=process_qn,
        labels=[labels],
        inputs=[input_entity],
        outputs=[output_entity],
        guid=process_guid,
        attributes={
            "columnMapping": json.dumps(column_mapping),

            "userDescription": f'<div>Lineage Created by FMD Fabric Purview Accelerator</p> <p> Process {process_qn}</p>'

        },
        # This custom attribute flips a switch inside of the Purview UI to render
        # the rich text description.
        customAttributes={
            "microsoft_isDescriptionRichText": "true"
        }
        )
        results = client.upload_entities(
                batch=[
                    input_entity,
                    output_entity,
                    process
                ]
            )
        print(f"   QualifiedName: {process_qn}")
        print("✅ Lineage created")
        process_guid = results["guidAssignments"].get(str(process.guid))
        print(f'Search for "{process.name}" or use guid {process_guid}')

    except Exception as e:
            print(f"ERROR: {type(e).__name__}: {e}")

        
    return results, process_guid


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sqlquery=(
f"SELECT "
    f"s.name  AS SchemaName, "
    f"t.name  AS TableName, "
    f"c.name  AS ColumnName, "
    f"c.column_id as ColumnId, "
    f"ty.name AS ColumnType, "
    f"c.max_length, "
    f"c.precision, "
    f"c.scale "
f"FROM sys.tables t "
f"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
f"INNER JOIN sys.columns c ON t.object_id = c.object_id "
f"INNER JOIN sys.types AS ty ON c.user_type_id=ty.user_type_id"
f" where 1=1"
)



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
        "ColumnID": "", \
        "PurviewFQN":""}

schema_df_columns = StructType([ \
    StructField("WorkspaceName",StringType(),True), \
    StructField("WorkspaceID",StringType(),True), \
    StructField("LakehouseName",StringType(),True), \
    StructField("LakehouseID",StringType(),True), \
    StructField("TableName",StringType(),True), \
    StructField("TableSchema",StringType(),True), \
    StructField("ColumnName", StringType(), True), \
    StructField("ColumnType", StringType(), True), \
    StructField("ColumnID", StringType(), True), \
    StructField("PurviewFQN", StringType(), True) \
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
                        name =  process_type_name,
                        serviceType =  process_type_name,
                        
                        attributeDefs = [
                          AtlasAttributeDef(name = "schedule",
                                              defaultValue = "adHoc"),
                          AtlasAttributeDef(name = "createTime"),
                          AtlasAttributeDef(name="dataLayer", isOptional=True),
                          AtlasAttributeDef("columnMapping")
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

#Create Fabric Database Connection
conn_sql = pyodbc.connect(
    f"DRIVER={driver};"
    f"SERVER={connstring_sql};"
    f"PORT=1433;"
    f"DATABASE={database};",
    attrs_before={1256: token_struct},
    timeout=12
)

query = """
SELECT
    DS.Namespace AS BrzTableSchema01,
    DS.Namespace AS SlvTableSchema01,
    CONCAT(LZE.FilePath,'/',DS.NameSpace,'/',LZE.FileName) as LdzFolderPath,
    LZE.FileName as LdzFolder,
    CONCAT(BLE.[Schema], '_', BLE.[Name]) AS BrzTableName01,
    CONCAT(SLE.[Schema], '_', SLE.[Name]) AS SlvTableName01,
    WL.[WorkspaceGuid] AS LdzWorkspaceId,
    WB.[WorkspaceGuid] AS BrzWorkspaceId,
    WS.[WorkspaceGuid] AS SlvWorkspaceId,
    LLH.[LakehouseGuid] AS LdzLakehouseId,
    BLH.[LakehouseGuid] AS BrzLakehouseId,
    SLH.[LakehouseGuid] AS SlvLakehouseId,
    LLH.[Name] AS LdzLakehouseName,
    BLH.[Name] AS BrzLakehouseName,
    SLH.[Name] AS SlvLakehouseName,
    WL.[Name] AS LdzWorkspaceName,
    WB.[Name] AS BrzWorkspaceName,
    WS.[Name] AS SlvWorkspaceName
FROM [integration].[SilverLayerEntity] SLE
INNER JOIN [integration].[BronzeLayerEntity] BLE
    ON SLE.BronzeLayerEntityId = BLE.BronzeLayerEntityId
INNER JOIN [integration].[LandingzoneEntity] LZE
    ON LZE.LandingzoneEntityId = BLE.LandingzoneEntityId
INNER JOIN [integration].[DataSource] DS
    ON DS.[DataSourceId] = LZE.[DataSourceId]
INNER JOIN [integration].[Lakehouse] LLH
    ON LZE.LakehouseId = LLH.LakehouseId
INNER JOIN [integration].[Lakehouse] BLH
    ON BLE.LakehouseId = BLH.LakehouseId
INNER JOIN [integration].[Lakehouse] SLH
    ON SLE.LakehouseId = SLH.LakehouseId
INNER JOIN [integration].[Workspace] WL
    ON WL.[WorkspaceGuid] = LLH.[WorkspaceGuid]
INNER JOIN [integration].[Workspace] WB
    ON WB.[WorkspaceGuid] = BLH.[WorkspaceGuid]
INNER JOIN [integration].[Workspace] WS
    ON WS.[WorkspaceGuid] = SLH.[WorkspaceGuid]
WHERE
    LZE.IsActive = 1
    AND BLE.IsActive = 1
    AND SLE.IsActive = 1

"""
sqlquery_whereclause=(f" and WB.[WorkspaceGuid] ='{SourceWorkspaceId}'" )

query=query+sqlquery_whereclause
conn_sql.timeout = 10

with conn_sql.cursor() as cursor:
    # Warm-up
    cursor.execute("SELECT 1")
    cursor.fetchone()
    cursor.execute(query)

    rows = cursor.fetchall()

    columns = [c[0] for c in cursor.description]

df_mapping = spark.createDataFrame(
    [tuple(row) for row in rows],
    schema=columns
)

#display(df_mapping)
conn_sql.close()


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

row = (
    df_mapping
    .select(
        "LdzWorkspaceName",
        "LdzWorkspaceId",
        "LdzLakehouseName",
        "LdzLakehouseId",
        "BrzWorkspaceName",
        "BrzWorkspaceId",
        "BrzLakehouseName",
        "BrzLakehouseId",
        "SlvWorkspaceName",
        "SlvWorkspaceId",
        "SlvLakehouseName",
        "SlvLakehouseId"
    )
    .distinct()
    .first()
    .asDict()
)

config = {
    "source": {
        "workspace_name": row["BrzWorkspaceName"],
        "workspace_id": row["BrzWorkspaceId"],
        "lakehouse_name": row["BrzLakehouseName"],
        "lakehouse_id": row["BrzLakehouseId"]
    },
    "target": {
        "workspace_name": row["SlvWorkspaceName"],
        "workspace_id": row["SlvWorkspaceId"],
        "lakehouse_name": row["SlvLakehouseName"],
        "lakehouse_id": row["SlvLakehouseId"]
    }
}

SourceWorkspaceName=row["BrzWorkspaceName"]

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

            print(f"SQLconn_string for {workspace_name} and {item_name} is {connstring}")

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
 
                with conn.cursor() as cursor:
                    # Warm-up
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

                    cursor.execute(sqlquery)
                    column_list = cursor.fetchall()

                    for column in column_list:
                        new_dict = df_columns_datarow.copy()

                        new_dict.update({
                            "WorkspaceName": workspace_name,
                            "WorkspaceID": workspace_id.lower(),
                            "LakehouseName": item_name,
                            "LakehouseID": item_id.lower(),
                            "TableSchema": column[0].lower(),
                            "TableName": column[1].lower(),
                            "ColumnName": column[2],
                            "ColumnID": column[3],
                            "ColumnType": column[4],
                            "PurviewFQN": "https://app.fabric.microsoft.com/groups/"+workspace_id.lower()+"/lakehouses/"+item_id.lower()+"/tables/"+column[0].lower()+"%252F"+column[1].lower()


                            
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

print(f"Found Table columns for:")

#display(df_columns)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_lineage_mapping = (
    df_mapping
    .select(
        col("LdzWorkspaceId").alias("LdzWorkspaceId"),
        col("LdzWorkspaceName").alias("LdzWorkspaceName"),
        col("LdzLakehouseId").alias("LdzLakehouseId"),
        col("LdzLakehouseName").alias("LdzLakehouseName"),
        col("LdzFolder").alias("LdzFolder"),
        col("LdzFolderPath").alias("LdzFolderPath"),

        col("BrzWorkspaceId").alias("BrzWorkspaceId"),
        col("BrzWorkspaceName").alias("BrzWorkspaceName"),
        col("BrzLakehouseId").alias("BrzLakehouseId"),
        col("BrzLakehouseName").alias("BrzLakehouseName"),
        col("BrzTableSchema01").alias("BrzSchemaName"),
        col("BrzTableName01").alias("BrzTableName"),

        col("SlvWorkspaceId").alias("SlvWorkspaceId"),
        col("SlvWorkspaceName").alias("SlvWorkspaceName"),
        col("SlvLakehouseId").alias("SlvLakehouseId"),
        col("SlvLakehouseName").alias("SlvLakehouseName"),
        col("SlvTableSchema01").alias("SlvSchemaName"),
        col("SlvTableName01").alias("SlvTableName")
    )
    .collect()
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in df_lineage_mapping:
    row = row.asDict()

    InputWorkspaceId01 = row["LdzWorkspaceId"]
    InputWorkspaceName01 = row["LdzWorkspaceName"]
    InputLakehouseId01 = row["LdzLakehouseId"]
    InputLakehouseName01 = row["LdzLakehouseName"]
    InputFolder = row["LdzFolder"]
    InputFolderPath = row["LdzFolderPath"]

    OutputWorkspaceId01 = row["BrzWorkspaceId"]
    OutputWorkspaceName01 = row["BrzWorkspaceName"]
    OutputLakehouseId01 = row["BrzLakehouseId"]
    OutputLakehouseName01 = row["BrzLakehouseName"]
    OutputTableSchema01 = row["BrzSchemaName"]
    OutputTableName01 = row["BrzTableName"]

     
    qualified_name_input01=f"https://app.fabric.microsoft.com/groups/{InputWorkspaceId01.lower()}/lakehouses/{InputLakehouseId01.lower()}/files/{InputFolderPath}"
    qualified_name_output01=f"https://app.fabric.microsoft.com/groups/{OutputWorkspaceId01.lower()}/lakehouses/{OutputLakehouseId01.lower()}/tables/{OutputTableSchema01.lower()}%252F{OutputTableName01.lower()}"

    #create Landingzone entity and upload to Purview
    input_table= get_or_create_entity(entity_name=InputFolder, type_name=fabric_path_type_name ,qualified_name = qualified_name_folder01, temp_guid = "-1")

    #create Bronze entity and upload to Purview
    output_table=get_or_create_entity(entity_name=OutputTableName01, type_name=fabric_table_type_name ,qualified_name = qualified_name_output01, temp_guid = "-2")

    process_qn = (f"fmdprocess://{InputWorkspaceName01}/{InputFolder} to {OutputTableSchema01}/{OutputTableName01}")
    print(process_qn)
    process_name=(f"{ldzprocesstype} Lineage Folder {InputFolder} to {OutputTableSchema01}.{OutputTableName01}")
    results=create_lineage_process(input_entity=input_table, output_entity = output_table, process_type_name=process_type_name, process_name = process_name, process_qn = process_qn, temp_guid='-5', labels=BrzLayerName, column_mapping=[])
    #print(json.dumps(results, indent=2)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Filter source and sink only once
source_all = df_columns.filter(
    (col("WorkspaceID") == config["source"]["workspace_id"].lower()) &
    (col("LakehouseID") == config["source"]["lakehouse_id"].lower())
)

sink_all = df_columns.filter(
    (col("WorkspaceID") == config["target"]["workspace_id"].lower()) &
    (col("LakehouseID") == config["target"]["lakehouse_id"].lower())
)

# 2. Get distinct table mappings
table_mappings = (
    df_mapping
    .select(
        "BrzTableSchema01",
        "BrzTableName01",
        "SlvTableSchema01",
        "SlvTableName01"
    )
    .distinct()
    .collect()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# 3. Loop over each source table from the mapping table
for table in table_mappings:

    InputTableSchema01 = table["BrzTableSchema01"].lower()
    InputTableName01   = table["BrzTableName01"].lower()
    OutputTableSchema01 = table["SlvTableSchema01"].lower()
    OutputTableName01   = table["SlvTableName01"].lower()
    print(f"\nProcessing: {InputTableSchema01}.{InputTableName01} -> {OutputTableSchema01}.{OutputTableName01}")

    # Source columns
    source = source_all.filter(
        (col("TableSchema") == InputTableSchema01) &
        (col("TableName") == InputTableName01)
    )

    # Target columns
    sink = sink_all.filter(
        (col("TableSchema") == OutputTableSchema01) &
        (col("TableName") == OutputTableName01)
    )

    source_rows = source.collect()
    sink_rows = sink.collect()

    if not source_rows:
        print(f"⚠️ Source table not found: {InputTableSchema01}.{InputTableName01}")
        continue

    if not sink_rows:
        print(f"⚠️ Target table not found: {OutputTableSchema01}.{OutputTableName01}")
        continue

    qualified_name_input01 = source_rows[0]["PurviewFQN"]
    qualified_name_output01 = sink_rows[0]["PurviewFQN"]

    # Skip if source or target FQN is missing
    if not qualified_name_input01 or not qualified_name_output01:
        print(f"⚠️ Missing Source or Target PurviewFQN for {InputTableSchema01}.{InputTableName01}")
        continue

    # Match columns by identical name
    source_columns = {
        row["ColumnName"]
        for row in source_rows
        if row["ColumnName"]
    }

    sink_columns = {
        row["ColumnName"]
        for row in sink_rows
        if row["ColumnName"]
    }

    final_column_mappings = [
        {
            "Source": c,
            "Sink": c
        }
        for c in sorted(source_columns.intersection(sink_columns))
    ]

    if not final_column_mappings:
        print(f"⚠️ No matching columns found for {InputTableSchema01}.{InputTableName01} -> {OutputTableSchema01}.{OutputTableName01}")
        continue

    column_mapping = [{
        "ColumnMapping": final_column_mappings,
        "DatasetMapping": {
            "Source": qualified_name_input01,
            "Sink": qualified_name_output01
        }
    }]
    #print(column_mapping)


    #create Bronze(input) entity and upload to Purview
    input_table= get_or_create_entity(entity_name=InputTableName01, type_name=fabric_table_type_name ,qualified_name = qualified_name_input01, temp_guid = "-3")

    #create Silver(output) entity and upload to Purview
    output_table=get_or_create_entity(entity_name=OutputTableName01, type_name=fabric_table_type_name ,qualified_name = qualified_name_output01, temp_guid = "-4")
    
    process_qn = (f"fmdprocess://{SourceWorkspaceName}/{InputTableSchema01}/{InputTableName01}->{OutputTableSchema01}/{OutputTableName01}")
    process_name=(f"{processtype} Lineage {InputTableSchema01}.{InputTableName01} to {OutputTableSchema01}.{OutputTableName01}")
    results=create_lineage_process(input_entity=input_table, output_entity = output_table, process_type_name =process_type_name, process_name = process_name, process_qn = process_qn,temp_guid='-999', labels=SlvLayerName, column_mapping=column_mapping)
    #print(json.dumps(results, indent=2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
