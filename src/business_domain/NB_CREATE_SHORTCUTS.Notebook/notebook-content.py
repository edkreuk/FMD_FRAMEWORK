# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook to create shortcuts in Gold
# 
# Don't forget to change the parameters in de **VAR_GOLD_COLD_FMD** library
# 
# 
# More details: https://github.com/edkreuk/FMD_FRAMEWORK


# CELL ********************

import json, requests
import notebookutils 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Shortcut location parameters

ShortcutNames=['Sales_BuyingGroups','Sales_CustomerCategories','Sales_InvoiceLines','Sales_Invoices','Sales_Orders','Sales_OrderLines','Sales_vCustomers','Warehouse_PackageTypes','Warehouse_StockItems' ]         #Tablenames of the to be created shortcuts, same name will be used for destination

ShortcutSettings=notebookutils.variableLibrary.getLibrary("VAR_GOLD_CODE_FMD")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_shortcut(path:str, name:str, target:dict, workspace_id:str ,lakehouse_id:str ,conflict_policy:str="Abort"):

    # Get Token
    token = notebookutils.credentials.getToken("pbi")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "path": path,    # e.g., "Files/raw" OR "Tables"
        "name": name,    # e.g., "sales_external"
        "target": target # see examples below
    }

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
    params = {"shortcutConflictPolicy": conflict_policy}

    resp = requests.post(url, headers=headers, params=params, data=json.dumps(body))
    if resp.status_code in (200, 201):
        print(f"Shortcut '{name}' created at '{path}'.")
        print("Location:", resp.headers.get("Location", "<none>"))
    else:
        print("Failed:", resp.status_code, resp.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for ShortcutName in ShortcutNames:
    oneLake_target = {    "oneLake": {        "workspaceId": ShortcutSettings.Shortcut_TargetWorkspaceId,        "itemId":      ShortcutSettings.Shortcut_TargetLakehouseId,"path": "Tables/"+ShortcutSettings.Shortcut_TargetSchema+"/"+ShortcutName   }}
    create_shortcut(path="Tables/"+ShortcutSettings.SourceSchema, name=ShortcutName, target=oneLake_target,workspace_id=ShortcutSettings.SourceWorkspaceId,lakehouse_id=ShortcutSettings.SourceLakehouseId, conflict_policy="CreateOrOverwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
