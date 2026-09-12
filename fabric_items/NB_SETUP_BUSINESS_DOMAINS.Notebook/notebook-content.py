# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# ![FMD_Overview](https://github.com/edkreuk/FMD_FRAMEWORK/blob/main/Images/FMD_DOMAIN_OVERVIEW.png?raw=true)

# CELL ********************

%pip install ms-fabric-cli pillow cairosvg --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Configuration and Parameters
# 
# **Fabric Administrator Role is required to create domain**

# CELL ********************

assign_icons = True                                # Set to True to assign default icons to workspaces; set to False if you have already assigned custom icons
lakehouse_schema_enabled = True                     # Set to True if you want to use the lakehouse schema, otherwise set to False

driver = '{ODBC Driver 18 for SQL Server}'          # Change this if you use a different driver
overwrite_variable_library=True                    # By default the Library is overwritten, change this to "False" if you have custom changes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## KeyVault settings
# For future usage

# CELL ********************

key_vault_uri_name='val_key_vault_uri_name'
key_vault_tenant_id='val_key_vault_tenant_id'
key_vault_client_id='val_key_vault_client_id'
key_vault_client_secret='val_key_vault_client_secret'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

##No need to fill in the first time, because the workspaces does no texist yet. After the first run, you can fill in the workspace and lakehouse ids to speed up the deployment process, otherwise the script will look them up by name every time which takes more time.
##Used for Shortcuts, can also manualy filled in
SourceWorkspaceId=''                            
SourceLakehouseId=''                            # Your Gold lakehouse  
SourceSchema=''
Shortcut_TargetSchema=''
Shortcut_TargetWorkspaceId=''
Shortcut_TargetLakehouseId=''                   # Your Silver Lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Capacity configuration

# CELL ********************

reassign_capacity= True                                        # If set to False existing assigned capacities to workspaces will no be overwritten

capacity_name_business_domain_dvlm = 'Trial-Erwin'              # Which capacity will be used for the workspaces in this business domain
capacity_name_business_domain_prod = 'Trial-Erwin'              # Which capacity will be used for the workspaces in this business domain

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Domain and Framework settings

# CELL ********************

framework_post_fix= ''                                   # post fix to be added at the end of workspace for example INTEGRATION CODE(D) FMD
if framework_post_fix != '':
   framework_post_fix= ' '+ framework_post_fix           #If empty leave as is else add a space before for better visibility

##Domains
create_domains=  True                                    # If you do not have a Fabric Admin role, you need to set this option to False. For domain creation the Fabric Admin role is needed

business_domain_names= ['FINANCE','SALES']               # Define business domains
domain_contributor_role = {"type": "Contributors","principals": [{"id": "86897900-8de5-4894-ae41-1b4d1642acda","type": "Group"}  ]}  # Which group(Object ID) can add or remove workspaces to this domain
configuration_database_workspace='INTEGRATION CONFIG'    # Name of the workspace of Fabric database which was deployed for the Integration Domain
configuration_database_name='SQL_INTEGRATION_FRAMEWORK'  # Name of the Fabric database which was deployed for the Integration Domain

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

configuration = {
                    'workspace': {
                        'name' : configuration_database_workspace            # Name of target workspace
                                       },
                    'DatabaseName' : configuration_database_name                    # Name of target configuration SQL Database
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Configuration


# MARKDOWN ********************

# ## Repo Configuration

# CELL ********************

#FMD Framework code
##### DO NOT CHANGE UNLESS SPECIFIED OTHERWISE ####
repo_owner = "edkreuk"              # Owner of the repository
repo_name = "FMD_FRAMEWORK"         # Name of the repository
branch = "main" #"main" is default                    
folder_prefix = ""
###################################################

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Workspace Roles Configuration

# CELL ********************

workspace_roles_data_business_domain = [ # Keep emtpy [] if you only want to assign this to your personal account
                    {
                      "principal": {
                            "id": '86897900-8de5-4894-ae41-1b4d1642acda',
                             "type": "Group"
                        },
                        "role": "admin"  #(choose from 'admin', 'member', 'contributor', 'viewer')
                        }
                    ]

workspace_roles_code_business_domain = [ # Keep emtpy [] if you only want to assign this to your personal account
                    {
                      "principal": {
                            "id": '86897900-8de5-4894-ae41-1b4d1642acda',
                             "type": "Group"
                        },
                        "role": "admin"  #(choose from 'admin', 'member', 'contributor', 'viewer')
                        }
                    ]

workspace_roles_reporting_business_domain = [ # Keep emtpy [] if you only want to assign this to your personal account
                    {
                        "principal": {
                            "id": '86897900-8de5-4894-ae41-1b4d1642acda',
                            "type": "Group"
                        },
                        "role": "admin"  #(choose from 'admin', 'member', 'contributor', 'viewer')
                        }
                    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Business Domain Configuration

# CELL ********************

##### DO NOT CHANGE UNLESS SPECIFIED OTHERWISE, FE ADDING NEW ENVIRONMENTS ####
business_domain_deployment = [
                    {
                        'environment_name' : 'development',                                 # Name of target environment
                        'environment_short' : 'D',                                          # Short of target environment
                        'workspaces': {
                         
                            'data' : {
                                'roles' : workspace_roles_data_business_domain,             # Roles to assign to the workspace
                                'capacity_name' : capacity_name_business_domain_dvlm        # Name of target data workspace for development
                            },
                            'code' : {
                                'roles' : workspace_roles_code_business_domain,             # Roles to assign to the workspace
                                'capacity_name' : capacity_name_business_domain_dvlm        # Name of target code workspace for development
                            },
                            'reporting' : {
                            'roles' : workspace_roles_reporting_business_domain,            # Roles to assign to the workspace
                            'capacity_name' : capacity_name_business_domain_dvlm            # Name of target code workspace for development
                            },
                            'semantic' : {
                            'roles' : workspace_roles_reporting_business_domain,            # Roles to assign to the workspace
                            'capacity_name' : capacity_name_business_domain_dvlm            # Name of target code workspace for development
                            }
                        }
                    },
                    {
                        'environment_name' : 'production',                                  # Name of target environment
                        'environment_short' : 'P',                                          # Short of target environment
                        'workspaces': {
                         
                            'data' : {
                                'roles' : workspace_roles_data_business_domain,             # Roles to assign to the workspace
                                'capacity_name' : capacity_name_business_domain_prod        # Name of target data workspace for development
                            },
                            'code' : {
                                'roles' : workspace_roles_code_business_domain,             # Roles to assign to the workspace
                                'capacity_name' : capacity_name_business_domain_prod        # Name of target code workspace for development
                            },
                            'reporting' : {
                                'roles' : workspace_roles_reporting_business_domain,        # Roles to assign to the workspace
                                'capacity_name' : capacity_name_business_domain_prod        # Name of target code workspace for development
                            },
                            'semantic' : {
                                'roles' : workspace_roles_reporting_business_domain,        # Roles to assign to the workspace
                                'capacity_name' : capacity_name_business_domain_prod        # Name of target code workspace for development
                            }
                        }
                    }
                ]
###################################################

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Download source & config files

# CELL ********************

import subprocess
import os
from time import sleep, time
import json
import nbformat
import shutil
import re
import requests
import zipfile
import yaml
import struct
import pyodbc
import sempy.fabric as fabric
import cairosvg
import base64
import xml.etree.ElementTree as ET

from PIL import Image, ImageDraw, ImageFont
from requests.adapters import HTTPAdapter, Retry
from io import BytesIO
from zipfile import ZipFile 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def download_folders_as_zip(repo_owner, repo_name, output_zip, branch="main", folders_to_extract=None, remove_folder_prefix=""):
    if folders_to_extract is None:
        folders_to_extract = []

    # Construct the URL for the GitHub API to download the repository as a zip file
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/zipball/{branch}"
    response = requests.get(url)
    response.raise_for_status()

    # Ensure the directory for the output zip file exists
    os.makedirs(os.path.dirname(output_zip), exist_ok=True)

    # Create a zip file in memory from GitHub response
    with zipfile.ZipFile(BytesIO(response.content)) as zipf:
        # Open output zip in append mode
        with zipfile.ZipFile(output_zip, 'w') as output_zipf:
            
            for file_info in zipf.infolist():
                for folder in folders_to_extract:
                    folder_path = f"/{folder}" if not folder.startswith("/") else folder
                    if re.sub(r'^.*?/', '/', file_info.filename).startswith(folder_path):
                        file_data = zipf.read(file_info.filename)
                        parts = file_info.filename.split('/')
                        if remove_folder_prefix:
                            parts = [p for p in parts if p != remove_folder_prefix]
                        output_zipf.writestr('/'.join(parts[1:]), file_data)

def uncompress_zip_to_folder(zip_path, extract_to):
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(zip_path)

def copy_to_tmp(name):
    shutil.rmtree("./builtin/tmp", ignore_errors=True)
    path2zip = "./builtin/src/src.zip"

    prefixes = [
        f"src/{name}"
    ]

    with ZipFile(path2zip) as archive:
        for prefix in prefixes:
            matched_files = [file for file in archive.namelist() if file.startswith(prefix)]
            if matched_files:
                for file in matched_files:
                    archive.extract(file, "./builtin/tmp")
                return f"./builtin/tmp/{prefix}"  # Return only the first matching prefix

    return None  # Nothing found

# ✅ Combine all folders into one zip
download_folders_as_zip(repo_owner, repo_name, output_zip = "./builtin/src/src.zip", branch = branch, folders_to_extract= [f"{folder_prefix}/src/business_domain"] , remove_folder_prefix = f"{folder_prefix}")
download_folders_as_zip(repo_owner, repo_name, output_zip = "./builtin/config/config.zip", branch = branch, folders_to_extract= [f"{folder_prefix}/config"] , remove_folder_prefix = f"{folder_prefix}")
# ✅ Uncompress everything into ./builtin
uncompress_zip_to_folder(zip_path = "./builtin/config/config.zip", extract_to= "./builtin")

mapping_table=[]
tasks=[]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # CLI Login

# CELL ********************

# Set environment parameters for Fabric CLI
token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Deployment functions

# MARKDOWN ********************

# ## Load configuration


# CELL ********************

base_path = './builtin/'
config_path = os.path.join(base_path, 'config/item_config.yaml')

with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

deploy_order_path = os.path.join(base_path, 'config/item_initial_setup.json')
with open(deploy_order_path, 'r') as file:
        item_initial_setup =json.load(file)

deploy_order_path = os.path.join(base_path, 'config/item_deployment_code_business_domain.json')
with open(deploy_order_path, 'r') as file:
        item_deployment_code_business_domain =json.load(file)

deploy_order_path = os.path.join(base_path, 'config/item_deployment_data_business_domain.json')
with open(deploy_order_path, 'r') as file:
        item_deployment_data_business_domain =json.load(file)

deploy_order_path = os.path.join(base_path, 'config/data_deployment.json')
with open(deploy_order_path, 'r') as file:
        data_deployment =json.load(file)

deploy_icons_path = os.path.join(base_path, 'config/fabric_icons.xml')

# Parse the XML file
tree = ET.parse(deploy_icons_path)
root = tree.getroot()

# Create a dictionary to store icon name and base64
fabric_icons_fmd = {}
for item in root.findall('icon'):
    name = item.find('name').text if item.find('name') is not None else "No name"
    base64_str = item.find('base64').text if item.find('base64') is not None else ""
    fabric_icons_fmd[name] = base64_str


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Deployment

# MARKDOWN ********************

# ## Load latest version NB_UTILITIES_SETUP_FMD

# CELL ********************

def run_fab_command(command, capture_output=False, silently_continue=False, raw_output=False):
    """
    Executes a Fabric CLI command with optional output capture and error handling.
    """
    result = subprocess.run(["fab", "-c", command], capture_output=capture_output, text=True)
    if not silently_continue and (result.returncode > 0 or result.stderr):
        raise Exception(f"Error running fab command. exit_code: '{result.returncode}'; stderr: '{result}'")
    if capture_output:
        return result if raw_output else result.stdout.strip()
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

workspace_id = fabric.get_workspace_id()
result=run_fab_command("api -X get workspaces/"+workspace_id, capture_output=True, silently_continue=False)
workspace_name=json.loads(result)["text"]["displayName"]

for it in item_initial_setup:
    name = it["name"]
    type = it["type"]
    tmp_path = copy_to_tmp(name)

    cli_parameter = ''

    if "Notebook" in name:
        cli_parameter += " --format .py"
        result = run_fab_command(f"import {workspace_name}.Workspace/{name} -i {tmp_path} -f {cli_parameter}",capture_output=True, silently_continue=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%run NB_UTILITIES_SETUP_FMD

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

variable_parameters.update ( {
     "SourceWorkspaceId": SourceWorkspaceId,
      "SourceLakehouseId": SourceLakehouseId,
       "SourceSchema": SourceSchema,
        "Shortcut_TargetSchema": Shortcut_TargetSchema,
         "Shortcut_TargetWorkspaceId": Shortcut_TargetWorkspaceId,
          "Shortcut_TargetLakehouseId": Shortcut_TargetLakehouseId,
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Integration Domain

# MARKDOWN ********************

# ### Get Fabric database (Configuration)

# CELL ********************

for deployment_item in data_deployment:
        if deployment_item['type'] in ('SQLDatabase'):
                name = configuration['DatabaseName']
                type = deployment_item["type"]
                name=name+'.'+type
                server, database_name=deploy_item(workspace_name=configuration['workspace']['name'],name=name,mapping_table=mapping_table, environment_name='config',tasks= tasks, lakehouse_schema_enabled=lakehouse_schema_enabled,it=deployment_item)

##!NOTE: in Case of an error check if you're not using a trial capacitiy or the capacity you assigned is paused

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Business Domain

# MARKDOWN ********************

# ### Create workspaces(Business Domains)

# CELL ********************

for business_domain_name in business_domain_names:
    if create_domains:
        create_fabric_domain(business_domain_name)

    for business_domain in business_domain_deployment:
        print(f"--------------------------")
        print(f"Updating Workspace: {business_domain['environment_name']}")
        deploy_workspaces(business_domain_name,workspace=business_domain['workspaces']['code'],workspace_name=business_domain_name + ' CODE ('+business_domain['environment_short']+')'+framework_post_fix,  environment_name=business_domain['environment_name'], old_id=config["workspaces"]["workspace_business_domain_code"], mapping_table=mapping_table, tasks=tasks)
        deploy_workspaces(business_domain_name,workspace=business_domain['workspaces']['data'],workspace_name=business_domain_name + ' DATA ('+business_domain['environment_short']+')'+framework_post_fix,  environment_name=business_domain['environment_name'], old_id=config["workspaces"]["workspace_business_domain_data"], mapping_table=mapping_table, tasks=tasks)
        deploy_workspaces(business_domain_name,workspace=business_domain['workspaces']['reporting'],workspace_name=business_domain_name + ' REPORTING ('+business_domain['environment_short']+')'+framework_post_fix, environment_name=business_domain['environment_name'], old_id=config["workspaces"]["workspace_business_domain_reporting"], mapping_table=mapping_table, tasks=tasks)
        deploy_workspaces(business_domain_name,workspace=business_domain['workspaces']['semantic'],workspace_name=business_domain_name + ' SEMANTIC ('+business_domain['environment_short']+')'+framework_post_fix, environment_name=business_domain['environment_name'], old_id=config["workspaces"]["workspace_business_domain_reporting"], mapping_table=mapping_table, tasks=tasks)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create Lakehouses(Business Domains)

# CELL ********************

for workspace_business_domain in business_domain_deployment:
    for business_domain in business_domain_names:
        for workspace in [workspace_business_domain['workspaces']['data']]:
            exclude = []
            for it in item_deployment_data_business_domain:
                new_id = None                    
                name = it["name"]
                type = it["type"]
                if name in exclude:
                    continue
                deploy_item(business_domain + ' DATA ('+ workspace_business_domain['environment_short']+')'+framework_post_fix, name,mapping_table, workspace_business_domain['environment_name'], tasks, lakehouse_schema_enabled,it)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create Items(Business Domains)

# CELL ********************

for workspace_business_domain in business_domain_deployment:
    for business_domain_name in business_domain_names:
        for workspace in [workspace_business_domain['workspaces']['code']]:
                for it in item_deployment_code_business_domain:
                    new_id = None
                    if it['type'] in ('VariableLibrary'):
                        name = it["name"]
                        type = it["type"]
                        deploy_item(business_domain_name + ' CODE ('+ workspace_business_domain['environment_short']+')'+framework_post_fix, name,mapping_table, workspace_business_domain['environment_name'], tasks, lakehouse_schema_enabled,it)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

for workspace_business_domain in business_domain_deployment:
    for business_domain_name in business_domain_names:
        for workspace in [workspace_business_domain['workspaces']['code']]:
                for it in item_deployment_code_business_domain:
                    new_id = None
                    if it['type'] not in ('VariableLibrary'):
                        name = it["name"]
                        type = it["type"]
                        deploy_item(business_domain_name + ' CODE ('+ workspace_business_domain['environment_short']+')'+framework_post_fix, name,mapping_table, workspace_business_domain['environment_name'], tasks, lakehouse_schema_enabled,it)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Workspace Icons

# CELL ********************

if assign_icons:  
    seen = set()
    workspaces = []
    # Get cluster URL for use in metadata endpoints
    cluster_base_url = get_cluster_url()

    for item in mapping_table:
        if item['ItemType'] == 'Workspace':
            key = (item['Description'], item['new_id'])
            if key not in seen:
                seen.add(key)
                workspaces.append({'displayName': item['Description'], 'id': item['new_id']})
    fabric_icons = fabric_icons_fmd 

    for workspace in workspaces:
        display_name = workspace['displayName'].lower()
       
        # Assign icon
        for icon_key, icon_value in workspace_icon_def['icons'].items():
            if icon_key in display_name:
                workspace["icon"] = icon_value
                workspace_icon = fabric_icons.get(icon_value)
                break
        else:
            workspace["icon"] = None
            workspace_icon = None

        workspace["icon_base64img"] = workspace_icon

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if assign_icons:
    # Dry run - Display pre and post icons based on specified workspace filters and workspace icon definition. Will NOT update any icons!
    display_workspace_icons(workspaces)
    sleep(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Deploy Workspace Icons

# CELL ********************

if assign_icons:
    for workspace in workspaces:
            set_workspace_icon(workspace.get('id'), workspace.get('icon_base64img'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Create SQL deployment Manifest

# MARKDOWN ********************

# ### Add Workspaces to Fabric Database

# CELL ********************

#add all created workspace to database
custom_sql_deployment = {"queries_stored_procedures": []}
unique_items = {}
for item in mapping_table:
    if item.get("ItemType") == "Workspace":
        unique_items[item["new_id"]] = item["Description"]

# Convert to list of tuples or dicts
workspaces = [{"Description": desc, "new_id": nid} for nid, desc in unique_items.items()]

for workspace in workspaces:
    print(f'EXEC [integration].[sp_UpsertWorkspace](@WorkspaceId = "{workspace["new_id"]}" ,@Name = "{workspace["Description"]}")')
    custom_sql_deployment["queries_stored_procedures"].append(f'EXEC [integration].[sp_UpsertWorkspace] @WorkspaceId = "{workspace["new_id"]}", @Name = "{workspace["Description"]}"')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

for workspace_business_domain in business_domain_deployment:
    for business_domain_name in business_domain_names:
        for workspace in [workspace_business_domain['workspaces']['code']]:
                workspace_id = get_workspace_id_by_name(business_domain_name + ' CODE ('+ workspace_business_domain['environment_short']+')'+framework_post_fix)
                result = run_fab_command(f"api -X get workspaces/{workspace_id}/items", capture_output=True, silently_continue=True)
                existing_items = json.loads(result)['text']
                for item in existing_items.get('value', []):
                    if item['type'] == 'DataPipeline':
                        print(f'EXEC [integration].[sp_UpsertPipeline] @PipelineId = "{item["id"]}", @WorkspaceId = "{workspace_id}" ,@Name = "{item["displayName"]}"')
                        custom_sql_deployment["queries_stored_procedures"].append(f'EXEC [integration].[sp_UpsertPipeline] @PipelineId = "{item["id"]}", @WorkspaceId = "{workspace_id}" ,@Name = "{item["displayName"]}"')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Add Lakehouses to Fabric Database

# CELL ********************

for workspace_business_domain in business_domain_deployment:
    for business_domain_name in business_domain_names:
        for workspace in [workspace_business_domain['workspaces']['data']]:
                workspace_id = get_workspace_id_by_name(business_domain_name + ' DATA ('+ workspace_business_domain['environment_short']+')'+framework_post_fix)
                result = run_fab_command(f"api -X get workspaces/{workspace_id}/items", capture_output=True, silently_continue=True)
                existing_items = json.loads(result)['text']
                for item in existing_items.get('value', []):
                    if item['type'] == 'Lakehouse':
                        print(f'EXEC [integration].[sp_UpsertLakehouse] @LakehouseId = "{item["id"]}", @WorkspaceId = "{workspace_id}" ,@Name = "{item["displayName"]}"')
                        custom_sql_deployment["queries_stored_procedures"].append(f'EXEC [integration].[sp_UpsertLakehouse] @LakehouseId = "{item["id"]}", @WorkspaceId = "{workspace_id}" ,@Name = "{item["displayName"]}"')
                        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Deploy SQL Code

# CELL ********************

#Make sure out token is still valid before we continou, sometime it can take a bit longer that's we will check it again
token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

for deployment_item in data_deployment:
        if deployment_item['type'] in ('SQLDatabase'):
                name = configuration['DatabaseName']
                type = deployment_item["type"]
                name=name+'.'+type
                if not server:         #it was already set, just in case
                    server=get_item_id(configuration['workspace']['name'], name, 'properties.serverFqdn')
                if not database_name:    
                    database_name=get_item_id(configuration['workspace']['name'], name, 'properties.databaseName')

try:
    i = 0

    token = notebookutils.credentials.getToken('pbi').encode('utf-16-le')
    token_struct = struct.pack(f'<I{len(token)}s', len(token), token)

    print(f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database_name};")
    connection = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database_name};", attrs_before={1256:token_struct}, timeout=12)

    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")  # Execute the warm-up query (a simple query like 'SELECT 1' can be used)
        cursor.fetchone()
        connection.timeout = 10  # Setting a lower timeout for subsequent queries

    for i, query in enumerate(custom_sql_deployment["queries_stored_procedures"]):
        print(f' - execute "{query}"')
        cursor.execute(query)
        cursor.commit()


    tasks.append({"task_name":f"{workspace.get('displayName')} {database_name} query {i}", "task_duration": 1, "status": f"success"})
except pyodbc.OperationalError as e:
    print(e) 
    tasks.append({"task_name":f"{workspace.get('displayName')} {database_name} query {i}", "task_duration": 1, "status": f"pyodbc failed: {e}"})
except Exception as e:
    print(e) 
    tasks.append({"task_name":f"{workspace.get('displayName')} {database_name} query {i}", "task_duration": 1, "status": f"failed: {e}"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

display(tasks)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
