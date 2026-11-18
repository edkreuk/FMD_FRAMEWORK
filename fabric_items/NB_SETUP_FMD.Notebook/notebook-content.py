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

# ![FMD_Overview](https://github.com/edkreuk/FMD_FRAMEWORK/blob/main/Images/FMD_Overview.png?raw=true)

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
load_demo_data= True                                # Set to True if you want to load the demo data, otherwise set to False
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



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Capacity configuration

# CELL ********************

reassign_capacity= True                                         # If set to False existing assigned capacities to workspaces will no be overwritten

capacity_name_dvlm = "Trial-Erwin"                              # Which capacity will be used for these workspaces in development
capacity_name_prod = 'Trial-Erwin'                              # Which capacity will be used for these workspaces in production
capacity_name_config = 'Trial-Erwin'                      # Which capacity will be used for this workspace for the Metadata Database

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Domain and Framework settings

# CELL ********************

framework_post_fix= ''                              # post fix to be added at the end of workspace for example INTEGRATION CODE(D) FMD
if framework_post_fix != '':
   framework_post_fix= ' '+ framework_post_fix      #If empty leave as is else add a space before for better visibility

##Domains
create_domains=  True                               # If you do not have a Fabric Admin role, you need to set this option to False. For domain creation the Fabric Admin role is needed

domain_name='INTEGRATION'                           # Main Domain for Integration for example INTEGRATION CODE(D) 

domain_contributor_role = {"type": "Contributors","principals": [{"id": "86897900-8de5-4894-ae41-1b4d1642acda","type": "Group"}  ]}  # Which group(Object ID) can add or remove workspaces to this domain

##Connections
connection_fabric_datapipelines_name='CON_FMD_FABRIC_PIPELINES'
connection_fabric_notebooks_name='CON_FMD_FABRIC_NOTEBOOKS'
connection_fabric_database_name='CON_FMD_FABRIC_SQL'
connection_fabric_adf_name='CON_FMD_ADF_PIPELINES'

connection_role =  {"role": "owner","principals": [{"id": "86897900-8de5-4894-ae41-1b4d1642acda","type": "Group"}  ]}  # Which group(Object ID) can add or remove workspaces to this domain


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
branch = "main"                     #"main" is default                    
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

workspace_roles_code = [ # Keep emtpy [] if you only want to assign this to your personal account
                                        {
                       "principal": {
                            "id": '86897900-8de5-4894-ae41-1b4d1642acda',
                            "type": "Group"
                        },
                        "role": "admin"  #(choose from 'admin', 'member', 'contributor', 'viewer')
                        },
                        {
                       "principal": {
                            "id": 'b0bf9847-5b7a-4560-947b-b149f71d303b',
                            "type": "ServicePrincipal"
                        },
                        "role": "contributor"  #(choose from 'admin', 'member', 'contributor', 'viewer')
                        }
                    ]
workspace_roles_data =  [ # Keep emtpy [] if you only want to assign this to your personal account
                                        {
                       "principal": {
                            "id": '86897900-8de5-4894-ae41-1b4d1642acda',
                            "type": "Group"
                        },
                        "role": "admin"  #(choose from 'admin', 'member', 'contributor', 'viewer')
                        },    
                         {
                       "principal": {
                            "id": '5c906b5c-d1d7-4984-b047-adacd8d795fe',
                            "type": "Group"
                        },
                        "role": "contributor"  #(choose from 'admin', 'member', 'contributor', 'viewer')
                        },
                        {
                       "principal": {
                            "id": 'b0bf9847-5b7a-4560-947b-b149f71d303b',
                            "type": "ServicePrincipal"
                        },
                        "role": "contributor"  #(choose from 'admin', 'member', 'contributor', 'viewer')
                        }
                    ]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Workspace configuration

# CELL ********************


##### DO NOT CHANGE UNLESS SPECIFIED OTHERWISE, FE ADDING NEW ENVIRONMENTS ####
environments = [
                    {
                        'environment_name' : 'development',                                     # Name of target environment
                        'workspaces': {
                            'data' : {
                                'name' : domain_name + ' DATA (D)' +  framework_post_fix,       # Name of target code workspace for development
                                'roles' : workspace_roles_data,                                 # Roles to assign to the workspace
                                'capacity_name' : capacity_name_dvlm                            # Name of target data workspace for development
                            },
                            'code' : {
                                'name' : domain_name + ' CODE (D)' +  framework_post_fix,       # Name of target data workspace for development
                                'roles' : workspace_roles_code,                                 # Roles to assign to the workspace
                                'capacity_name' : capacity_name_dvlm                            # Name of target code workspace for development
                            }
                        }
                    },
                    {
                        'environment_name' : 'production',                                      # Name of target environment
                        'workspaces': {
                            'data' : {
                                'name' : domain_name + ' DATA (P)' +  framework_post_fix,       # Name of target data workspace for production
                                'roles' : workspace_roles_data,                                 # Roles to assign to the workspace
                                'capacity_name' : capacity_name_prod                            # Name of target data workspace for production   
                            },
                            'code' : {
                                'name' : domain_name + ' CODE (P)' +  framework_post_fix,       # Name of target code workspace for production
                                'roles' : workspace_roles_code,                                 # Roles to assign to the workspace
                                'capacity_name' : capacity_name_prod                            # Name of target code workspace for production
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

# ## Fabric Database Configuration

# CELL ********************

configuration = {
                    'workspace': {
                        'name' : domain_name + ' CONFIG' +  framework_post_fix,             # Name of target workspace
                        'roles' : workspace_roles_data,                                     # Roles to assign to the workspace
                        'capacity_name' : capacity_name_config                              # Name of target capacity for the configuration workspace
                    },
                       'DatabaseName' : 'SQL_'+domain_name+'_FRAMEWORK'                     # Name of target configuration SQL Database
}

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
download_folders_as_zip(repo_owner, repo_name, output_zip = "./builtin/src/src.zip", branch = branch, folders_to_extract= [f"{folder_prefix}/src"] , remove_folder_prefix = f"{folder_prefix}")
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

deploy_order_path = os.path.join(base_path, 'config/item_deployment.json')
with open(deploy_order_path, 'r') as file:
        item_deployment =json.load(file)

deploy_order_path = os.path.join(base_path, 'config/item_initial_setup.json')
with open(deploy_order_path, 'r') as file:
        item_initial_setup =json.load(file)

deploy_order_path = os.path.join(base_path, 'config/data_deployment.json')
with open(deploy_order_path, 'r') as file:
        data_deployment =json.load(file)

deploy_order_path = os.path.join(base_path, 'config/lakehouse_deployment.json')
with open(deploy_order_path, 'r') as file:
        lakehouse_deployment =json.load(file)

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

# MARKDOWN ********************

# ## Integration Domain

# CELL ********************

if create_domains:
    create_fabric_domain(domain_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Create Connections

# MARKDOWN ********************

# Get existing connections

# CELL ********************

#Create connection and get connection id

##CON_FMD_FABRIC_PIPELINES
connection_id=create_or_get_fmd_connection(connection_fabric_datapipelines_name,connection_role, 'FabricDataPipelines')
mapping_table.append({"Description": connection_fabric_datapipelines_name,"ItemType": "connection" ,"environment": 'config' ,"old_id": config["connections"]["CON_FMD_FABRIC_PIPELINES"], "new_id": connection_id})

##CON_FMD_FABRIC_NOTEBOOKS
connection_id=create_or_get_fmd_connection(connection_fabric_notebooks_name,connection_role,'Notebooks' )
mapping_table.append({"Description": connection_fabric_notebooks_name,"ItemType": "connection" ,"environment": 'config' ,"old_id": config["connections"]["CON_FMD_FABRIC_NOTEBOOKS"], "new_id": connection_id})

##CON_FMD_FABRIC_SQL
connection_id=create_or_get_fmd_connection(connection_fabric_database_name,connection_role,'FabricSql' )
mapping_table.append({"Description": connection_fabric_database_name,"ItemType": "connection" ,"environment": 'config' ,"old_id": config["connections"]["CON_FMD_FABRIC_SQL"], "new_id": connection_id})

#CON_FMD_ADF_PIPELINES GET Connection id of ADF Connection if exist, when using different name, change below
connection_id=create_or_get_fmd_connection(connection_fabric_adf_name,connection_role,'AzureDataFactory' )
mapping_table.append({"Description": "CON_FMD_ADF_PIPELINES","ItemType": "connection" ,"environment": 'config' ,"old_id": config["connections"]["CON_FMD_ADF_PIPELINES"], "new_id": connection_id})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create workspaces(Code and Data)

# CELL ********************

for environment in environments:
    print(f"--------------------------")
    print(f"Creating/Updating Workspace: {environment['environment_name']}")
    deploy_workspaces(domain_name,workspace=environment['workspaces']['code'], workspace_name=environment['workspaces']['code']['name'], environment_name=environment['environment_name'], old_id=config["workspaces"]["workspace_code"], mapping_table=mapping_table, tasks=tasks)
    deploy_workspaces(domain_name,workspace=environment['workspaces']['data'], workspace_name=environment['workspaces']['data']['name'], environment_name=environment['environment_name'], old_id=config["workspaces"]["workspace_data"], mapping_table=mapping_table, tasks=tasks)
    assign_identity_role_to_different_workspace(environment['workspaces']['code']['name'],environment['workspaces']['data']['name'] )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create workspace(Configuration)

# CELL ********************

print(f"Creating/Updating Workspace: Configuration")
deploy_workspaces(domain_name,workspace=configuration['workspace'], workspace_name=configuration['workspace']['name'], environment_name='config', old_id=config["workspaces"]["workspace_config"], mapping_table=mapping_table, tasks=tasks)
for environment in environments:
    assign_identity_role_to_different_workspace(environment['workspaces']['code']['name'],configuration['workspace']['name'] )  ##Assign workspace identity to Configuration workspace while Identity is used to connect


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create Lakehouses

# CELL ********************

for environment in environments:
    print(f"\n--------------------------")
    print(f"Processing: {environment['environment_name']}")
    for workspace in [environment['workspaces']['data']]:
        for it in lakehouse_deployment:
            new_id = None
            name = it["name"]
            type = it["type"]
            deploy_item(workspace['name'],name,mapping_table,environment['environment_name'],tasks,lakehouse_schema_enabled,it)  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create Fabric database (Configuration)

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

# ### Create Items

# MARKDOWN ********************

# #### Environments

# CELL ********************

for it in item_deployment:
    if it['type'] in ('Environment'):
        new_id = None
        name = it["name"]
        type = it["type"]
        deploy_item(configuration['workspace']['name'],name,mapping_table,environment['environment_name'], tasks, lakehouse_schema_enabled,it)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Notebooks and VariableLibrary

# CELL ********************

for environment in environments:
    print(f"\n--------------------------")
    print(f"Processing: {environment['environment_name']}")
    for workspace in [environment['workspaces']['code']]:
        for it in item_deployment:
            if it['type'] in ('Notebook','VariableLibrary'):

                name = it["name"]
                type = it["type"]
                deploy_item(workspace['name'],name,mapping_table,environment['environment_name'], tasks, lakehouse_schema_enabled,it)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

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

# MARKDOWN ********************

# #### DataPipelines

# CELL ********************

for environment in environments:
    print(f"\n--------------------------")
    print(f"Processing: {environment['environment_name']}")
    for workspace in [environment['workspaces']['code']]:
        exclude = []
        for it in item_deployment:
            if it['type'] in ('DataPipeline'):
                new_id = None
                name = it["name"]
                type = it["type"]

                if name in exclude:
                    continue
                deploy_item(workspace['name'],name,mapping_table,environment['environment_name'], tasks, lakehouse_schema_enabled,it)

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

# ### Add Connections to Fabric Database

# CELL ********************

result = subprocess.run(["fab", "api", "-X", "get", "connections"], capture_output=True, text=True)
connections=json.loads(result.stdout)["text"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

custom_sql_deployment = {"queries_stored_procedures": []}
for connection in connections['value']:
    
    display_name = connection.get('displayName', '')
    if display_name and display_name.startswith('CON_FMD'):
        connection_type = connection.get('connectionDetails', {}).get('type', 'Unknown')
        connection_id = connection.get('id')
      
        exec_statement = (
            f"EXEC [integration].[sp_UpsertConnection] "
            f"@ConnectionGuid = \"{connection_id}\", "
            f"@Name = \"{display_name}\", "
            f"@Type = \"{connection_type}\", "
            f"@IsActive = 1"
        )
        custom_sql_deployment["queries_stored_procedures"].append(exec_statement)
        custom_sql_deployment["queries_stored_procedures"].append(f'EXEC [integration].[sp_UpsertConnection] @ConnectionGuid = "00000000-0000-0000-0000-000000000001", @Name = "CON_FMD_NOTEBOOK", @Type = "NOTEBOOK", @IsActive = 1')
        custom_sql_deployment["queries_stored_procedures"].append(f'EXEC [integration].[sp_UpsertConnection] @ConnectionGuid = "00000000-0000-0000-0000-000000000000", @Name = "CON_FMD_ONELAKE", @Type = "ONELAKE", @IsActive = 1')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Add DataSources to Fabric Database

# CELL ********************

custom_sql_deployment["queries_stored_procedures"].append("""
    DECLARE @DataSourceIdInternal INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = 'LH_DATA_LANDINGZONE' and Type='ONELAKE_TABLES_01')
    DECLARE @ConnectionIdInternal INT = (SELECT ConnectionId FROM integration.Connection WHERE ConnectionGuid = '00000000-0000-0000-0000-000000000000')
    EXECUTE [integration].[sp_UpsertDataSource] 
        @ConnectionId = @ConnectionIdInternal
        ,@DataSourceId = @DataSourceIdInternal
        ,@Name = 'LH_DATA_LANDINGZONE'
        ,@Namespace = 'ONELAKE'
        ,@Type = 'ONELAKE_TABLES_01'
        ,@Description = 'ONELAKE_TABLES'
        ,@IsActive = 1
""")
custom_sql_deployment["queries_stored_procedures"].append("""
    DECLARE @DataSourceIdInternal INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = 'LH_DATA_LANDINGZONE' and Type ='ONELAKE_FILES_01')
    DECLARE @ConnectionIdInternal INT = (SELECT ConnectionId FROM integration.Connection WHERE ConnectionGuid = '00000000-0000-0000-0000-000000000000')
    EXECUTE [integration].[sp_UpsertDataSource] 
        @ConnectionId = @ConnectionIdInternal
        ,@DataSourceId = @DataSourceIdInternal
        ,@Name = 'LH_DATA_LANDINGZONE'
        ,@Namespace = 'ONELAKE'
        ,@Type = 'ONELAKE_FILES_01'
        ,@Description = 'ONELAKE_FILES'
        ,@IsActive = 1
""")
custom_sql_deployment["queries_stored_procedures"].append("""
        DECLARE @DataSourceIdInternal INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = 'CUSTOM_NOTEBOOK' and Type='NOTEBOOK')
        DECLARE @ConnectionIdInternal INT = (SELECT ConnectionId FROM integration.Connection WHERE ConnectionGuid = '00000000-0000-0000-0000-000000000001')
        EXECUTE [integration].[sp_UpsertDataSource] 
            @ConnectionId = @ConnectionIdInternal
            ,@DataSourceId = @DataSourceIdInternal
            ,@Name = 'CUSTOM_NOTEBOOK'
            ,@Namespace = 'NB'
            ,@Type = 'NOTEBOOK'
            ,@Description = 'Custom Notebook'
            ,@IsActive = 1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Add Workspaces to Fabric Database

# CELL ********************

#add all created workspace to database
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

# MARKDOWN ********************

# ### Add Data Pipelines to Fabric Database

# CELL ********************

for environment in environments:
    result = run_fab_command(f"api -X get workspaces/{environment['workspaces']['code']['id']}/items", capture_output=True, silently_continue=True)
    existing_items = json.loads(result)['text']
    for item in existing_items.get('value', []):
        if item['type'] == 'DataPipeline':
            print(f'EXEC [integration].[sp_UpsertPipeline] @PipelineId = "{item["id"]}", @WorkspaceId = "{environment["workspaces"]["code"]["id"]}" ,@Name = "{item["displayName"]}"')
            custom_sql_deployment["queries_stored_procedures"].append(f'EXEC [integration].[sp_UpsertPipeline] @PipelineId = "{item["id"]}", @WorkspaceId = "{environment["workspaces"]["data"]["id"]}" ,@Name = "{item["displayName"]}"')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Add Lakehouses to Fabric Database

# CELL ********************

for environment in environments:
    result = run_fab_command(f"api -X get workspaces/{environment['workspaces']['data']['id']}/items", capture_output=True, silently_continue=True)
    existing_items = json.loads(result)['text']
    for item in existing_items.get('value', []):
        if item['type'] == 'Lakehouse':
            print(f'EXEC [integration].[sp_UpsertLakehouse] @LakehouseId = "{item["id"]}", @WorkspaceId = "{environment["workspaces"]["data"]["id"]}" ,@Name = "{item["displayName"]}"')
            custom_sql_deployment["queries_stored_procedures"].append(f'EXEC [integration].[sp_UpsertLakehouse] @LakehouseId = "{item["id"]}", @WorkspaceId = "{environment["workspaces"]["data"]["id"]}" ,@Name = "{item["displayName"]}"')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Add Demo data for testing to Fabric Database

# CELL ********************

if load_demo_data:  
    demo_sql_deployment = {"queries_stored_procedures": []}
    demo_sql_deployment["queries_stored_procedures"].append("""
        DECLARE @LandingzoneEntityIdInternal INT = (SELECT LandingzoneEntityId FROM integration.LandingzoneEntity WHERE SourceSchema = 'in' and SourceName = 'customer')
        DECLARE @DataSourceIdInternal INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = 'LH_DATA_LANDINGZONE' and Type='ONELAKE_TABLES_01')
        DECLARE @LakehouseIdInternal INT = (SELECT top 1 LakehouseId FROM integration.Lakehouse WHERE Name = 'LH_DATA_LANDINGZONE')
        EXECUTE [integration].[sp_UpsertLandingzoneEntity] 
            @LandingzoneEntityId = @LandingzoneEntityIdInternal
            ,@DataSourceId = @DataSourceIdInternal
            ,@LakehouseId = @LakehouseIdInternal
            ,@SourceSchema = 'in'
            ,@SourceName = 'customer'
            ,@SourceCustomSelect = ''
            ,@FileName = 'customer'
            ,@FilePath = 'fmd'
            ,@FileType = 'parquet'
            ,@IsIncremental = 0
            ,@IsIncrementalColumn = ''
            ,@IsActive = 1
            ,@CustomNotebookName = ''
    """)
    demo_sql_deployment["queries_stored_procedures"].append("""
        DECLARE @LandingzoneEntityIdInternal INT = (SELECT LandingzoneEntityId FROM integration.LandingzoneEntity WHERE SourceSchema = 'in' and SourceName = 'customer')
        DECLARE @BronzeLayerEntityIdInternal INT = (SELECT BronzeLayerEntityId FROM integration.BronzeLayerEntity WHERE [Schema] = 'in' and [Name] = 'customer')
        DECLARE @LakehouseIdInternal INT = (SELECT top 1 LakehouseId FROM integration.Lakehouse WHERE Name = 'LH_BRONZE_LAYER')
        EXECUTE [integration].[sp_UpsertBronzeLayerEntity] 
            @BronzeLayerEntityId = @BronzeLayerEntityIdInternal
            ,@LandingzoneEntityId = @LandingzoneEntityIdInternal
            ,@Schema = 'in'
            ,@Name = 'customer'
            ,@FileType = 'Delta'
            ,@LakehouseId = @LakehouseIdInternal
            ,@PrimaryKeys = 'CustomerId'
            ,@IsActive = 1
    """)
    demo_sql_deployment["queries_stored_procedures"].append("""
        DECLARE @BronzeLayerEntityIdInternal INT = (SELECT BronzeLayerEntityId FROM integration.BronzeLayerEntity WHERE [Schema] = 'in' and [Name] = 'customer')
        DECLARE @SilverLayerEntityIdInternal INT = (SELECT SilverLayerEntityId FROM integration.SilverLayerEntity WHERE [Schema] = 'in' and [Name] = 'customer')
        DECLARE @LakehouseIdInternal INT = (SELECT top 1 LakehouseId FROM integration.Lakehouse WHERE Name = 'LH_SILVER_LAYER')
        EXECUTE [integration].[sp_UpsertSilverLayerEntity] 
            @SilverLayerEntityId = @SilverLayerEntityIdInternal
            ,@BronzeLayerEntityId = @BronzeLayerEntityIdInternal
            ,@LakehouseId = @LakehouseIdInternal
            ,@Name = 'customer'
            ,@Schema = 'in'
            ,@FileType = 'delta'
            ,@IsActive = 1
    """)

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
        if load_demo_data: 
            for i, query in enumerate(demo_sql_deployment["queries_stored_procedures"]):
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
