---
Title: Deploy the FMD Framework
Description: Learn how to deploy the Fabric Metadata-Driven Framework (FMD) in Microsoft Fabric, including prerequisites, setup, and configuration.
Topic: how-to
Date: 07/2025
Author: edkreuk
---

# Deploy the FMD Framework

![FMD Overview](/Images/FMD_Overview.png)

This article describes how to deploy the Fabric Metadata-Driven Framework (FMD) in Microsoft Fabric. Follow these steps to configure your environment, set up required connections, and apply deployment settings.

## 📦 Installation

### Prerequisites

Before you begin, ensure the following prerequisites are met in the Admin Portal:
- Contributor role is assigned on the target capacity or capacities.

### Prerequisite: Enable access in the Fabric Admin portal

Sign in to the Fabric admin portal. You need to be a Fabric admin to see the tenant settings page.
Make sure the following settings are enabled:
**Microsoft Fabric settings:**
- Users can create Fabric items.
**Workspace settings:**
- Create Workspaces

Select the switch for the type of admin APIs you want to enable:
**Developer settings:**
- Service principals can create workspaces, connections, and deployment pipelines
- Service principals can call Fabric public APIs
**Admin API settings:**
- Service principals can access read-only admin APIs
- Service principals can access admin APIs used for update

In case you need to use a security group add the security group to above settings 
Add Workspace identity(after deployment) or Service Principal to the security groups


### Workspace identity or Service Principal used for execution must have the following role assigned:
- Workspace Contributor role on the workspace (Workspace Identity is automatically assigned during deployment, Service principal must be assigned manually)
- Workspace identity or Service Principal must be added to the correct security groups which have been assigned in the Prerequisite step

## Deployment steps

### 1. Download deployment assets

Download the deployment notebook from the setup folder to your local machine:

- `NB_SETUP_FMD.ipynb` – Automates artifact creation for FMD_FRAMEWORK in Fabric, based on your configuration.

### 2. Create required connections

Set up the following connections and note their Connection IDs for later configuration:

| Connection name              | Connection type            | Authentication                                    |Remarks |
|------------------------------|----------------------------|---------------------------------------------------|--------|
| CON_FMD_FABRIC_PIPELINES     | Fabric Data Pipelines      | OAuth2/Service Principal/Workspace Identity       |  Connection is automatically created during deployment      |
| CON_FMD_FABRIC_SQL           | Fabric SQL database        | OAuth2                        |  Connection needs to be created manually due to limitations      |
| CON_FMD_FABRIC_NOTEBOOKS     | Fabric Notebooks           | OAuth2/Service Principal/Workspace Identity       |  For future use     |

If you use Azure Data Factory Pipelines, create this additional connection:

| Connection name              | Connection type            | Authentication                                    |Remarks |
|------------------------------|----------------------------|---------------------------------------------------|--------|
| CON_FMD_ADF_PIPELINES        | Azure Data Factory         | OAuth2  or Service Principal                      |You must add the Service Principal to the workspace_roles_code.        |         |

### 3. Create a configuration workspace

- Create a new workspace (for example, `FMD_FRAMEWORK_CONFIGURATION`).
- Import the deployment notebook into the workspace (ensure you are in the Fabric Experience):
  - `NB_SETUP_FMD.ipynb`
  > [!NOTE]
> Make sure you set Set Spark session timeout to at least 1 hour in the workspace settings/Data Engineering/Jobs .

![Fabric Experience](/Images/FMD_Fabric_Experience.png)

### 4. Configure deployment settings

Open `NB_SETUP_FMD.ipynb` and navigate to the configuration cell. Update the following parameters as needed.

#### Key configuration parameters

**Framework settings**

> [!NOTE]
> Fabric Administrator Role is required to create a domain. Otherwise, disable domain creation in the next step.


**Framework settings**  

```python
assign_icons = True                       # Set to True to assign default icons to workspaces; set to False if you have already assigned custom icons
load_demo_data = True                     # Set to True if you want to load the demo data, otherwise set to False
lakehouse_schema_enabled = True           # Set to True if you want to use the lakehouse schema, otherwise set to False

driver = '{ODBC Driver 18 for SQL Server}'# Change this if you use a different driver
overwrite_variable_library = True         # By default the Library is overwritten, change this to "False" if you have custom changes
```
**Keyvault settings**  
For future use.
```python
key_vault_uri_name='val_key_vault_uri_name'
key_vault_tenant_id='val_key_vault_tenant_id'
key_vault_client_id='val_key_vault_client_id'
key_vault_client_secret='val_key_vault_client_secret'
```

**Capacity settings**  
  Specify the unique name for the capacity:

  ```python
  capacity_name_dvlm = 'Name of your capacity'
  reassign_capacity= True                 # Set to False if you don't want to reassign the capacity to an existing workspace in case you set the capacity manually
               
  ```

**Domain settings**

Define the name for the Main Domain, and you can add 1 or more business domains.


```python
framework_post_fix= ''                              # post fix to be added at the end of workspace for example INTEGRATION CODE(D) FMD
if framework_post_fix != '':
   framework_post_fix= ' '+ framework_post_fix      #If empty leave as is else add a space before for better visibility

##Domains
create_domains=  True                               # If you do not have a Fabric Admin role, you need to set this option to False. For domain creation the Fabric Admin role is needed
domain_name='INTEGRATION'                           # Main Domain for Integration for example INTEGRATION CODE(D) 

domain_contributor_role = {"type": "Contributors","principals": [{"id": "00000000-0000-0000-0000-000000000000","type": "Group"}  ]}  # Which group(Object ID) can add or remove workspaces to this domain

##Connections
connection_fabric_datapipelines_name='CON_FMD_FABRIC_PIPELINES'
connection_fabric_notebooks_name='CON_FMD_FABRIC_NOTEBOOKS'
connection_fabric_database_name='CON_FMD_FABRIC_SQL'
connection_fabric_adf_name='CON_FMD_ADF_PIPELINES'
connection_role =  {"role": "owner","principals": [{"id": "00000000-0000-0000-0000-000000000000","type": "Group"}  ]}  # Which group(Object ID) can add or remove workspaces to this domain
```
You need to create workspace roles for the different workspaces:

> [!NOTE]
> The id of the User, Group or Service Principal is the Object ID in Microsoft Entra ID. For a Service Principal, you can find the Object ID in the Azure Portal under 'Enterprise applications'. Don't use the Object ID of the App Registration.'

workspace_roles_code
workspace_roles_data
workspace_roles_configuration

Check the examples below
```python

# Replace placeholder IDs with real Object IDs from Microsoft Entra ID.
# Use "Group", "User" or "ServicePrincipal" for "type" as appropriate.

workspace_roles_code = [
    {
        "principal": {"id": "00000000-0000-0000-0000-000000000000", "type": "Group"},
        "role": "Member"
    },
    {
        "principal": {"id": "00000000-0000-0000-0000-000000000000", "type": "ServicePrincipal"},
        "role": "Contributor"
    }
]

workspace_roles_data = [
    {
        "principal": {"id": "00000000-0000-0000-0000-000000000000", "type": "Group"},
        "role": "Member"
    },
    {
        "principal": {"id": "00000000-0000-0000-0000-000000000000", "type": "Group"},
        "role": "Admin"
    }
]

workspace_roles_configuration = [
    {
        "principal": {"id": "00000000-0000-0000-0000-000000000000", "type": "Group"},
        "role": "Contributor"
    }
]
```

**Workspace configuration**  
```python
##### DO NOT CHANGE UNLESS SPECIFIED OTHERWISE, FE ADDING NEW ENVIRONMENTS ####
# Define settings for each environment (add more environments as needed)
environments = [
    {
        'environment_name': 'development',                                     # Name of target environment
        'workspaces': {
            'data': {
                'name': domain_name + ' DATA (D)' + framework_post_fix,       # Name of target data workspace for development
                'roles': workspace_roles_data,                                # Roles to assign to the workspace
                'capacity_name': capacity_name_dvlm                           # Name of target data workspace capacity for development
            },
            'code': {
                'name': domain_name + ' CODE (D)' + framework_post_fix,       # Name of target code workspace for development
                'roles': workspace_roles_code,                                # Roles to assign to the workspace
                'capacity_name': capacity_name_dvlm                           # Name of target code workspace capacity for development
            },
        }
    },
    {
        'environment_name': 'production',                                      # Name of target environment
        'workspaces': {
            'data': {
                'name': domain_name + ' DATA (P)' + framework_post_fix,       # Name of target data workspace for production
                'roles': workspace_roles_data,                                # Roles to assign to the workspace
                'capacity_name': capacity_name_prod                           # Name of target data workspace capacity for production
            },
            'code': {
                'name': domain_name + ' CODE (P)' + framework_post_fix,       # Name of target code workspace for production
                'roles': workspace_roles_code,                                # Roles to assign to the workspace
                'capacity_name': capacity_name_prod                           # Name of target code workspace capacity for production
            },
        }
    }
]
```
**Repo Configuration**

Location of the FMD Framework repository. Unless you have a forked version, do not change these settings. If you want to use another branch, you can change the branch name to your own branch.
  ```python
#FMD Framework code
##### DO NOT CHANGE UNLESS SPECIFIED OTHERWISE ####
repo_owner = "edkreuk"              # Owner of the repository
repo_name = "FMD_FRAMEWORK"         # Name of the repository
branch = "main"                     #"main" is default                    
folder_prefix = ""
###################################################
```

### 5. Run the deployment

Execute the **notebook** to apply your configuration and deploy the framework.

---

Check out the [wiki](https://github.com/edkreuk/FMD_FRAMEWORK/wiki) for more information and detailed guidance on using the FMD Framework and how to load demo data.




