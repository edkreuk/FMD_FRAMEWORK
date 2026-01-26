---
Title: Deploy the Business Domain for the FMD Framework
Description: Learn how to deploy the Fabric Metadata-Driven Framework (FMD) in Microsoft Fabric, including prerequisites, setup, and configuration.
Topic: how-to

Author: edkreuk
---

# Deploy the FMD Business Domain Framework in Microsoft Fabric

![FMD Overview](/Images/FMD_DOMAIN_OVERVIEW.png)

This article describes how to deploy the Business Domains in Microsoft Fabric. Follow these steps to configure your environment, set up required connections, and apply deployment settings.

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

In case you need to use a security group add the security group to above settings.
Add Workspace identity (after deployment) or Service Principal to the security groups


### Managed identity or Service Principal used for execution must have the following role assigned:
- Workspace Contributor role on the workspace (Workspace Identity is automatically assigned during deployment, Service principal must be assigned manually)
- Managed identity or Service Principal must be added to the correct security groups which have been assigned in the Prerequisite step

## Deployment steps

### 1. Download deployment assets

Download the deployment notebook from the setup folder to your local machine:

- `NB_SETUP_BUSINESS_DOMAINS.ipynb` – Automates artifact creation for FMD_FRAMEWORK in Fabric, based on your configuration.


### 2. Create a configuration workspace

- Create a new workspace (for example, `FMD_FRAMEWORK_CONFIGURATION`).
- Import the deployment notebook into the workspace (ensure you are in the Fabric Experience):
  - `NB_SETUP_BUSINESS_DOMAINS.ipynb`
  > [!NOTE]
> Make sure you set Spark session timeout to at least 1 hour in the workspace settings/Data Engineering/Jobs .

![Fabric Experience](/Images/FMD_Fabric_Experience.png)

### 3. Configure deployment settings

Open `NB_SETUP_BUSINESS_DOMAINS.ipynb` and navigate to the configuration cell. Update the following parameters as needed.

#### Key configuration parameters

**Framework settings**

> [!NOTE]
> Fabric Administrator Role is required to create a domain. Otherwise, disable domain creation in the next step.


**settings**  

```python
assign_icons = True                       # Set to True to assign default icons to workspaces; set to False if you have already assigned custom icons

lakehouse_schema_enabled = True           # Set to True if you want to use the lakehouse schema, otherwise set to False

driver = '{ODBC Driver 18 for SQL Server}'# Change this if you use a different driver
overwrite_variable_library = True         # By default the Library is overwritten, change this to "False" if you have custom changes
```
**Variable settings**
```python
key_vault_uri_name='val_key_vault_uri_name'
SourceWorkspaceId=''                            
SourceLakehouseId=''                            # Your Gold lakehouse  
SourceSchema=''
Shortcut_TargetSchema=''
Shortcut_TargetWorkspaceId=''
Shortcut_TargetLakehouseId=''                   # Your Silver Lakehouse
````

**Capacity settings**  
  Specify the unique name for the capacity:

  ```python
  capacity_name_dvlm = 'Name of your capacity'
  reassign_capacity= True                 # Set to False if you don't want to reassign the capacity to an existing workspace in case you set the capacity manually
               
  ```

**Domain settings**

Define the name for the Main Domain, and you can add 1 or more business domains.


```python
framework_post_fix= ''                              # postfix to be added at the end of workspace for example INTEGRATION CODE(D) FMD
if framework_post_fix != '':
   framework_post_fix= ' '+ framework_post_fix      #If empty leave as is else add a space before for better visibility

##Domains
create_domains=  True                               # If you do not have a Fabric Admin role, you need to set this option to False. For domain creation the Fabric Admin role is needed
business_domain_names= ['FINANCE','SALES']          # Define business domains
domain_contributor_role = {"type": "Contributors","principals": [{"id": "00000000-0000-0000-0000-000000000000","type": "Group"}  ]}  # Which group(Object ID) can add or remove workspaces to this domain
configuration_database_workspace=  'Name of the workspace of your SQL Server database which holds the configuration database'
configuration_database_name= 'Name of the configuration database'

```
You need to create workspace roles for the different workspaces:

> [!NOTE]
> The id of the User, Group or Service Principal is the Object ID in Microsoft Entra ID. For a Service Principal, you can find the Object ID in the Azure Portal under 'Enterprise applications'. Don't use the Object ID of the App Registration.'


workspace_roles_reporting
workspace_roles_gold

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



# Optional: business-domain specific role lists
workspace_roles_data_business_domain = [
    {"principal": {"id": "00000000-0000-0000-0000-000000000000", "type": "Group"}, "role": "Member"}
]

workspace_roles_code_business_domain = [
    {"principal": {"id": "00000000-0000-0000-0000-000000000000", "type": "Group"}, "role": "Member"}
]

workspace_roles_reporting_business_domain = [
    {"principal": {"id": "00000000-0000-0000-0000-000000000000", "type": "Group"}, "role": "Member"}
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




