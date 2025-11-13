---
Title: Deploy the FMD Framework
Description: Learn how to deploy the Fabric Metadata-Driven (FMD) Framework in Microsoft Fabric, including prerequisites, setup, and configuration.
Topic: how-to
Date: 07/2025
Author: edkreuk
---

# Deploy the FMD Framework

![FMD Overview](/Images/FMD_Overview.png)

This article describes how to deploy the Fabric Metadata-Driven (FMD) Framework in Microsoft Fabric. Follow these steps to configure your environment, set up required connections, and apply deployment settings.

## 📦 Installation

### Prerequisites

Before you begin, ensure the following prerequisites are met in the Admin Portal of Microsoft Fabric:

- Users can create Fabric items.
- Fabric SQL database is available.
- Contributor role is assigned on the target capacity or capacities.

### Prerequisite: Enable Admin API access
Enable the Fabric admin settings:

Sign in to the Fabric admin portal. You need to be a Fabric admin to see the tenant settings page.
Under Admin API settings, select the switch for the type of admin APIs you want to enable:
- Service principals can access read-only admin APIs
- Service principals can access admin APIs used for update

## Deployment steps

### 1. Download deployment assets

Download the deployment notebook from the setup folder to your local machine:

- `NB_SETUP_FMD.ipynb` – Automates artifact creation for FMD_FRAMEWORK in Fabric, based on your configuration.

### 2. Create required connections

Set up the following connections and note their Connection IDs for later configuration:

| Connection name              | Connection type         | Authentication |Remarks |
|------------------------------|------------------------|----------------|--------|
| CON_FMD_FABRIC_PIPELINES     | Fabric Data Pipelines  | OAuth2  or Service Principal       |  You must add the Service Principal to the workspace_roles and data        |
| CON_FMD_FABRICSQL            | Fabric SQL database    | OAuth2         |  Currently Service Principal is not supported      |

If you use Azure Data Factory Pipelines, create this additional connection:

| Connection name              | Connection type         | Authentication |
|------------------------------|------------------------|----------------|
| CON_FMD_ADF_PIPELINES        | Azure Data Factory     | OAuth2  or Service Principal       |  You must add the Service Principal to the workspace_roles_code.        |         |

### 3. Create a configuration workspace

- Create a new workspace (for example, `FMD_FRAMEWORK_CONFIGURATION`).
- Import the deployment notebook into the workspace (ensure you are in the Fabric Experience):
  - `NB_SETUP_FMD.ipynb`

![Fabric Experience](/Images/FMD_Fabric_Experience.png)

### 4. Configure deployment settings

Open `NB_SETUP_FMD.ipynb` and navigate to the configuration cell. Update the following parameters as needed.

#### Key configuration parameters

**Framework settings**

> [!NOTE]
> Fabric Administrator Role is required to create a domain. Otherwise, disable domain creation in the next step.


Define the name for the Main Domain, and you can add 1 or more sub domains

```python
##FrameworkName = 'DEMO'              # max 6 characters, no spaces. Is now removed, if used this please use the new option framework_post_fix(this will a lot of users more flexibility)
assign_icons = True                 # Set to True to assign default icons to workspaces; set to False if you have already assigned custom icons

load_demo_data = True               # Set to True if you want to load the demo data, otherwise set to False
lakehouse_schema_enabled = True     # Set to True if you want to use the lakehouse schema, otherwise set to False
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
create_domains=  True                               # If you do not have a Fabric Admin role, you need to set this option to False. For domain creation the Fabric Admin role is needed

domain_name='INTEGRATION'                           # Main Domain    for example INTEGRATION CODE(D) 
business_domain_names= ['FINANCE','SALES']          # Create business domains(sub)
framework_post_fix= ''                              # post fix to be added at the end of workspace for example INTEGRATION CODE(D) FMD
if framework_post_fix != '':
   framework_post_fix= ' '+ framework_post_fix      #If empty leave as is else add a space before for better visibility
# Replace '00000000-0000-0000-0000-000000000000' with the actual Entra AD group or user ID that should have contributor access.
domain_contributor_role = {
    "type": "Contributors",
    "principals": [
        {"id": "00000000-0000-0000-0000-000000000000", "type": "Group"}  # <--- PLACEHOLDER: Enter your real group/user ID here
    ]
}  # Which group/user can add or remove workspaces to this domain
  ```
You need to create workspace roles for the different workspaces:

> [!NOTE]
> The id of the User, Group or Service Principal is the Object ID in Microsoft Entra ID. For a Service Principal, you can find the Object ID in the Azure Portal under 'Enterprise applications'. Don't use the Object ID of the App Registration.'

workspace_roles_code
workspace_roles_data
workspace_roles_configuration
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


**Configuration settings  (Fabric Database)**  
    Define settings for the configuration database. The database where all the metadata is stored. Do not change if not necessary.

  
```python
##### DO NOT CHANGE UNLESS SPECIFIED OTHERWISE ####
configuration = {
                    'workspace': {
                        'name' : domain_name + ' CONFIG' +  framework_post_fix,             # Name of target workspace
                        'roles' : workspace_roles_data,                                     # Roles to assign to the workspace
                        'capacity_name' : capacity_name_config                              # Name of target capacity for the configuration workspace
                    },
                       'DatabaseName' : 'SQL_'+domain_name+'_FRAMEWORK'                     # Name of target configuration SQL Database
}
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
        },
        'connections': {
            'CON_FMD_FABRIC_SQL': '00000000-0000-0000-0000-000000000000',          # GUID for the Fabric SQL connection
            'CON_FMD_FABRIC_PIPELINES': '00000000-0000-0000-0000-000000000000',    # GUID for the Fabric Data Pipelines connection
            'CON_FMD_ADF_PIPELINES': '00000000-0000-0000-0000-000000000000'        # GUID for the ADF connection (if used)
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
        },
        'connections': {
            'CON_FMD_FABRIC_SQL': '00000000-0000-0000-0000-0000000000000',          # GUID for the Fabric SQL connection
            'CON_FMD_FABRIC_PIPELINES': '00000000-0000-0000-0000-000000000000',    # GUID for the Fabric Data Pipelines connection
            'CON_FMD_ADF_PIPELINES': '00000000-0000-0000-0000-000000000000'        # GUID for the ADF connection (if used)
        }
    }
]
```
**Domain Settings** 

Define settings for every business domain.
  ```python
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
                            }
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

### Load Demo data

When load_demo_data = True, you have to upload a csv file (which is available in this repo). With this you can easily test if every pipeline and the full process is working

1. **Upload** `customer.csv` from demodata folder in this repo to the file section of `LH_DATA_LANDINGZONE` in the development environment.
2. **Create table:** Generate a table named `in_customer` from the uploaded file. If you use schema-enabled lakehouse, use `dbo.in_customer`.
3. **Run process:** Execute the process to validate deployment, by start the `PL_LOAD_ALL` pipeline in the `FMD_FRAMEWORK_CODE (D)` workspace.
Example of loading the file and creating the table:

![Load File to table](./Images/FMD_load_file_to_table.png)

Example of loading the file and creating the table with Lakehouse schema enabled:

![Load File to table](./Images/FMD_load_file_to_table_schema.png)


