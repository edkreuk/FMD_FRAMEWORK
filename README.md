## Introduction

Fabric Metadata driven FRAMEWORK (short: FMD) is a solution to enable a holistic monitoring on top of Power BI and Fabric. 
Today monitoring for Fabric can be done through different reports, apps and tools. Here is a short overview about the available monitoring solutions which are shipped with Fabric:
- Feature Usage & Adoption
- Purview Hub
- Capacity Metrics App
- Log Analytics Integration at Workspace Level
- Usage Metrics Report



## FMD_FRAMEWORK Deployment

### Requirements

You need to have the Fabric Adminstrator Role fot his action
The admin settings below needs to be enabled:
- Users can create Fabric items
- SQL database (preview)​



The following steps need to be done:

### 1. Download Files

Download the three files 
- FMD_deployment.json : Contains all definitions to create all artifacts for the FMD_FRAMEWORK (Input for the notebook below)
- NB_FMD_SETUP_MULTI_ENV.ipynb : Creates all artifacts for the FMD_FRAMEWORK within Fabric Data Platform(baed on your configuration)
- NB_FMD_UTILS.ipynb: Contains all definition to Creates all artifacts
to your local machine.

or 

1. Clone this repository:
   ```bash
   git clone https://github.com/edkreuk/FMD_FRAMEWORK.git
   cd Fabric/FabricSolutionInit
   ```


### 2. Create and authorize Service Principal

Create a new service principal with client secret within Azure Entra ID, without any API permissions. 


# You need to have the Fabric Adminstrator Role fot his action
Add the service principal to a group(SG_FABRIC_SERVICES) 
The admin settings below needs to be enabledand the above group needs to be assigned
- Service Principals can use Fabric APIs
- Service Principals can access read-only admin APIs





### 3. Create Connections

Create the following two connections using Service Principal authorization:
| | Connection |
|-------------| ------------- | ------------- |
|Connection Name| CON_FMD_FABRIC_ADMIN_API  |
|Connection Type|  Web v2  |
|Base Url  | https://api.fabric.microsoft.com/v1/admin  |
|Token Audience Url|https://api.fabric.microsoft.com|
|Authentification|Service Principal|

Write down the Connection IDs for later usage.


### 4. Create Workspace and Config Lakehouse

- Create a new workspace "FMD_FRAMEWORK_CONFIGURATION" (Name is up to you)
- Create a new Lakehouse called "LH_CONFIGURATION"
- Within the files section, create a subfolder named "deployment"
- Upload the file "FMD_deployment.json" to the new folder

![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_basic_add_deployment_file.png)


### 5. Import Notebooks

- Import the notebook "NB_FMD_SETUP_MULTI_ENV.ipynb" into the workspace
- Import the notebook "NB_FMD_UTILS.ipynb" into the workspace


### 6. Notebook Configuration
- Open the notebook and adjust the following variables with the connection IDs
  - conn_pbi_service_api_admin
  - conn_fabric_service_api_admin
-  Press "Run All" to execute the deployment of the FUAM artifacts

In case of an error, you'll be able to run the notebook again. It has an update mechansim, which will handle an update










## Contributing

Contributions are welcome! If you have suggestions or improvements, feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License.