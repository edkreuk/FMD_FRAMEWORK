
## FMD_FRAMEWORK Deployment
![FMD Overview](/Images/FMD_Overview.png)

### Requirements

The admin settings below need to be enabled:
- Users can create Fabric items
- Fabric SQL database 
Contributor role on a capacity or capacities


### Steps for Deployment

#### 1. Download Files

Download the following files to your local machine:
- `NB_FMD_DEPLOYMENT_MULTI_ENV.ipynb`: Creates all artifacts for the FMD_FRAMEWORK within the Fabric Data Platform (based on your configuration).
- `NB_FMD_DEPLOYMENT_UTILS.ipynb`: Contains all definitions to create all artifacts.

Alternatively, clone the repository:
```bash
git clone https://github.com/edkreuk/FMD_FRAMEWORK.git
```

#### 2. Create Connections

Create the following connections and note their Connection IDs for later use:

| **Connection Name**       | **Connection Type**       | **Authentication** |
|----------------------------|---------------------------|---------------------|
| CON_FMD_FABRIC_PIPELINES    | Fabric Data Pipelines     | OAuth2              |
| CON_FMD_FABRICSQL          | Fabric SQL database       | OAuth2              |

If using Azure Data Factory Pipelines, create this additional connection:

| **Connection Name**       | **Connection Type**       | **Authentication** |
|----------------------------|---------------------------|---------------------|
| CON_FMD_ADF_PIPELINES      | Azure Data Factory        | OAuth2              |

#### 3. Create Workspace for Configuration

- Create a new workspace (e.g., `FMD_FRAMEWORK_CONFIGURATION`).
- Import the notebooks into the workspace (ensure you are in the Fabric Experience):
	- `NB_FMD_DEPLOYMENT_MULTI_ENV.ipynb`
	- `NB_FMD_DEPLOYMENT_UTILS.ipynb`

![Fabric Experience](/Images/FMD_Fabric_Experience.png)

#### 4. Configure Deployment Settings

Open the notebook `NB_FMD_DEPLOYMENT_MULTI_ENV.ipynb` and navigate to the third cell to configure deployment settings.

##### Key Configuration Details

1. **Capacity ID**  
	 Define the unique identifier for the capacity to be used:
	 ```python
	 capacity_id = '075e5656-1234-5678-a36c8c0e8bca'
	 ```

2. **Workspace Roles**  
	 Assign roles to workspaces:
	 ```python
	 workspace_roles = [
			 {
					 "principal": {
							 "id": "00000000-0000-0000-0000-000000000000",
							 "displayName": "sg-fabric-contributor",
							 "type": "Group"
					 },
					 "role": "Member"
			 },
			 {
					 "principal": {
							 "id": "00000000-0000-0000-0000-000000000000",
							 "displayName": "sg-fabric-admin",
							 "type": "Group"
					 },
					 "role": "Admin"
			 }
	 ]
	 ```

3. **Environments**  
	 Define settings for development and production stages:
	 You can optionally add as many environments as needed. Each environment should include workspace configurations, roles, capacity IDs, and connection details. Below is an example structure for defining multiple environments:
	 
	 ```python
	 environments = [
			 {
					 'environment_name': 'development',
					 'workspaces': {
							 'data': {
									 'name': 'FMD_FRAMEWORK_DATA (D)',
									 'roles': workspace_roles,
									 'capacity_id': capacity_id_dvlm
							 },
							 'code': {
									 'name': 'FMD_FRAMEWORK_CODE (D)',
									 'roles': workspace_roles,
									 'capacity_id': capacity_id_dvlm
							 }
					 },
					 'connections': {
							 'CON_FMD_FSQL': '372237f9-709a-48f8-8fb2-ce06940c990e',
							 'CON_FMD_ASQL_01': 'cf673e6a-13f6-4ebb-9cbb-4ba4ab390818',
							 'CON_FMD_ADLS_01': 'a0581b6e-5e38-46eb-bab2-7f08e9a35c30',
							 'CON_FMD_FABRIC_PIPELINES': '6d8146c6-a438-47df-94e2-540c552eb6d7'
					 }
			 },
			 {
					 'environment_name': 'production',
					 'workspaces': {
							 'data': {
									 'name': 'FMD_FRAMEWORK_DATA (P)',
									 'roles': workspace_roles,
									 'capacity_id': capacity_id_prod
							 },
							 'code': {
									 'name': 'FMD_FRAMEWORK_CODE (P)',
									 'roles': workspace_roles,
									 'capacity_id': capacity_id_prod
							 }
					 },
					 'connections': {
							 'CON_FMD_FSQL': '372237f9-709a-48f8-8fb2-ce06940c990e',
							 'CON_FMD_ASQL_01': 'cf673e6a-13f6-4ebb-9cbb-4ba4ab390818',
							 'CON_FMD_ADLS_01': 'a0581b6e-5e38-46eb-bab2-7f08e9a35c30',
							 'CON_FMD_FABRIC_PIPELINES': '6d8146c6-a438-47df-94e2-540c552eb6d7'
					 }
			 }
	 ]
	 ```

4. **Execute**  
	 Execute the notebook to apply the settings.



**Data Cleansing**

You can add data cleansing rules for the Bronze and Silver Layer

It is possible to perform cleansing functions on incoming data. For example, converting all text in a column to uppercase. This can be achieved by defining cleansing rules for a table. The cleansing_rules contains a piece of JSON as shown below. This is an array of one or more functions that need to be called.

function: name of the function
columns: semi-colon separated list of columns to which the function should be applied
parameters: JSON string with the different parameters and their values

Example:
```json
[
	{"function": "to_upper",
	 "columns": "TransactionTypeName"}, 
	{"function": "custom_function_with_params",
	 "columns": "TransactionTypeName;LastEditedBy",
	 "parameters": {"param1" : "abc", "param2" : "123"}}
]
```

## Custom functions

Custom functions can be added in NB_FMD_DQ_CLEANSING . The function has the following structure.


```python
def <functioname> (df, columns, args):

	print(args['<custom parameter name>']) # use of custom parameters

	for column in columns: # apply function foreach column
		df = df.<custom logic>

	return df #always return dataframe.
```

More data cleansing examples:

[Data Cleansing Examples][fmdDataCleansingExamplesLink]

[fmdDataCleansingExamplesLink]: /FMD_DATA_CLEANSING.MD