
## Deploy the FMD_FRAMEWORK

![FMD Overview](/Images/FMD_Overview.png)

This article describes how to deploy the FMD_FRAMEWORK in Microsoft Fabric. Follow the steps below to configure your environment, set up required connections, and apply deployment settings.

### Prerequisites

Before you begin, ensure the following admin settings are enabled:

- Users can create Fabric items
- Fabric SQL database is available
- Contributor role assigned on the target capacity or capacities

### Deployment steps

#### 1. Download deployment assets

Download the deployment notebook to your local machine:

- `NB_SETUP_FMD.ipynb` – Automates artifact creation for FMD_FRAMEWORK in Fabric, based on your configuration.

Alternatively, clone the repository:

```bash
git clone https://github.com/edkreuk/FMD_FRAMEWORK.git
```

#### 2. Create required connections

Set up the following connections and note their Connection IDs for later configuration:

| Connection name              | Connection type           | Authentication |
|------------------------------|--------------------------|----------------|
| CON_FMD_FABRIC_PIPELINES     | Fabric Data Pipelines    | OAuth2         |
| CON_FMD_FABRICSQL            | Fabric SQL database      | OAuth2         |

If you use Azure Data Factory Pipelines, create this additional connection:

| Connection name              | Connection type           | Authentication |
|------------------------------|--------------------------|----------------|
| CON_FMD_ADF_PIPELINES        | Azure Data Factory        | OAuth2         |

#### 3. Create a configuration workspace

- Create a new workspace (for example, `FMD_FRAMEWORK_CONFIGURATION`).
- Import the deployment notebook into the workspace (ensure you are in the Fabric Experience):
	- `NB_SETUP_FMD.ipynb`

![Fabric Experience](/Images/FMD_Fabric_Experience.png)

#### 4. Configure deployment settings

Open `NB_SETUP_FMD.ipynb` and navigate to the configuration cell. Update the following parameters as needed.

##### Key configuration parameters

1. **Capacity ID**  
	 Specify the unique identifier for the capacity:

	 ```python
	 capacity_name = 'Name of your capacity'
	 ```

2. **Workspace roles**  
	 Assign security roles to workspaces:

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

3. **Environment configuration**  
	 Define settings for each environment (for example, development and production). You can add multiple environments as needed. Each environment should include workspace configurations, roles, capacity IDs, and connection details.

	 ```python
	 environments = [
			 {
					 'environment_name': 'development',
					 'workspaces': {
							 'data': {
									 'name': 'FMD_FRAMEWORK_DATA (D)',
									 'roles': workspace_roles,
									 'capacity_name': capacity_name_dvlm
							 },
							 'code': {
									 'name': 'FMD_FRAMEWORK_CODE (D)',
									 'roles': workspace_roles,
									 'capacity_name': capacity_name_dvlm
							 }
					 },
					 'connections': {
							 'CON_FMD_FABRIC_SQL': '372237f9-709a-48f8-8fb2-ce06940c990e',
							 'CON_FMD_FABRIC_PIPELINES': '6d8146c6-a438-47df-94e2-540c552eb6d7',
							 'CON_FMD_ADF_PIPELINES': '02e107b8-e97e-4b00-a28c-668cf9ce3d9a'
					 }
			 },
			 {
					 'environment_name': 'production',
					 'workspaces': {
							 'data': {
									 'name': 'FMD_FRAMEWORK_DATA (P)',
									 'roles': workspace_roles,
									 'capacity_name': capacity_name_prod
							 },
							 'code': {
									 'name': 'FMD_FRAMEWORK_CODE (P)',
									 'roles': workspace_roles,
									 'capacity_name': capacity_name_prod
							 }
					 },
					 'connections': {
							 'CON_FMD_FABRIC_SQL': '372237f9-709a-48f8-8fb2-ce06940c990e',
							 'CON_FMD_FABRIC_PIPELINES': '6d8146c6-a438-47df-94e2-540c552eb6d7',
							 'CON_FMD_ADF_PIPELINES': '02e107b8-e97e-4b00-a28c-668cf9ce3d9a'
					 }
			 }
	 ]
	 ```

4. **Run the deployment**  
	 Execute the notebook to apply your configuration and deploy the framework.

---

## Data cleansing

You can define data cleansing rules for the Bronze and Silver layers. Cleansing rules are specified as a JSON array, where each object defines a function, target columns, and optional parameters.

- `function`: Name of the cleansing function
- `columns`: Semicolon-separated list of columns
- `parameters`: (Optional) JSON object with function parameters

**Example:**

```json
[
		{"function": "to_upper", "columns": "TransactionTypeName"},
		{"function": "custom_function_with_params", "columns": "TransactionTypeName;LastEditedBy", "parameters": {"param1": "abc", "param2": "123"}}
]
```

### Add custom cleansing functions

Custom functions can be added in `NB_FMD_DQ_CLEANSING`. Each function should use the following structure:

```python
def <function_name>(df, columns, args):
		# Access custom parameters
		print(args['<custom parameter name>'])

		# Apply logic to each column
		for column in columns:
				df = df.<custom logic>

		return df  # Always return the DataFrame
```

For more examples, see [Data Cleansing Examples][fmdDataCleansingExamplesLink].

[fmdDataCleansingExamplesLink]: /FMD_DATA_CLEANSING.MD

