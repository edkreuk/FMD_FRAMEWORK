## Introduction to FMD Framework

### Fabric Metadata-Driven Framework (FMD)

Efficient data management is essential for modern organizations, and the Fabric Metadata-Driven Framework (FMD) offers a state-of-the-art solution to streamline data operations. This framework leverages the advanced capabilities of the Fabric SQL Database to establish a robust, scalable, and metadata-driven architecture.

Fabric Metadata-Driven Framework is built on dynamic data pipelines and parameterized notebooks, fully configured through the Fabric SQL Database. It works seamlessly out of the box, requiring no initial modifications. Organizations can easily extend and scale the framework to meet evolving data needs, ensuring flexibility and adaptability for future growth.

![FMD Framework Deployment](/Images/FMD_FRAMEWORK.jpeg)

# Key Contributors
- [Erwin de Kreuk][ErwinLink]
- [Marco Hansma][MarcoLink]

## Key Features:

-**Enhanced Data Governance:** 
Fabric Metadata-Driven Framework ensures comprehensive data governance by maintaining detailed metadata, enabling better data quality, consistency, and compliance.

-**Scalability and Flexibility:** 
The framework is designed to scale seamlessly with your organization's growth, adapting to evolving data needs without compromising performance.

-**Streamlined Data Integration:** 
The framework simplifies the integration of diverse data sources, providing a unified view of your data landscape.

-**Cost Efficiency:** 
By optimizing data processes and reducing redundancy, Fabric Metadata-Driven Framework helps organizations achieve significant cost savings.

## Metadata-driven Data Ingestion Framework Components

![FMD Framework Deployment](/Images/FMD_TASKFLOW_OVERVIEW.png)

### Workspaces Structure

The Framework deploys the following default Workspace Architecture to ensure a clear separation of data, code, and orchestration for enhanced security. This structure is designed to restrict access based on roles, ensuring that individuals who require access to data do not necessarily have access to code or orchestration components.

#### Recommended Structure

1. **Data Workspaces**  
  Dedicated workspaces for managing and storing data:
  - **Data Landing Zone**  
  - **Bronze Layer**  
  - **Silver Layer**  

2. **Code Workspaces**  
  Workspaces for managing code and development artifacts:
  - **Data Pipelines**  
  - **Notebooks**  
  - **Spark Environments**  

3. **Orchestration and Logging Workspaces**  
  Workspaces for orchestration and monitoring:
  - **Fabric SQL Database**  
  - **Semantic Model for Auditing and Logging** *(Work in Progress)*  

#### Additional Recommendation for Gold Layer  
If a Gold Layer is added, it is advisable to create a separate workspace for reports. This ensures that users who need access to reports do not require access to the data workspace. This recommendation is based on practical experience from recent customer implementations.

![FMD Workspace Overview](/Images/FMD_WORKSPACE_OVERVIEW.png)

### Medallion Architecture

The Framework implements a structured approach to data organization using the Medallion Architecture. This architecture is supported by the deployment of Lakehouses for the **Data Landing Zone**, **Bronze Layer**, and **Silver Layer**. All data pipelines and notebooks are orchestrated and executed based on this architecture.

#### Data Landing Zone
- Handles both structured and unstructured data.
- Supports incremental data loads.
- Stores raw data "as-is" in a datetime-based folder structure.
- No schema is enforced at this stage.

#### Bronze Layer
- Deduplicates data.
- Adds data types for better structure.
- Data may still be inconsistent.
- Primarily serves as a copy of the source data.
- Schema is applied.

#### Silver Layer
- Maintains historical data.
- Enforces data quality rules and performs data cleansing.
- Stores validated data.
- Does not include business-specific models or data.

![FMD Lakehouse Overview](/Images/FMD_LAKEHOUSE_OVERVIEW.png)


### Getting Started

To begin using the FMD Framework, refer to the deployment guide:

[FMD Framework Deployment][fmdFrameworkDeployment]

### Additional Resources

- **Data Model Details:** Learn more about the framework's data model and its components.  
  [FMD Framework DataModel][fmdDataModelLink]

- **Data Pipelines Overview:** Explore the data pipelines used within the framework.  
  
  [FMD Framework Data Pipelines][fmdDataPipelinesLink]

- **Easily Configure and Load Data into the Framework:** Concept to easily extract metadata from you sql server into the Framework.  
  
  [FMD Configure and Load data][fmdDataLoadLink]

- **Pipelines Logging:** Logging and auditing information.
  
  [FMD Auditing and Logging][fmdDataLoggingLink]

## Supported Sources

The FMD Framework supports a wide range of data sources, enabling seamless integration and data ingestion. Below is the list of supported sources:

- **SQL Server**  
  Connect and ingest data from on-premises or cloud-hosted SQL Server databases.

- **Azure Data Lake Gen2**  
  Leverage Azure Data Lake Gen2 for scalable and secure data storage and processing.

- **SFTP**  
  Securely transfer and ingest files using the SFTP protocol.

- **FTP**  
  Ingest data from legacy systems using the FTP protocol.

- **Azure Data Factory**  
  Utilize Azure Data Factory for orchestrating and automating data workflows.

- **Onelake Tables**  
  Integrate with Onelake Tables for unified data access and management.

- **Onelake Files**  
  Access and process files stored in Onelake for streamlined data operations.

### Remarks

- **Fabric SQL Database Limitations:**  
  The Fabric SQL Database may encounter failures, often due to exceeding the allowed number of Fabric Databases in your tenant. To verify this, attempt to create the Fabric Database manually. This will immediately indicate if the issue is related to database limits.  

- **Trial Capacity Restrictions:**  
  Note that trial capacities are limited to a maximum of three databases. Ensure that your deployment does not exceed this limit.  

- **Error Handling During Deployment:**  
  During deployment, the notebook may display the following error. If this occurs, you can always re-run the notebook to resolve the issue.
  
![Fabric Experience](/Images/FMD_DATABASE_ERROR_NOTEBOOK.png)

  You can check this by creating the database manually
  
![Fabric Database Error](/Images/FMD_DATABASE_ERROR.png)
### Test Process

1. **Upload the File**  
  Upload the `customer.csv` file to the **file section** of `LH_DATA_LANDINGZONE` in the **Development environment**.

2. **Create a Table**  
  Create a table from the uploaded file and name it `in_customer`.

3. **Run the Process**  
  Once the table is created, execute the complete process to verify that everything has been deployed and configured correctly.

![Load File to table](/Images/FMD_load_file_to_table.png)


## Contributing

Contributions are welcome! If you have suggestions or improvements, feel free to open an issue or submit a pull request.

## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE.


[fmdFrameworkDeployment]: /FMD_FRAMEWORK_DEPLOYMENT.md
[fmdDataModelLink]: /FMD_DATA_MODEL.md
[fmdDataLoggingLink]: /FMD_DATA_LOGGING.md
[fmdDataPipelinesLink]: /FMD_DATA_PIPELINES.md
[fmdDataLoadLink]: /FMD_LOAD_DATA.md


[ErwinLink]:  https://www.linkedin.com/in/erwindekreuk/
[MarcoLink]: https://www.linkedin.com/in/marcohansma/
