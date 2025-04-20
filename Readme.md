## Introduction to FMD Framework

Fabric Metadata-Driven Framework (FMD)

Efficient data management is a cornerstone of modern organizations, and leveraging the right tools can make all the difference. The Fabric Metadata-Driven Framework is a cutting-edge solution designed to optimize data handling and utilization. This innovative framework harnesses the powerful capabilities of the Fabric SQL Database to build a robust, scalable, and flexible metadata-driven architecture.

![FMD Framework Deployment](/Images/FMD_FRAMEWORK.jpeg)

# Key Contributors
- Erwin de Kreuk
- Marco Hansma

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

### Getting Started

To begin using the FMD Framework, refer to the deployment guide:

[FMD Framework Deployment][fmdFrameworkDeployment]

### Additional Resources

- **Data Model Details:** Learn more about the framework's data model and its components.  
  [FMD Framework DataModel][fmdDataModelLink]

- **Data Pipelines Overview:** Explore the data pipelines used within the framework.  
  [FMD Framework Data Pipelines][fmdDataPipelinesLink]

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
## Easily Configure and Load Data in the Framework

### **PL_TOOLING_POST_ASQL_TO_FMD**

The `PL_TOOLING_POST_ASQL_TO_FMD` tool assists in inserting metadata from one of your connections. Ensure that a SQL connection is created and inserted into the metadata-driven database.

![PL_TOOLING_POST_ASQL_TO_FMD](/Images/PL_TOOLING_POST_ASQL_TO_FMD.png)

### Parameters to be Set

| **Parameter Name**        | **Description**                                   | **Example**                                   | 
|---------------------------|---------------------------------------------------|-----------------------------------------------|
| `ConnectionGuid`          | GUID of the connection                           | `cf673e6a-13f6-4ebb-9cbb-4ba4ab390818`       |  
| `Data_WorkspaceGuid`      | Workspace where the Lakehouse is deployed        | `Pf7647b15-7df1-4db5-99cb-90eee61737a4`      |  
| `DatasourceName`          | Name of the database                             | `WideWorldImporters`                         |  
| `DatasourceNamespace`     | Prefix for tables/files                          | `wwi`                                        |  
| `DatasourceType`          | Type depending on your connection                | `ASQL_01`                                    |  
| `Tables`                  | List of tables to include                        | `('PurchaseOrders', 'PurchaseOrderLines', 'Orders', 'OrderLines', 'Invoices', 'InvoiceLines', 'BuyingGroups', 'CustomerCategories')` |


## Contributing

Contributions are welcome! If you have suggestions or improvements, feel free to open an issue or submit a pull request.

## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE.


[fmdFrameworkDeployment]: /FMD_FRAMEWORK_DEPLOYMENT.md
[fmdDataModelLink]: /FMD_Datamodel.md
[fmdDataLoggingLink]: /FMD_DATA_LOGGING.md
[fmdDataPipelinesLink]: /FMD_DATA_PIPELINES