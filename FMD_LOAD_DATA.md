## Easily Configure and Load Data into the Framework

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
