# FMD Data Pipelines

![FMD Process Overview](/Images/FMD_PROCESS_OVERVIEW.png)



**PL_TOOLING_POST_ASQL_TO_FMD**

Can help to insert Metadata from one of your connections, so make sure created SQL Connection, which is insert in the FMD Database

Parameters to be set

| Parameter Name         | Description         | Example                        | 
|---------------------|--------------------|------------------------------------|
| ConnectionGuid        | Guid of the connection                | cf673e6a-13f6-4ebb-9cbb-4ba4ab390818       |  
| Data_WorkspaceGuid        | Workspace where the Lakehouse are deployed              | Pf7647b15-7df1-4db5-99cb-90eee61737a4        |  
| DatasourceName        | Name of the database                | WideWorldImporters        |  
| DatasourceNamespace        | prefix tables/files                | wwi       |  
| DatasourceType        | Type depends of your Connection                | ASQL_01        |  
| Tables        | int                | ('PurchaseOrders' ,'PurchaseOrderLines' ,'Orders' ,'OrderLines' ,'Invoices' ,'InvoiceLines' ,'BuyingGroups' ,'CustomerCategories')        |  
