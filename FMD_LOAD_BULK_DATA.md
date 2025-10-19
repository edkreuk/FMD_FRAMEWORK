---
Title: Configure and load Bulk data into the FMD Framework
Description: Learn how to use the PL_TOOLING_POST_ASQL_TO_FMD pipeline to insert metadata and load bulk data into the Fabric Metadata-Driven (FMD) Framework.
Topic: how-to
Date: 07/2025
Author: edkreuk
---

# Configure and load bulk data into the FMD Framework

This article describes how to configure and load demo data into the Fabric Metadata-Driven (FMD) Framework using the `PL_TOOLING_POST_ASQL_TO_FMD` pipeline.

## Overview

The `PL_TOOLING_POST_ASQL_TO_FMD` pipeline assists in inserting metadata from one of your SQL connections into the metadata-driven database. Before running the pipeline, ensure that a SQL connection is created and registered in the framework.

![PL_TOOLING_POST_ASQL_TO_FMD](/Images/PL_TOOLING_POST_ASQL_TO_FMD.png)

## Required parameters

Set the following parameters when running the pipeline:

| Parameter name         | Description                                   | Example                                         |
|------------------------|-----------------------------------------------|-------------------------------------------------|
| `ConnectionGuid`       | GUID of the SQL connection                    | `cf673e6a-13f6-4ebb-9cbb-4ba4ab390818`          |
| `Data_WorkspaceGuid`   | Workspace GUID where the Lakehouse is deployed| `pf7647b15-7df1-4db5-99cb-90eee61737a4`         |
| `DatasourceName`       | Name of the database                          | `WideWorldImporters`                            |
| `DatasourceNamespace`  | Prefix for tables or files                    | `wwi`                                           |
| `DatasourceType`       | Type depending on your connection             | `ASQL_01`                                       |
| `Tables`               | List of tables to include (comma-separated)   | `PurchaseOrders,PurchaseOrderLines,Orders,OrderLines,Invoices,InvoiceLines,BuyingGroups,CustomerCategories` |

> [!TIP]
> Ensure all parameter values match your environment and connection details.

## Next steps

- After configuring the parameters, run the `PL_TOOLING_POST_ASQL_TO_FMD` pipeline to load bulk data and metadata into the FMD Framework.
- For more information on configuring connections and workspaces, see [FMD Framework Deployment Guide](./FMD_FRAMEWORK_DEPLOYMENT.md).
