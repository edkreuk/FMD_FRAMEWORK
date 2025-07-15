---
title: Fabric Metadata-Driven (FMD) Framework Overview
description: Discover the architecture, components, and deployment guidance for the Fabric Metadata-Driven (FMD) Framework on Fabric SQL Database.
ms.service: fabric
ms.topic: overview
ms.date: 07/2025
author: edkreuk
---

# Fabric Metadata-Driven (FMD) Framework Overview

The **Fabric Metadata-Driven (FMD) Framework** is a scalable, extensible solution for managing, integrating, and governing data using a metadata-driven approach on Fabric SQL Database. This article provides an overview of the FMD Framework, including its architecture, core components, workspace structure, supported data sources, and guidance for deployment and troubleshooting.

## What is the FMD Framework?

The FMD Framework enables organizations to streamline data operations by leveraging metadata to drive dynamic data pipelines and parameterized notebooks. Built on Fabric SQL Database, the framework supports secure, flexible, and modern data management at scale.

> [!TIP]
> The FMD Framework is designed for rapid deployment and extensibility. You can use it out-of-the-box or customize it to meet your organization's evolving data needs.

![FMD Framework Deployment](./Images/FMD_FRAMEWORK.jpeg)

## Key Features

- **Comprehensive Data Governance**  
  Maintain detailed metadata for improved data quality, consistency, and compliance.

- **Scalability and Flexibility**  
  Seamlessly scale with organizational growth and adapt to changing data requirements.

- **Streamlined Data Integration**  
  Integrate diverse data sources for a unified data landscape.

- **Cost Efficiency**  
  Optimize data processes and reduce redundancy to achieve cost savings.

## Architecture and Components

The FMD Framework is organized around a modular architecture that separates data, code, and orchestration for enhanced security and manageability.

### Workspace Architecture

The recommended workspace structure ensures clear separation of responsibilities and access control:

| Workspace Type                | Purpose                                      | Examples                        |
|-------------------------------|----------------------------------------------|----------------------------------|
| **Data Workspaces**           | Manage and store data                        | Data Landing Zone, Bronze, Silver|
| **Code Workspaces**           | Develop and manage code artifacts            | Data Pipelines, Notebooks, Spark |
| **Orchestration & Logging**   | Orchestrate and monitor data operations      | Fabric SQL Database, Auditing    |

> [!NOTE]
> For organizations with a Gold Layer, create a separate workspace for reports to restrict access appropriately.

![FMD Workspace Overview](./Images/FMD_WORKSPACE_OVERVIEW.png)

### Medallion Architecture

The framework implements the Medallion Architecture using Lakehouses:

- **Data Landing Zone**: Stores raw, unstructured, and structured data with incremental loads and no enforced schema.
- **Bronze Layer**: Deduplicates and structures data, applying schema for consistency.
- **Silver Layer**: Maintains historical, validated data with enforced quality rules.

![FMD Lakehouse Overview](./Images/FMD_LAKEHOUSE_OVERVIEW.png)

### Taskflow Import

To deploy the default taskflow:

1. Import `FMD_FABRIC_TASKFLOW.json` from the Taskflow folder into your workspace.
2. Assign the correct artifacts to the taskflows as shown in the documentation.

## Supported Data Sources

The FMD Framework supports integration with the following sources:

- **SQL Server** (on-premises or cloud)
- **Azure Data Lake Gen2**
- **SFTP / FTP**
- **Azure Data Factory**
- **Onelake Tables and Files**

## Deployment and Getting Started

To get started:

1. Review the [FMD Framework Deployment Guide](./FMD_FRAMEWORK_DEPLOYMENT.md).
2. Import the taskflow and configure your workspaces as recommended.
3. Refer to additional resources for data model, pipelines, and logging.

## Additional Resources

- [FMD Framework Data Model](./FMD_DATA_MODEL.md)
- [FMD Framework Data Pipelines](./FMD_DATA_PIPELINES.md)
- [Configure and Load Data](./FMD_LOAD_DATA.md)
- [Auditing and Logging](./FMD_DATA_LOGGING.md)

## Troubleshooting

- **Fabric SQL Database Limitations**:  
  If deployment fails, verify that you have not exceeded the allowed number of Fabric Databases in your tenant. Trial capacities are limited to three databases.

- **Error Handling**:  
  If you encounter errors during deployment (e.g., database creation failures), re-run the notebook or manually create the database to diagnose the issue.

![Fabric Database Error](./Images/FMD_DATABASE_ERROR.png)



## Contributing

We welcome contributions! To suggest improvements, open an issue or submit a pull request.

## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE.

---

**Contributors:**  
[Erwin de Kreuk](https://www.linkedin.com/in/erwindekreuk/)  
[Marco Hansma](https://www.linkedin.com/in/marcohansma/)