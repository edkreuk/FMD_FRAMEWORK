# 📘 Fabric Metadata‑Driven Framework (FMD)
### *A Microsoft Fabric Framework for Metadata‑Driven Data Pipelines*

The **Fabric Metadata‑Driven Framework (FMD)** is a powerful and extensible **Microsoft Fabric framework** designed to automate, orchestrate, and standardize **metadata‑driven data pipelines**. Built for lakehouse‑first architectures, FMD provides a configurable and reusable approach to ingestion, transformation, governance, and automation across Microsoft Fabric.

## 🚀 Why the FMD Framework?
Modern data platforms demand agility, scalability, and consistency. FMD simplifies these challenges by enabling:
- **Dynamic, metadata‑driven pipelines**  
- **Consistent orchestration across ingestion, processing, and publishing**  
- **Centralized configuration** for all data entities  
- **Alignment with Microsoft Fabric Lakehouse & Medallion Architecture**  
- **Reduced engineering effort** through reusable patterns  
- **Faster delivery** with standardized, tested components  

## Overview

The FMD Framework enables organizations to streamline data operations by leveraging metadata to drive dynamic data pipelines and parameterized notebooks. Built on Fabric SQL Database, the framework supports secure, flexible, and modern data management at scale.

> [!TIP]
> The FMD Framework is designed for rapid deployment and extensibility. You can use it out-of-the-box or customize it to meet your organization's evolving data needs.


## Video with the Data Factory Team

[![Watch the FMD Framework overview](https://img.youtube.com/vi/UzqSFajSvtY/0.jpg)](https://www.youtube.com/watch?v=UzqSFajSvtY&t=829s)

Watch this conversation with the Azure Data Factory team for a walkthrough of the concepts described in [🚀 Why the FMD Framework?](#-why-the-fmd-framework).

- Dynamic, metadata‑driven pipelines
- Consistent orchestration across ingestion, processing, and publishing
- Centralized configuration for all data entities
- Alignment with Microsoft Fabric Lakehouse & Medallion Architecture
- Reduced engineering effort through reusable patterns
- Faster delivery with standardized, tested components

### ✔ Dynamic Pipelines
Automatically adjusts pipeline execution based on metadata—ideal for large‑scale, multi‑source environments.
## Key Features
### ✔ Scalable & Extensible
Modular design allows custom logic, new transformations, and custom patterns without breaking existing workloads.

### ✔ Governance & Observability
Track rows processed, load statuses, timestamps, and operational metrics through centralized metadata.

## 🏗️ Architecture Overview
The FMD Framework is built on top of core **Microsoft Fabric** capabilities:

The FMD Framework uses a modular architecture that separates data, code, and orchestration for enhanced security and manageability.

- **[Workspace architecture](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Workspace-architecture)**
- **[Medallion architecture](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Medallion-architecture)**
- **[Supported data sources](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Supported-data-sources)**
- **[Data Cleansing](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Cleansing)**
- **[Logging and auditing](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Logging-and-auditing)**  
- **[Variable Library](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Variable-Library)**
- **[Data Pipelines and Notebooks](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Pipelines-and-Notebooks)**
- **[Taskflow orchestration](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Taskflow)**
- **[Business Domains](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Business-Domains)**

## Deployment and getting started

To get started:

1. Review the **[FMD Framework Deployment Guide](./FMD_FRAMEWORK_DEPLOYMENT.md)**.
2. Set up the required connections in your Fabric environment.
3. Configure the deployment parameters as per your environment.
4. Deploy the FMD Framework using the provided deployment scripts.
5. Import the taskflow and configure your workspaces as recommended.
6. Refer to **[wiki](https://github.com/edkreuk/FMD_FRAMEWORK/wiki)** for data model, pipelines, and logging.

## Business Domain Deployment
Review the **[Business Domain Deployment Framework Deployment Guide](./FMD_BUSINESS_DOMAIN_DEPLOYMENT.md)**.


## Additional resources

| Resource | Description |
|----------|-------------|
| **[FMD Integration Framework reference](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Integration)** |Overview on how to add sources and demo data to the FMD Framework |
| **[FMD Data Model reference](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Model)** | Overview of the data model used in the FMD Framework |
| **[Configure and load demo data](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Integration#configure-and-load-bulk-data-into-the-fmd-framework)** | Instructions for loading demo data into the FMD Framework |

## Troubleshooting
[Troubleshooting](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Troubleshooting)

## 📚 Documentation
Full overview & tutorials: https://erwindekreuk.com/fmd-framework/
| **[FMD Integration Framework reference](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Integration)** | Overview on how to add sources and demo data to the FMD Framework |
## 📦 Use Cases
- Enterprise‑grade ingestion frameworks  
- Reusable ingestion + transformation patterns  
- Standardized data architecture for Fabric Lakehouse  
- Multi‑source data ingestion using dynamic pipelines  
- Rapid onboarding of new datasets  
- Migration from Synapse or ADF metadata‑driven solutions  

## 🤝 Contributing
Contributions, enhancements, and feature requests are welcome.  To contribute:

1. Fork the repository.
2. Create a feature branch.
3. Commit your changes.
4. Push to the feature branch.
5. Create a pull request and add documentation on what you have changed.

📝 License
Contributions, enhancements, and feature requests are welcome. To contribute:

***

**Contributors:**  
[Erwin de Kreuk](https://www.linkedin.com/in/erwindekreuk/)  
## License
This project is released under the MIT License. See the [LICENSE](LICENSE) file for details.
