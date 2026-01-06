---
Title: Fabric Metadata-Driven Framework (FMD) Overview
Description: Discover the architecture, components, and deployment guidance for the Fabric Metadata-Driven Framework (FMD) on Fabric SQL Database.
Date: 07/2025
Author: edkreuk
---

# Fabric Metadata-Driven Framework (FMD)  overview

The Fabric Metadata-Driven Framework (FMD) is a scalable, extensible solution for managing, integrating, and governing data using a metadata-driven approach on Fabric SQL Database. This article provides an overview of the FMD Framework, including its architecture, core components, workspace structure, supported data sources, and deployment guidance.

## Overview

The FMD Framework enables organizations to streamline data operations by leveraging metadata to drive dynamic data pipelines and parameterized notebooks. Built on Fabric SQL Database, the framework supports secure, flexible, and modern data management at scale.

> [!TIP]
> The FMD Framework is designed for rapid deployment and extensibility. You can use it out-of-the-box or customize it to meet your organization's evolving data needs.


## Video with the Data Factory Team

[![Watch the FMD Framework overview](https://img.youtube.com/vi/UzqSFajSvtY/0.jpg)](https://www.youtube.com/watch?v=UzqSFajSvtY&t=829s)
## Key features

- **Comprehensive data governance:** Maintain detailed metadata for improved data quality, consistency, and compliance.
- **Scalability and flexibility:** Seamlessly scale with organizational growth and adapt to changing data requirements.
- **Streamlined data integration:** Integrate diverse data sources for a unified data landscape.
- **Cost efficiency:** Optimize data processes and reduce redundancy to achieve cost savings.

## Architecture and components

The FMD Framework uses a modular architecture that separates data, code, and orchestration for enhanced security and manageability.

- **[Workspace architecture](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Workspace-architecture)**.
- **[Medallion architecture](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Medallion-architecture)**.
- **[Supported data sources](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Supported-data-sources)**.
- **[Data Cleansing](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Cleansing)**.
- **[Logging and auditing](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Logging-and-auditing)**.  
- **[Variable Library](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Variable-Library)**.
- **[Data Pipelines and Notebooks](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Pipelines-and-Notebooks)**.
- **[Taskflow orchestration](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Taskflow)**.
- **[Business Domains](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Business-Domains)**.

## Deployment and getting started

To get started:

1. Review the **[FMD Framework Deployment Guide](./FMD_FRAMEWORK_DEPLOYMENT.md)**.
2. Set up the required connections in your Fabric environment.
3. Configure the deployment parameters as per your environment.
4. Deploy the FMD Framework using the provided deployment scripts.
5. Import the taskflow and configure your workspaces as recommended.
6. Refer to **[wiki](https://github.com/edkreuk/FMD_FRAMEWORK/wiki)** for data model, pipelines, and logging.


## Additional resources

| Resource | Description |
|----------|-------------|
| **[FMD Integration Framework reference](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Integration)** |Overview on how to add sources and demo data to the FMD Framework |
| **[FMD Data Model reference](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Model)** | Overview of the data model used in the FMD Framework |
| **[Configure and load demo data](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Integration#configure-and-load-bulk-data-into-the-fmd-framework)** | Instructions for loading demo data into the FMD Framework |

## Troubleshooting
[Troubleshooting](https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Troubleshooting)


## Contributing

We welcome contributions! To suggest improvements, open an issue or submit a pull request.  
If opening a pull request, please follow these steps:

1. Fork the repository.
2. Create a feature branch.
3. Commit your changes.
4. Push to the feature branch.
5. Create a pull request and add documentation on what you have changed.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Contributors:**  
[Erwin de Kreuk](https://www.linkedin.com/in/erwindekreuk/)  
