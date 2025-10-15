---
Title: FMD Data Pipelines overview
Description: Learn about the architecture, key pipelines, and configuration of the Fabric Metadata-Driven (FMD) Framework data pipelines.
Topic: overview
Date: 07/2025
Author: edkreuk
---

# FMD Data Pipelines overview

The Fabric Metadata-Driven (FMD) Framework provides a robust data pipeline architecture for orchestrating data movement and transformation across multiple layers, including Landingzone, Bronze, and Silver. This article describes the core pipelines, their roles, and configuration guidance.

## Architecture overview

The FMD Data Pipelines framework automates data ingestion, transformation, and movement between layers. The following diagram illustrates the end-to-end process flow:

![FMD Process Overview](/Images/FMD_PROCESS_OVERVIEW.png)

## Key pipelines

### PL_FMD_LOAD_ALL

Coordinates the execution of the Landingzone, Bronze, and Silver pipelines. Use this pipeline to initiate a complete data load across all layers.

**Parameters:**
- `key_vault_name`: Reserved for future use.
- `Lakehouse_schema_enabled`: Set to `True` if deployed with schema support; otherwise, set to `False`.

![PL_FMD_LOAD_ALL](/Images/PL_FMD_LOAD_ALL.png)

### PL_FMD_LOAD_LANDINGZONE

Determines which command pipeline to execute based on the supported connection types for the data Landingzone. Enables flexible integration with various data sources.

![PL_FMD_LOAD_LANDINGZONE](/Images/PL_FMD_LOAD_LANDINGZONE.png)

### PL_FMD_LDZ_COMMAND_ASQL

Executes copy pipelines for data ingestion from Azure SQL sources. Enhances monitoring and introduces flexibility for future extensibility.

![PL_FMD_LDZ_COMMAND_ASQL](/Images/PL_FMD_LDZ_COMMAND_ASQL.png)

### PL_FMD_LDZ_COPY_FROM_ASQL_01

Manages the copy activity for data ingestion. Determines which objects require processing, checks the last load value in the source, and performs either a full or incremental data copy. After the copy activity completes, the framework updates the last load value and records the filename. This ensures successful execution and enables further processing in the Bronze layer.

![Pipeline Overview PL_FMD_LDZ_COPY_FROM_ASQL_01](/Images/PL_FMD_LDZ_COPY_FROM_ASQL_01.png)

### PL_FMD_LDZ_COPY_FROM_ADF

Manages the copy activity for data ingestion from Azure Data Factory pipelines. Ensure you build ADF pipelines that write data back to OneLake. All parameters in this pipeline are passed to the ADF pipelines. After the copy activity completes, the framework updates the last load value and records the filename, enabling further processing in the Bronze layer.

![Pipeline Overview PL_FMD_LDZ_COPY_FROM_ADF](/Images/PL_FMD_LDZ_COPY_FROM_ADF.png)

### PL_FMD_LOAD_BRONZE

Manages the execution of `NB_FMD_LOAD_LANDING_BRONZE` through `NB_FMD_PROCESSING_PARALLEL_MAIN`. This notebook receives all entities to be processed, handles parallel processing, and manages retries. The default timeout is set to 7200 seconds, with 2 retries.

![Pipeline Overview PL_FMD_LOAD_BRONZE ](/Images/PL_FMD_LOAD_BRONZE.png)

### PL_FMD_LOAD_SILVER

Manages the execution of `NB_FMD_LOAD_BRONZE_SILVER` through `NB_FMD_PROCESSING_PARALLEL_MAIN`. The process is identical to `PL_FMD_LOAD_BRONZE` but loads different metadata for the Silver layer.

![Pipeline Overview PL_FMD_LOAD_SILVER ](/Images/PL_FMD_LOAD_SILVER.png)

## Pipeline configuration

Each pipeline logs a record at the start, end, and upon failure. All copy and lookup activities are parameterized using the `connectionGuid` parameter, which references your connection as defined in the `integration.connections` table.

> **Note:**  
> Verify the appropriate `connectionType` and `datasourceType` for your specific connection. For example, the connection type may be `Sql`, with datasource types such as `ASQL_01` and `ASQL_02`. This configuration supports efficient handling of high data volumes by splitting them based on the datasource.

The FMD Framework is designed for extensibility, allowing you to accommodate new requirements or data sources as needed.

