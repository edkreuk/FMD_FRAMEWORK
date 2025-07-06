# FMD Data Pipelines

This article provides an overview of the FMD Data Pipelines architecture and describes the key pipelines and their configurations.

## Overview

The FMD Data Pipelines framework orchestrates data movement and transformation across multiple layers, including Landingzone, Bronze, and Silver. The following diagram illustrates the overall process flow:

![FMD Process Overview](/Images/FMD_PROCESS_OVERVIEW.png)

## Pipeline Descriptions

### PL_FMD_LOAD_ALL

The `PL_FMD_LOAD_ALL` pipeline coordinates the execution of the Landingzone, Bronze, and Silver pipelines. Use this pipeline to initiate a complete data load across all layers.

![PL_FMD_LOAD_ALL](/Images/PL_FMD_LOAD_ALL.png)

### PL_FMD_LOAD_LANDINGZONE

The `PL_FMD_LOAD_LANDINGZONE` pipeline determines which command pipeline to execute based on the supported connection types for the data Landingzone. This enables flexible integration with various data sources.

![PL_FMD_LOAD_LANDINGZONE](/Images/PL_FMD_LOAD_LANDINGZONE.png)

### PL_FMD_LDZ_COMMAND_ASQL

The `PL_FMD_LDZ_COMMAND_ASQL` pipeline is designed to execute copy pipelines. It enhances monitoring and introduces flexibility, making it easier to extend the framework for future requirements.

![PL_FMD_LDZ_COMMAND_ASQL](/Images/PL_FMD_LDZ_COMMAND_ASQL.png)

### PL_FMD_LDZ_COPY_FROM_ASQL_01

The `PL_FMD_LDZ_COPY_FROM_ASQL_01` pipeline manages the copy activity for data ingestion. It determines which objects require processing, checks the last load value in the source, and performs either a full or incremental data copy.

After the copy activity completes, the framework updates the last load value and records the filename. This ensures successful execution and enables further processing in the Bronze layer.

![Pipeline Overview PL_FMD_LDZ_COPY_FROM_ASQL_01](/Images/PL_FMD_LDZ_COPY_FROM_ASQL_01.png)

## Pipeline Configuration

Each pipeline logs a record at the start, end, and upon failure. All copy and lookup activities are parameterized using the `connectionGuid` parameter, which references your connection as defined in the `integration.connections` table.

> [!NOTE]
> Verify the appropriate `connectionType` and `datasourceType` for your specific connection. In the example above, the connection type is `Sql`, and two datasource types, `ASQL_01` and `ASQL_02`, are defined. This configuration supports efficient handling of high data volumes by splitting them based on the datasource.

The framework is designed for extensibility, allowing you to accommodate new requirements or data sources as needed.

