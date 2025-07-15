---
title: FMD Data Pipelines overview
description: Learn about the architecture, key pipelines, and configuration of the Fabric Metadata-Driven (FMD) Framework data pipelines.
ms.service: fabric
ms.topic: overview
ms.date: 07/2025
author: edkreuk
---

# FMD Data Pipelines overview

The Fabric Metadata-Driven (FMD) Framework provides a robust data pipeline architecture for orchestrating data movement and transformation across multiple layers, including Landingzone, Bronze, and Silver. This article describes the core pipelines, their roles, and configuration guidance.

## Architecture overview

The FMD Data Pipelines framework automates data ingestion, transformation, and movement between layers. The following diagram illustrates the end-to-end process flow:

![FMD Process Overview](/Images/FMD_PROCESS_OVERVIEW.png)

## Key pipelines

### PL_FMD_LOAD_ALL

Coordinates the execution of the Landingzone, Bronze, and Silver pipelines. Use this pipeline to initiate a complete data load across all layers.

Parameters:
key_vault_name: No Function yet, for later purposes
Lakehouse_schema_enabled: If Deployed with Lakehouse_schema_enabled = True, then leave it like this other wise change to False

![PL_FMD_LOAD_ALL](/Images/PL_FMD_LOAD_ALL.png)

### PL_FMD_LOAD_LANDINGZONE

Determines which command pipeline to execute based on the supported connection types for the data Landingzone. Enables flexible integration with various data sources.

![PL_FMD_LOAD_LANDINGZONE](/Images/PL_FMD_LOAD_LANDINGZONE.png)

### PL_FMD_LDZ_COMMAND_ASQL

Executes copy pipelines for data ingestion from Azure SQL sources. Enhances monitoring and introduces flexibility for future extensibility.

![PL_FMD_LDZ_COMMAND_ASQL](/Images/PL_FMD_LDZ_COMMAND_ASQL.png)

### PL_FMD_LDZ_COPY_FROM_ASQL_01

Manages the copy activity for data ingestion. Determines which objects require processing, checks the last load value in the source, and performs either a full or incremental data copy.

After the copy activity completes, the framework updates the last load value and records the filename. This ensures successful execution and enables further processing in the Bronze layer.

![Pipeline Overview PL_FMD_LDZ_COPY_FROM_ASQL_01](/Images/PL_FMD_LDZ_COPY_FROM_ASQL_01.png)

## Pipeline configuration

Each pipeline logs a record at the start, end, and upon failure. All copy and lookup activities are parameterized using the `connectionGuid` parameter, which references your connection as defined in the `integration.connections` table.

> [!NOTE]
> Verify the appropriate `connectionType` and `datasourceType` for your specific connection. For example, the connection type may be `Sql`, with datasource types such as `ASQL_01` and `ASQL_02`. This configuration supports efficient handling of high data volumes by splitting them based on the datasource.

The FMD Framework is designed for extensibility, allowing you to accommodate new requirements or data sources as needed.

