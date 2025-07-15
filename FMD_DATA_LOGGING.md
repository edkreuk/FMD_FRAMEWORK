---
Title: Pipeline logging in the FMD Framework
Description: Learn how pipeline execution is logged and monitored in the Fabric Metadata-Driven (FMD) Framework.
Topic: how-to
Date: 07/2025
Author: edkreuk
---

# Pipeline logging in the FMD Framework

The Fabric Metadata-Driven (FMD) Framework provides built-in logging for pipeline execution. Each pipeline logs a record when it starts, ends, or fails. Logging data is stored in the `audit.PipelineExecution` table.

![FMD_Logging](/Images/FMD_Logging.png)

> [!NOTE]
> A semantic model for audit and logging reports is not yet available. You can use the raw data in the `audit.PipelineExecution` table to build custom monitoring and logging

