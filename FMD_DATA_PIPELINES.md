# FMD Data Pipelines

![FMD Process Overview](/Images/FMD_PROCESS_OVERVIEW.png)

**PL_FMD_LOAD_ALL**

This Pipeline will execute the Pipelines for Landingzone, Bronze and Silver

![PL_FMD_LOAD_ALL](/Images/PL_FMD_LOAD_ALL.png)

**PL_FMD_LOAD_LANDINGZONE**

This Pipeline will define which Command Pipeline will be executed based on the supported Connection Types fo rthe Data Landingzone

![PL_FMD_LOAD_LANDINGZONE](/Images/PL_FMD_LOAD_LANDINGZONE.png)


### PL_FMD_LDZ_COMMAND_ASQL

This pipeline is designed to execute the Copy Pipelines. It enhances monitoring capabilities and introduces flexibility, making it easier to extend in the future.

![PL_FMD_LDZ_COMMAND_ASQL](/Images/PL_FMD_LDZ_COMMAND_ASQL.png)

**PL_FMD_LDZ_COPY_FROM_ASQL_01**

This pipeline executes the Copy Activity by determining which objects need to be processed. For each object, it checks the Last Load Value in the source and performs either a full or incremental data copy. 

After the copy activity completes, the framework is updated with the Last Load Value, ensuring that the copy activity ran successfully. Additionally, the filename is recorded, enabling further processing in the Bronze Layer.


![Pipeline Overview PL_FMD_LDZ_COPY_FROM_ASQL_01](/Images/PL_FMD_LDZ_COPY_FROM_ASQL_01.png)

### Pipeline Configuration

Each pipeline logs a record when it starts, ends, or fails. 

All copy and lookup activities are parameterized, utilizing the `connectionGuid` parameter. This parameter references your connection, which is defined in the `integration.connections` table. Ensure that you verify the appropriate `connectionType` and `datasourceType` for your specific connection. 

In the example above, the connection type is `Sql`, and two datasource types, `ASQL_01` and `ASQL_02`, are defined. This setup allows for efficient handling of high data volumes by splitting them based on the datasource.

Additionally, this approach is easily extendable to accommodate new requirements or data sources.

