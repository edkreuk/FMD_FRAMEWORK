## Introduction to FMD Framework

Fabric Metadata-Driven Framework (FMD)

Efficient data management is a cornerstone of modern organizations, and leveraging the right tools can make all the difference. The Fabric Metadata-Driven Framework (FMD) is a cutting-edge solution designed to optimize data handling and utilization. This innovative framework harnesses the powerful capabilities of the Fabric SQL Database to build a robust, scalable, and flexible metadata-driven architecture.

![FMD Framework Deployment](/Images/FMD_FRAMEWORK.jpeg)


## Key Features:

-**Enhanced Data Governance:** 
FMD ensures comprehensive data governance by maintaining detailed metadata, enabling better data quality, consistency, and compliance.

-**Scalability and Flexibility:** 
The framework is designed to scale seamlessly with your organization's growth, adapting to evolving data needs without compromising performance.

-**Streamlined Data Integration:** 
FMD simplifies the integration of diverse data sources, providing a unified view of your data landscape.

-**Cost Efficiency:** 
By optimizing data processes and reducing redundancy, FMD helps organizations achieve significant cost savings.

Click on the link below to get started

[FMD Framework Deployment][fmdFrameworkDeployment]


**More detailed information**

[FMD Framework DataModel][fmdDataModelLink]


[FMD Framework Data Pipelines](/FMD_DATA_PIPELINES.md)

**Remarks:**

- Fabric SQL Database can fail. Mostly this will be caused by too many Fabric Databases in your tenant. Try to create the Fabric Database manually, you will directly see if this is the case. If not you can add the manual added database setting to the Deployment Notebook in cell 3.
- In a trial capacity you can't create more than 3 databases.
- In the deployment notebook you can receive the following error
  
![Fabric Experience](/Images/FMD_DATABASE_ERROR_NOTEBOOK.png)

  You can check this by creating the database manually
  
![Fabric Database Error](/Images/FMD_DATABASE_ERROR.png)

**TEST PROCESS**

Upload the file customer.csv to the file section of LH_DATA_LANDINGZONE in the Development environment
Create a table of the file called in_customer

Once the table is created you can run the complete process to check if everything was deployed and configured correctly

![Load File to table](/Images/FMD_load_file_to_table.png)

## Contributing

Contributions are welcome! If you have suggestions or improvements, feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License.


[fmdFrameworkDeployment]: /FMD_FRAMEWORK_DEPLOYMENT.md
[fmdDataModelLink]: /FMD_Datamodel.md