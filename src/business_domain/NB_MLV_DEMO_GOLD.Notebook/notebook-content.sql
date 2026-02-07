-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "b6e09e2a-99d3-4a4a-9d6e-8d978cd44956",
-- META       "default_lakehouse_name": "LH_GOLD_LAYER",
-- META       "default_lakehouse_workspace_id": "c90c9850-99fe-4050-899c-417cc30d7f70",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "b6e09e2a-99d3-4a4a-9d6e-8d978cd44956"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # CREATE or REPLACE materialized lake views 
-- 1. Use this notebook to CREATE or REPLACE materialized lake views. 
-- 2. Select **Run all** to run the notebook. 
-- 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 
-- 
-- **Make sure you connect the correct Lakehouse from on your Data Workspace as default lakehouse**


-- CELL ********************

CREATE SCHEMA IF NOT EXISTS gold

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE or REPLACE MATERIALIZED LAKE VIEW gold.FactOrderLines 
AS 
SELECT      SOL.OrderLineID,
			SOL.OrderID,
            SO.CustomerID,
			SOL.StockItemID,
			SOL.Description,
			SOL.PackageTypeID,
			SOL.Quantity,
			SOL.UnitPrice,
			SOL.TaxRate,
			SO.OrderDate,
			SO.ExpectedDeliveryDate,
			SOL.PickedQuantity,
			SOL.PickingCompletedWhen
FROM LH_GOLD_LAYER.dbo.Sales_OrderLines SOL inner join LH_GOLD_LAYER.dbo.Sales_Orders SO 
on SOL.OrderID=SO.OrderID
where SOL.IsCurrent=1 and SO.IsCurrent=1



-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE or REPLACE MATERIALIZED LAKE VIEW gold.DimOrders 
AS 
SELECT     OrderID,
			
			CustomerPurchaseOrderNumber,
			IsUndersupplyBackordered,
			Comments,
			DeliveryInstructions,
			InternalComments
FROM LH_GOLD_LAYER.dbo.Sales_Orders
where IsCurrent=1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE or REPLACE MATERIALIZED LAKE VIEW gold.DimCustomer 
AS 
SELECT CustomerID
      ,CustomerName
      ,BG.BuyingGroupName
      ,CreditLimit
      ,AccountOpenedDate
      ,StandardDiscountPercentage
      ,IsStatementSent
      ,IsOnCreditHold
      ,PaymentDays
      ,PhoneNumber
      ,FaxNumber
      ,DeliveryRun
      ,RunPosition
      ,WebsiteURL

  FROM LH_GOLD_LAYER.dbo.Sales_vCustomers C inner join LH_GOLD_LAYER.dbo.Sales_BuyingGroups BG  on C.BuyingGroupID=BG.BuyingGroupID
where C.IsCurrent=1 and BG.IsCurrent=1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE or REPLACE MATERIALIZED LAKE VIEW gold.DimPackageType 
AS 
SELECT PackageTypeID
      ,PackageTypeName

  FROM LH_GOLD_LAYER.dbo.Warehouse_PackageTypes 
where IsCurrent=1 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE or REPLACE MATERIALIZED LAKE VIEW gold.DimStockItems 
AS 
SELECT StockItemID
      ,StockItemName
      ,Brand
      ,Size
      ,LeadTimeDays
      ,QuantityPerOuter
      ,IsChillerStock
      ,Barcode
      ,TaxRate
      ,UnitPrice
      ,RecommendedRetailPrice
      ,TypicalWeightPerUnit
      ,MarketingComments
      ,InternalComments
      ,Photo
      ,CustomFields
      ,Tags
      ,SearchDetails
  FROM LH_GOLD_LAYER.dbo.Warehouse_StockItems
where IsCurrent=1 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
