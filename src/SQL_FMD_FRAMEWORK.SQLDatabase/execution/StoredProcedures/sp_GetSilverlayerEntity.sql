
CREATE PROCEDURE [execution].[sp_GetSilverlayerEntity] 
	WITH EXECUTE AS CALLER
AS
BEGIN
	SET NOCOUNT ON;	
    
SELECT CONCAT('[',
    STRING_AGG(
        CONCAT(CONVERT(NVARCHAR(MAX),'{"path": "NB_FMD_LOAD_BRONZE_SILVER", "params": {
        "SourceSchema": ') , '"' , REPLACE(REPLACE(SourceSchema,'\\', '\\'), '"', '\"') , '"' ,
        ',"SourceName": ' , '"' , REPLACE(REPLACE(SourceName,'\\', '\\'), '"', '\"') , '"' ,
        ',"TargetSchema": ' , '"' , REPLACE(REPLACE(TargetSchema,'\\', '\\'), '"', '\"') , '"' ,
        ',"TargetName": ' , '"' , REPLACE(REPLACE(TargetName,'\\', '\\'), '"', '\"') , '"' ,
        ',"SourceFileType": ' , '"' , REPLACE(REPLACE(SourceFileType,'\\', '\\'), '"', '\"') , '"' ,
        ',"TargetLakehouse": ' , '"' , LOWER(CONVERT(NVARCHAR(36), TargetLakehouseId)) , '"' ,
        ',"SourceLakehouse": ' , '"' , LOWER(CONVERT(NVARCHAR(36), SourceLakehouseId)) , '"' ,
        ',"TargetWorkspace": ' , '"' , LOWER(CONVERT(NVARCHAR(36), TargetWorkspaceId)) , '"' ,
        ',"SourceWorkspace": ' , '"' , LOWER(CONVERT(NVARCHAR(36), SourceWorkspaceId)) , '"' ,
        ',"TargetLakehouseName": ' , '"' , REPLACE(REPLACE(TargetLakehouseName,'\\', '\\'), '"', '\"') , '"' ,
        ',"SourceLakehouseName": ' , '"' , REPLACE(REPLACE(SourceLakehouseName,'\\', '\\'), '"', '\"') , '"' ,
        ',"BronzeLayerEntityId" : ', '"', LOWER(CONVERT(NVARCHAR(36), [BronzeLayerEntityId])), '"',
        ',"SilverLayerEntityId": ' , '"' ,  LOWER(CONVERT(NVARCHAR(36),EntityId)) , '"' ,
        ',"DataSourceNamespace" : ' , '"' ,  LOWER(convert(NVARCHAR(30), [DataSourceNamespace])) , '"' ,
         ',"cleansing_rules" : ', '"', REPLACE(REPLACE([CleansingRules],'\\', '\\'), '"', '\"'), '"',
              '}}' )
    , ',') WITHIN GROUP (ORDER BY EntityId)
    ,']') AS NotebookParams
FROM [execution].[vw_LoadToSilverLayer]
             
END

GO

