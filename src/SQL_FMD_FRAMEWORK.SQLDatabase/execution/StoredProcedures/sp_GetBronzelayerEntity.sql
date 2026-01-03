CREATE PROCEDURE [execution].[sp_GetBronzelayerEntity] 
	WITH EXECUTE AS CALLER
AS
BEGIN
	SET NOCOUNT ON;	
    
	SELECT CONCAT('[', 
        STRING_AGG(CONCAT(CONVERT(NVARCHAR(MAX),'{"path": "NB_FMD_LOAD_LANDING_BRONZE", "params":{"SourceFilePath": ')  , '"' , REPLACE(REPLACE([SourceFilePath],'\\', '\\'), '"', '\"') , '"' ,
        ',"SourceFileName" : ', '"', REPLACE(REPLACE([SourceFileName], '\\', '\\'), '"', '\"'), '"',
        ',"TargetSchema"   : ', '"', REPLACE(REPLACE([TargetSchema], '\\', '\\'), '"', '\"'), '"',
        ',"TargetName"     : ', '"', REPLACE(REPLACE([TargetName], '\\', '\\'), '"', '\"'), '"',
        ',"PrimaryKeys"    : ', '"', REPLACE(REPLACE([PrimaryKeys], '\\', '\\'), '"', '\"'), '"',
        ',"SourceFileType" : ', '"', REPLACE(REPLACE([SourceFileType], '\\', '\\'), '"', '\"'), '"',
        ',"IsIncremental"  : ', '"', CASE WHEN [IsIncremental] = 1 THEN 'True' ELSE 'False' END, '"',
        ',"TargetLakehouse" : ', '"', LOWER(CONVERT(NVARCHAR(36), [TargetLakehouseId])), '"',
        ',"SourceLakehouse" : ', '"', LOWER(CONVERT(NVARCHAR(36), [SourceLakehouseId])), '"',
        ',"TargetWorkspace" : ', '"', LOWER(CONVERT(NVARCHAR(36), [TargetWorkspaceId])), '"',
        ',"SourceWorkspace" : ', '"', LOWER(CONVERT(NVARCHAR(36), [SourceWorkspaceId])), '"',
        ',"TargetLakehouseName" : ', '"', REPLACE(REPLACE([TargetLakehouseName], '\\', '\\'), '"', '\"'), '"',
        ',"SourceLakehouseName" : ', '"', REPLACE(REPLACE([SourceLakehouseName], '\\', '\\'), '"', '\"'), '"',
        ',"LandingzoneEntityId" : ', '"', LOWER(CONVERT(NVARCHAR(36), [LandingzoneEntityId])), '"',
        ',"BronzeLayerEntityId" : ', '"', LOWER(CONVERT(NVARCHAR(36), [EntityId])), '"',
		',"DataSourceNamespace" : ' , '"' ,  LOWER(convert(NVARCHAR(20), [DataSourceNamespace])) , '"' ,
        ',"cleansing_rules" : ', '"', REPLACE(REPLACE([CleansingRules], '\\', '\\'), '"', '\"'), '"', 
        '}}'),', ') WITHIN GROUP (ORDER BY [EntityId])
        ,']') AS NotebookParams
         FROM (SELECT TOP 100 PERCENT * FROM [execution].[vw_LoadToBronzeLayer]  ORDER BY [SourceFileName] ASC) AS [vw_LoadToBronzeLayer]
               
END

GO

