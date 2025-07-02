



    CREATE     VIEW [execution].[vw_LoadToBronzeLayer]
    AS
   SELECT 
    BLE.[BronzeLayerEntityId] AS [EntityId], 
    PLZE.[FilePath] AS SourceFilePath,
    PLZE.[FileName] AS SourceFileName,
    LZE.[LandingzoneEntityId],
    LZE.[FileType] AS SourceFileType,
    BLE.[Schema] AS [TargetSchema],
    BLE.[Name] AS [TargetName],
    WT.[WorkspaceGuid] AS [TargetWorkspaceId],
    WS.[WorkspaceGuid] AS [SourceWorkspaceId],
    BLH.[LakehouseGuid] AS [TargetLakehouseId],
    LZH.[LakehouseGuid] AS [SourceLakehouseId],
    BLH.[Name] AS [TargetLakehouseName],
    LZH.[Name] AS [SourceLakehouseName],
    LZE.[IsIncremental],
    BLE.[PrimaryKeys],
    BLE.[CleansingRules],
    DS.[Namespace] AS [DataSourceNamespace]
FROM [integration].[BronzeLayerEntity] BLE
INNER JOIN [integration].[LandingzoneEntity] LZE
    ON LZE.[LandingzoneEntityId] = BLE.[LandingzoneEntityId]
INNER JOIN [execution].[PipelineLandingzoneEntity] PLZE
    ON LZE.[LandingzoneEntityId] = PLZE.[LandingzoneEntityId]
INNER JOIN [execution].[LandingzoneEntityLastLoadValue] LZELV
    ON LZE.[LandingzoneEntityId] = LZELV.[LandingzoneEntityId]
INNER JOIN [integration].[DataSource] DS
    ON DS.[DataSourceId] = LZE.[DataSourceId]
INNER JOIN [integration].[Lakehouse] BLH
    ON BLE.[LakehouseId] = BLH.[LakehouseId]
INNER JOIN [integration].[Lakehouse] LZH
    ON LZE.[LakehouseId] = LZH.[LakehouseId]
INNER JOIN [integration].[Workspace] WT
    ON WT.[WorkspaceGuid] = BLH.[WorkspaceGuid]
INNER JOIN [integration].[Workspace] WS
    ON WS.[WorkspaceGuid] = LZH.[WorkspaceGuid]
WHERE 1 = 1
    AND LZE.[IsActive] = 1
    AND BLE.[IsActive] = 1
    AND PLZE.[IsProcessed] = 0

GO

