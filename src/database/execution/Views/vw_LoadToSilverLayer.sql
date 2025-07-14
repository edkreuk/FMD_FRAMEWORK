




    CREATE     VIEW [execution].[vw_LoadToSilverLayer]
    AS
   SELECT 
    LZE.[LandingzoneEntityId],
    BLE.[BronzeLayerEntityId],
    SLE.[SilverLayerEntityId] AS [EntityId],
    SLE.[Schema] AS [SourceSchema],
    SLE.[Name] AS [SourceName],
    BLE.[FileType] AS [SourceFileType],
    SLE.[Schema] AS [TargetSchema],
    SLE.[Name] AS [TargetName],
    SLE.[FileType] AS [TargetFileType],
    WT.[WorkspaceGuid] AS [TargetWorkspaceId],
    WS.[WorkspaceGuid] AS [SourceWorkspaceId],
    SLH.[LakehouseGuid] AS [TargetLakehouseId],
    BLH.[LakehouseGuid] AS [SourceLakehouseId],
    SLH.[Name] AS [TargetLakehouseName],
    BLH.[Name] AS [SourceLakehouseName],
    SLE.[CleansingRules],
    DS.[Namespace] AS [DataSourceNamespace]
FROM [integration].[SilverLayerEntity] SLE
INNER JOIN [integration].[BronzeLayerEntity] BLE
    ON SLE.BronzeLayerEntityId = BLE.BronzeLayerEntityId
INNER JOIN [integration].[LandingzoneEntity] LZE
    ON LZE.LandingzoneEntityId = BLE.LandingzoneEntityId
INNER JOIN [execution].[LandingzoneEntityLastLoadValue] LZELV
    ON LZE.LandingzoneEntityId = LZELV.LandingzoneEntityId
INNER JOIN [integration].[DataSource] DS
    ON DS.[DataSourceId] = LZE.[DataSourceId]
INNER JOIN [integration].[Lakehouse] BLH
    ON BLE.LakehouseId = BLH.LakehouseId
INNER JOIN [integration].[Lakehouse] SLH
    ON SLE.LakehouseId = SLH.LakehouseId
INNER JOIN [integration].[Workspace] WT
    ON WT.[WorkspaceGuid] = SLH.[WorkspaceGuid]
INNER JOIN [integration].[Workspace] WS
    ON WS.[WorkspaceGuid] = BLH.[WorkspaceGuid]
WHERE 1 = 1
    AND LZE.IsActive = 1
    AND BLE.IsActive = 1
    AND SLE.IsActive = 1

GO

