

    CREATE VIEW [execution].[vw_LoadSourceToLandingzone]
    AS
    SELECT 
    LZE.[LandingzoneEntityId] AS [EntityId],
    DS.[DataSourceId] AS [DataSourceId],
    DS.[Name] AS [DataSourceName],
    DS.[Namespace] AS [DataSourceNamespace],
    DS.[Type] AS [DataSourceType],
    C.[Type] AS [ConnectionType],
    C.[ConnectionGuid] AS [ConnectionGuid],
    [SourceSchema],
    [SourceName],
    [FilePath] + '/' + DS.[Namespace] + FORMAT(GETUTCDATE(), '/yyyy/MM/dd') AS [TargetFilePath],
    LZE.[FileName] + '_' + FORMAT(GETUTCDATE(), 'yyyyMMddHHmm') + '.' + [FileType] AS [TargetFileName],
    LZE.[FileType] AS [TargetFileType],
    LH.[LakehouseGuid] AS [TargetLakehouseGuid],
    W.[WorkspaceGuid] AS [WorkspaceGuid],
    [IsIncremental],
    [IsIncrementalColumn],
    [LastLoadValue] = CASE
        WHEN C.[Type] IN ('SQL') THEN
            'SELECT CASE WHEN ' + CONVERT(NVARCHAR(1), CASE WHEN LZE.[IsIncremental] = '' OR ISNULL(LZE.[IsIncrementalColumn], '') = '' THEN 0 ELSE LZE.[IsIncremental] END) + ' = 1 
            THEN CONVERT(VARCHAR, MAX(' + (
                CASE WHEN ISNULL(LZE.[IsIncrementalColumn], '') = '' THEN '1'
                ELSE LZE.[IsIncrementalColumn]
            END) + '), 120) 
            ELSE CONVERT(VARCHAR, GETDATE(), 120) 
            END AS [LastLoadValue] 
            FROM ' + QUOTENAME(ISNULL(
                CASE WHEN LZE.[SourceSchema] != '' THEN LZE.[SourceSchema]
                END, 'Unknown')) + '.' + QUOTENAME(LZE.[SourceName])
        ELSE 
            LZELV.[LoadValue]
    END,
    [SourceDataRetrieval] = CASE 
        WHEN LZE.[IsIncremental] = 1 THEN 
            'SELECT * FROM ' + QUOTENAME(ISNULL(CASE WHEN LZE.[SourceSchema] != '' THEN LZE.[SourceSchema] END, 'Unknown')) + '.' + QUOTENAME(LZE.[SourceName]) + 
            CASE WHEN ISNULL(LZE.[IsIncrementalColumn], '') <> '' AND TRY_CONVERT(VARCHAR, [LoadValue]) IS NOT NULL THEN 
                ' WHERE ' + LZE.[IsIncrementalColumn] + ' > ''' + TRY_CONVERT(VARCHAR, ISNULL([LoadValue], '1900-01-01')) + '''' 
            ELSE ''
            END
        WHEN ISNULL(LZE.[IsIncremental], 0) = 0 THEN 
            'SELECT * FROM ' + QUOTENAME(ISNULL(CASE WHEN LZE.[SourceSchema] != '' THEN LZE.[SourceSchema] END, 'Unknown')) + '.' + QUOTENAME(LZE.[SourceName]) 
        ELSE ''
    END
FROM [integration].[LandingzoneEntity] LZE
INNER JOIN [integration].[Lakehouse] LH
    ON LZE.[LakehouseId] = LH.[LakehouseId]
INNER JOIN [integration].[Workspace] W
    ON W.[WorkspaceGuid] = LH.[WorkspaceGuid]
INNER JOIN [integration].[DataSource] DS
    ON DS.[DataSourceId] = LZE.[DataSourceId]
INNER JOIN [integration].[Connection] C
    ON DS.[ConnectionId] = C.[ConnectionId]
LEFT JOIN [execution].[LandingzoneEntityLastLoadValue] LZELV
    ON LZELV.[LandingzoneEntityId] = LZE.[LandingzoneEntityId]
WHERE 1 = 1
    AND LZE.[IsActive] = 1

GO

