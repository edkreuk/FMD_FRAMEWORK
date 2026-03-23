/*
=============================================================================
001_add_dqrules_columns.sql
=============================================================================
Adds the DQRules column to integration.BronzeLayerEntity and
integration.SilverLayerEntity to store metadata-driven Data Quality rule
definitions (JSON array).

These columns mirror the existing CleansingRules pattern and are consumed by:
  - execution.sp_GetBronzeDQRule  (NB_FMD_LOAD_LANDING_BRONZE)
  - execution.sp_GetSilverDQRule  (NB_FMD_LOAD_BRONZE_SILVER)

Run this script once against the FMD Framework SQL database.
It is idempotent: it skips the ALTER if the column already exists.
=============================================================================
*/

-- -------------------------------------------------------------------------
-- integration.BronzeLayerEntity  →  add DQRules column
-- -------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[integration].[BronzeLayerEntity]')
      AND name = N'DQRules'
)
BEGIN
    ALTER TABLE [integration].[BronzeLayerEntity]
        ADD [DQRules] NVARCHAR(MAX) NULL;
    PRINT 'Column DQRules added to integration.BronzeLayerEntity.';
END
ELSE
BEGIN
    PRINT 'Column DQRules already exists on integration.BronzeLayerEntity — skipping.';
END
GO

-- -------------------------------------------------------------------------
-- integration.SilverLayerEntity  →  add DQRules column
-- -------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[integration].[SilverLayerEntity]')
      AND name = N'DQRules'
)
BEGIN
    ALTER TABLE [integration].[SilverLayerEntity]
        ADD [DQRules] NVARCHAR(MAX) NULL;
    PRINT 'Column DQRules added to integration.SilverLayerEntity.';
END
ELSE
BEGIN
    PRINT 'Column DQRules already exists on integration.SilverLayerEntity — skipping.';
END
GO
