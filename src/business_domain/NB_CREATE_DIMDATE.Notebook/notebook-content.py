# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "63ddad67-a2c0-424b-bae6-d93db2fc592a",
# META       "default_lakehouse_name": "LH_GOLD_LAYER",
# META       "default_lakehouse_workspace_id": "89c45add-7ca9-4fdf-be0f-8e5665294031",
# META       "known_lakehouses": [
# META         {
# META           "id": "63ddad67-a2c0-424b-bae6-d93db2fc592a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook to create shortcuts in Gold
# 
# Don't forget to change the parameters
# 
# 
# More details: https://github.com/edkreuk/FMD_FRAMEWORK


# CELL ********************

import json, requests
import notebookutils 
import sempy.fabric as fabric           # provides workspace/item context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Shortcut location parameters

ShortcutNames=['Purchasing_PurchaseOrderLines','Purchasing_PurchaseOrders']         #Tablenames of the to be created shortcuts, same name will be used for destination

ShortcutSettings=notebookutils.variableLibrary.getLibrary("VAR_GOLD_FMD")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



def create_shortcut(path:str, name:str, target:dict, conflict_policy:str="Abort"):

    # Get Local Lakehouse details
    workspace_id = fabric.get_workspace_id()
    lakehouse_id = fabric.get_lakehouse_id()

    # Get Token
    token = notebookutils.credentials.getToken("pbi")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    body = {
        "path": path,    # e.g., "Files/raw" OR "Tables"
        "name": name,    # e.g., "sales_external"
        "target": target # see examples below
    }

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
    params = {"shortcutConflictPolicy": conflict_policy}

    resp = requests.post(url, headers=headers, params=params, data=json.dumps(body))
    if resp.status_code in (200, 201):
        print(f"Shortcut '{name}' created at '{path}'.")
        print("Location:", resp.headers.get("Location", "<none>"))
    else:
        print("Failed:", resp.status_code, resp.text)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for ShortcutName in ShortcutNames:
    oneLake_target = {    "oneLake": {        "workspaceId": ShortcutSettings.Shortcut_TargetWorkspaceId,        "itemId":      ShortCutSettings.Shortcut_TargetLakehouseId,"path": "Tables/"+ShortcutSettings.Shortcut_TargetSchema+"/"+ShortcutName   }}
    create_shortcut(path="Tables/"+ShortcutSettings.Shortcut_SourceSchema, name=ShortcutName, target=oneLake_target, conflict_policy="CreateOrOverwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T

def dim_date_df(spark, start_date: str, end_date: str, holidays=None):
    """
    Build a DimDate dataframe for the [start_date, end_date] range (inclusive).

    Parameters
    ----------
    spark : SparkSession
    start_date : str (YYYY-MM-DD)
    end_date   : str (YYYY-MM-DD)
    holidays   : Optional[List[str]]  list of 'YYYY-MM-DD' holiday dates to flag (per country/BU)

    Returns
    -------
    pyspark.sql.DataFrame : DimDate with common BI attributes
    """

    # 1) Base date series
    df = (
        spark.range(1)  # single row
        .select(F.sequence(F.to_date(F.lit(start_date)), F.to_date(F.lit(end_date)), F.expr("interval 1 day")).alias("darr"))
        .select(F.explode("darr").alias("FullDate"))
    )

    # 2) ISO day-of-week (1=Mon..7=Sun) using 'u' pattern
    #    Spark: date_format(x,'u') returns 1..7 with Monday=1
    df = df.withColumn("DayNumberOfWeek", F.date_format("FullDate", "u").cast("int"))

    # 3) Day/Month/Year basics
    df = (
        df
        .withColumn("DayName", F.date_format("FullDate", "EEEE"))
        .withColumn("DayNumberOfMonth", F.dayofmonth("FullDate"))
        .withColumn("DayNumberOfYear", F.dayofyear("FullDate"))
        .withColumn("MonthNumber", F.month("FullDate"))
        .withColumn("MonthName", F.date_format("FullDate", "MMMM"))
        .withColumn("YearNumber", F.year("FullDate"))
    )

    # 4) Week calculations (ISO-like)
    # weekofyear() in Spark aligns with ISO-8601 semantics for most locales (Mon start; week 1 has >= 4 days)
    df = df.withColumn("WeekNumberOfYear", F.weekofyear("FullDate"))

    # WeekStart = Monday of current week; WeekEnd = Sunday
    df = (
        df
        .withColumn("WeekStartDate", F.date_sub(F.col("FullDate"), F.col("DayNumberOfWeek") - F.lit(1)))
        .withColumn("WeekEndDate",   F.date_add(F.col("WeekStartDate"), F.lit(6)))
    )

    # 5) Month start/end
    df = (
        df
        .withColumn("MonthStartDate", F.trunc("FullDate", "month"))
        .withColumn("MonthEndDate",   F.last_day("FullDate"))
    )

    # 6) Quarter attributes
    df = (
        df
        .withColumn("QuarterNumber", F.quarter("FullDate"))
        .withColumn("QuarterName", F.concat(F.lit("Q"), F.quarter("FullDate")))
    )

    # QuarterStart = first day of quarter; QuarterEnd = last day of quarter
    qstart = F.to_date(
        F.concat_ws("-",
            F.year("FullDate"),
            ( (F.quarter("FullDate") - F.lit(1)) * F.lit(3) + F.lit(1) ).cast("int"),
            F.lit(1)
        ),
        "yyyy-M-d"
    )
    df = (
        df
        .withColumn("QuarterStartDate", qstart)
        .withColumn("QuarterEndDate", F.last_day(F.add_months(F.col("QuarterStartDate"), F.lit(2))))
    )

    # 7) Year start/end
    df = (
        df
        .withColumn("YearStartDate", F.to_date(F.concat_ws("-", F.year("FullDate"), F.lit(1), F.lit(1))))
        .withColumn("YearEndDate",   F.to_date(F.concat_ws("-", F.year("FullDate"), F.lit(12), F.lit(31))))
    )

    # 8) Flags and formatting helpers
    df = (
        df
        .withColumn("IsWeekend", F.when(F.col("DayNumberOfWeek").isin(6,7), F.lit(True)).otherwise(F.lit(False)))
        .withColumn("IsLeapYear", ((F.year("FullDate") % 4 == 0) & (F.year("FullDate") % 100 != 0)) | (F.year("FullDate") % 400 == 0))
        .withColumn("YearMonth", F.date_format("FullDate", "yyyy-MM"))
        .withColumn("YearWeekISO", F.concat(F.date_format("FullDate","yyyy"), F.lit("-"), F.lpad(F.weekofyear("FullDate").cast("string"), 2, "0")))
        .withColumn("YYYYMM", F.date_format("FullDate","yyyyMM").cast("int"))
        .withColumn("YYYYQ",  F.concat(F.date_format("FullDate","yyyy"), F.lit("-Q"), F.quarter("FullDate")))
    )

    # 9) Surrogate key (int YYYYMMDD)
    df = df.withColumn("DateKey", F.date_format("FullDate", "yyyyMMdd").cast("int"))

    # 10) Holidays
    if holidays:
        hol_df = spark.createDataFrame([(h,) for h in holidays], T.StructType([T.StructField("HolidayDate", T.DateType(), False)])) \
                      .withColumn("IsHolidayMark", F.lit(True))
        df = (df
              .join(hol_df, df.FullDate == hol_df.HolidayDate, "left")
              .withColumn("IsHoliday", F.coalesce(F.col("IsHolidayMark"), F.lit(False)))
              .drop("HolidayDate", "IsHolidayMark"))
    else:
        df = df.withColumn("IsHoliday", F.lit(False))

    # 11) Final select & column order
    df = df.select(
        "DateKey", "FullDate",
        "DayNumberOfWeek", "DayName", "DayNumberOfMonth", "DayNumberOfYear",
        "WeekNumberOfYear", "WeekStartDate", "WeekEndDate",
        "MonthNumber", "MonthName", "MonthStartDate", "MonthEndDate",
        "QuarterNumber", "QuarterName", "QuarterStartDate", "QuarterEndDate",
        "YearNumber", "YearStartDate", "YearEndDate",
        "IsWeekend", "IsHoliday", "IsLeapYear",
        "YearMonth", "YearWeekISO", "YYYYMM", "YYYYQ"
    )

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date = dim_date_df(spark, "2010-01-01", "2025-12-31", holidays=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


target_path = "Tables/gold/DimDate"   # Fabric Lakehouse relative path 
target_table = "gold.DimDate"         # adjust database/schema

# Write as Delta (idempotent merge pattern if you want to upsert later)
(
    dim_date
    .repartition("YearNumber")                # helps parallelism & partitioning
    .write
    .format("delta")
    .mode("overwrite")                        # overwrite full range; for incremental, use merge (see below)
    .option("overwriteSchema", "true")
    .save(target_path)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table}
    USING DELTA
    LOCATION '{target_path}'
""")

# (Optional) add simple Z-Order / Optimize if supported
spark.sql(f"OPTIMIZE {target_table} ZORDER BY (YearNumber, MonthNumber)")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
