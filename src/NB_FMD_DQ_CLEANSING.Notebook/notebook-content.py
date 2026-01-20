# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# functioname # Data cleansing functions
# 
# It is possible to perform cleansing functions on incoming data. For example, converting all text in a column to uppercase. This can be achieved by defining cleansing rules for a table. The cleansing_rules contains a piece of JSON as shown below. This is an array of one or more functions that need to be called.
# 
# - function: name of the function
# - columns: semi-colon separated list of columns to which the function should be applied
# - parameters: JSON string with the different parameters and their values
# 
# Example:
# 
# ```
# [
#    {"function": "to_upper",
#     "columns": "TransactionTypeName"}, 
#    {"function": "custom_function_with_params",
#     "columns": "TransactionTypeName;LastEditedBy",
#     "parameters": {"param1" : "abc", "param2" : "123"}}
# ]
# ```
# 
# ## Custom functions
# 
# Custom functions can be added to the section bellow. The function has the following structure.
# 
# ```
# def <functioname> (df, columns, args):
# 
#     print(args['<custom parameter name>']) # use of custom parameters
# 
#     for column in columns: # apply function foreach column
#         df = df.<custom logic>
# 
#     return df #always return dataframe.
# ```
# 


# CELL ********************

%run NB_FMD_CUSTOM_DQ_CLEANSING

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dynamic_call_cleansing_function(df: DataFrame,
        func_name: str, 
        columns: str, 
        *args, 
        **kwargs):

    func = globals().get(func_name)

    if func:
        try:
            return func(df, columns, *args, **kwargs)
        except Exception as e:
            raise ValueError(f"Function '{func_name}' failed with Error: {e}")
    else:
        raise ValueError(f"Function '{func_name}' not found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def handle_cleansing_functions(
        df: DataFrame,
        cleansing_rules: json):

    if cleansing_rules is not None:
        for cleansing_rule in cleansing_rules:
            if 'function' not in cleansing_rule:
                print(f"function doesn't exists in: {cleansing_rule}")
                continue
            if 'parameters' not in cleansing_rule:
                cleansing_rule['parameters'] = None
            if 'columns' not in cleansing_rule:
                cleansing_rule['columns'] = None
            
            columns = re.split(';', cleansing_rule['columns'])
            columns = [column.strip() for column in columns if column != ""]

            print (f"\n\nFunction: {cleansing_rule['function']}\nParameters: {cleansing_rule['parameters']}\nColumns: {cleansing_rule['columns']}")
            df = dynamic_call_cleansing_function(df, cleansing_rule['function'], columns, cleansing_rule['parameters'])
    else:
        print(f"CleansingOptions doesn't exists in: {cleansing_rules}")
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cleansing functions
# 
# Different cleansing functions can be defined here. Add your own!

# CELL ********************

from pyspark.sql.functions import col, trim, regexp_replace, lower, upper, initcap, when, length, lit
from pyspark.sql import DataFrame

def normalize_text(df: DataFrame, columns, args):
    """
    Args (all optional in args dict):
      - case: one of {'lower','upper','title', None}  (default: None)
      - collapse_spaces: bool (default: True)
      - empty_as_null: bool (default: True)
    """
    case = args.get('case', None)
    collapse_spaces = args.get('collapse_spaces', True)
    empty_as_null = args.get('empty_as_null', True)

    for c in columns:
        expr = trim(col(c))
        if collapse_spaces:
            # Replace 2+ spaces with a single space
            expr = regexp_replace(expr, r"\s{2,}", " ")
        if case == 'lower':
            expr = lower(expr)
        elif case == 'upper':
            expr = upper(expr)
        elif case == 'title':
            expr = initcap(expr)

        if empty_as_null:
            expr = when(length(expr) == 0, lit(None)).otherwise(expr)

        df = df.withColumn(c, expr)
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import coalesce, lit

def fill_nulls(df: DataFrame, columns, args):
    """
    Args:
      - defaults: dict[str, any]   -> per-column default values
      - default_string: str or None
      - default_numeric: int/float or None
      - default_date: date string in 'yyyy-MM-dd' or None
    """
    defaults = args.get('defaults', {}) or {}
    default_string = args.get('default_string', None)
    default_numeric = args.get('default_numeric', None)
    default_date = args.get('default_date', None)

    for c in columns:
        if c in defaults:
            df = df.withColumn(c, coalesce(col(c), lit(defaults[c])))
        else:
            dtype = [f.dataType for f in df.schema.fields if f.name == c]
            dtype = dtype[0] if dtype else None
            if dtype is None:
                continue

            if default_string is not None and dtype.simpleString().startswith('string'):
                df = df.withColumn(c, coalesce(col(c), lit(default_string)))
            elif default_numeric is not None and dtype.simpleString() in ('int', 'bigint', 'double', 'float', 'decimal'):
                df = df.withColumn(c, coalesce(col(c), lit(default_numeric)))
            elif default_date is not None and dtype.simpleString() in ('date',):
                df = df.withColumn(c, coalesce(col(c), lit(default_date)))
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date, to_timestamp, when

def parse_datetime(df: DataFrame, columns, args):
    """
    Args:
      - target_type: 'date'|'timestamp' (default: 'date')
      - formats: list[str] of formats, e.g. ['yyyy-MM-dd','dd/MM/yyyy','MM-dd-yyyy']
      - into: str or None  -> if provided and len(columns)==1, write into this column name
      - keep_original: bool (default: True)
    """
    target_type = args.get('target_type', 'date')
    formats = args.get('formats', ['yyyy-MM-dd'])
    into = args.get('into', None)
    keep_original = args.get('keep_original', True)

    for c in columns:
        parsed = None
        for fmt in formats:
            candidate = to_timestamp(col(c), fmt) if target_type == 'timestamp' else to_date(col(c), fmt)
            parsed = candidate if parsed is None else coalesce(parsed, candidate)

        out_col = into if (into and len(columns) == 1) else c
        df = df.withColumn(out_col, parsed)
        if into and not keep_original and out_col != c:
            df = df.drop(c)
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, upper, lower

def to_upper(df, columns, args):
    #print(args['param1'])
    for column in columns:
        df = df.withColumn(column, upper(col(column)))
    return df

def to_lower(df, columns, args):
    #print(args['param1'])
    for column in columns:
        df = df.withColumn(column, lower(col(column)))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Execution

# CELL ********************

# apply rules to dataframe
dfDataChanged = handle_cleansing_functions(dfDataChanged, cleansing_rules)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
