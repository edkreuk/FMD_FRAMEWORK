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


# PARAMETERS CELL ********************

# define parameters
# cleansing_rules = "[{\"function\": \"to_lower\", \"columns\" : \"City\", \"parameters\" : \"\"}, {\"function\": \"to_upper\", \"columns\" : \"Country\", \"parameters\" : \"\"}]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame
import json
import re
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if type(cleansing_rules) is str:
    cleansing_rules = json.loads(cleansing_rules)

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
