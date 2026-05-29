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

# ## Data Quality Rule functions
# 
# It is possible to perform data quality validation on incoming data. For example, checking that a column has no null values. This can be achieved by defining DQ rules for a table. The `dq_rules` variable contains a piece of JSON as shown below. This is an array of one or more rules that need to be applied.
# 
# - rule: name of the validation rule function
# - columns: semi-colon separated list of columns to which the rule should be applied
# - parameters: JSON object with rule-specific parameters
# - action: what to do when rows fail the rule — `"reject"` (raise an error) or `"flag"` (add a boolean flag column, default)
# 
# Example:
# 
# ```
# [
#    {"rule": "not_null",
#     "columns": "CustomerId;Name",
#     "action": "reject"},
#    {"rule": "allowed_values",
#     "columns": "Status",
#     "parameters": {"values": ["Active", "Inactive"]},
#     "action": "flag"},
#    {"rule": "min_value",
#     "columns": "Age",
#     "parameters": {"min": 0},
#     "action": "reject"},
#    {"rule": "max_value",
#     "columns": "Price",
#     "parameters": {"max": 99999.99},
#     "action": "flag"},
#    {"rule": "regex_pattern",
#     "columns": "Email",
#     "parameters": {"pattern": "^[^@]+@[^@]+\\.[^@]+$"},
#     "action": "flag"},
#    {"rule": "min_length",
#     "columns": "Description",
#     "parameters": {"length": 5},
#     "action": "flag"},
#    {"rule": "max_length",
#     "columns": "Code",
#     "parameters": {"length": 10},
#     "action": "reject"}
# ]
# ```
# More info https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Quality-Rules
# 
# 
# ## Custom functions
# 
# Custom DQ rule functions can be added in a separate notebook NB_FMD_CUSTOM_DQ_RULES
# 
# ```
# def <functionname> (df, columns, args, action):
# 
#     print(args['<custom parameter name>']) # use of custom parameters
# 
#     for column in columns: # apply function foreach column
#         df = df.withColumn(column, <transformation>)
# 
#     return df #always return dataframe.
# ```
# 


# CELL ********************

%run NB_FMD_CUSTOM_DQ_RULES

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dynamic_call_dq_function(df: DataFrame,
        func_name: str,
        columns: str,
        *args,
        **kwargs):

    func = globals().get(func_name)

    if func:
        try:
            return func(df, columns, *args, **kwargs)
        except Exception as e:
            raise ValueError(f"DQ rule function '{func_name}' failed with Error: {e}")
    else:
        raise ValueError(f"DQ rule function '{func_name}' not found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_dq_rules(dq_rules):
    if dq_rules is None:
        return []

    # JSON string → Python object
    if isinstance(dq_rules, str):
        dq_rules = dq_rules.strip()
        if not dq_rules:
            return []
        dq_rules = json.loads(dq_rules)

    # Single dict → wrap in list
    if isinstance(dq_rules, dict):
        dq_rules = [dq_rules]

    if not isinstance(dq_rules, list):
        raise TypeError(
            f"dq_rules must be a list of dicts, got {type(dq_rules).__name__}"
        )

    for i, rule in enumerate(dq_rules):
        if not isinstance(rule, dict):
            raise TypeError(
                f"Rule at index {i} is not a dict (got {type(rule).__name__})"
            )

    return dq_rules

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def handle_dq_rules(df: DataFrame, dq_rules):
    dq_rules = normalize_dq_rules(dq_rules)

    for rule in dq_rules:
        rule_name = rule.get("rule")
        if not rule_name:
            print(f"'rule' missing in: {rule}")
            continue

        parameters = rule.get("parameters")
        columns_raw = rule.get("columns")
        action = rule.get("action", "flag")

        columns = (
            [c.strip() for c in columns_raw.split(";") if c.strip()]
            if columns_raw else []
        )

        print(
            f"\nDQ Rule: {rule_name}"
            f"\nAction: {action}"
            f"\nParameters: {parameters}"
            f"\nColumns: {columns}"
        )

        df = dynamic_call_dq_function(
            df,
            rule_name,
            columns,
            parameters,
            action
        )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Quality Rule functions
# 


# CELL ********************

from pyspark.sql.functions import col, trim, length, lit, regexp_extract
from pyspark.sql import DataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def not_null(df: DataFrame, columns, args, action):
    """
    Validates that the specified columns contain no null values.

    Args (no args required):
      - (none)

    Action:
      - "reject": raises a ValueError if any null values are found
      - "flag": adds a boolean column DQFlag_not_null_<column> (True = passed, False = failed)
    """
    for c in columns:
        failing = df.filter(col(c).isNull())
        fail_count = failing.count()

        if action == "reject":
            if fail_count > 0:
                raise ValueError(
                    f"DQ rule 'not_null' failed for column '{c}': "
                    f"{fail_count} row(s) contain null values."
                )
        else:
            flag_col = f"DQFlag_not_null_{c}"
            df = df.withColumn(flag_col, col(c).isNotNull())
            if fail_count > 0:
                print(f"  ⚠ DQ rule 'not_null' flagged {fail_count} row(s) in column '{c}'")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def not_empty(df: DataFrame, columns, args, action):
    """
    Validates that the specified string columns contain no empty or whitespace-only values.

    Args (no args required):
      - (none)

    Action:
      - "reject": raises a ValueError if any empty values are found
      - "flag": adds a boolean column DQFlag_not_empty_<column> (True = passed, False = failed)
    """
    for c in columns:
        failing = df.filter(
            col(c).isNull() | (trim(col(c)) == lit(""))
        )
        fail_count = failing.count()

        if action == "reject":
            if fail_count > 0:
                raise ValueError(
                    f"DQ rule 'not_empty' failed for column '{c}': "
                    f"{fail_count} row(s) contain empty or whitespace-only values."
                )
        else:
            flag_col = f"DQFlag_not_empty_{c}"
            df = df.withColumn(
                flag_col,
                col(c).isNotNull() & (trim(col(c)) != lit(""))
            )
            if fail_count > 0:
                print(f"  ⚠ DQ rule 'not_empty' flagged {fail_count} row(s) in column '{c}'")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def min_value(df: DataFrame, columns, args, action):
    """
    Validates that numeric column values are greater than or equal to a minimum.

    Args:
      - min: numeric minimum value (inclusive)

    Action:
      - "reject": raises a ValueError if any values fall below the minimum
      - "flag": adds a boolean column DQFlag_min_value_<column> (True = passed, False = failed)
    """
    min_val = args.get('min') if args else None
    if min_val is None:
        raise ValueError("DQ rule 'min_value' requires a 'min' parameter.")

    for c in columns:
        failing = df.filter(
            col(c).isNotNull() & (col(c) < lit(min_val))
        )
        fail_count = failing.count()

        if action == "reject":
            if fail_count > 0:
                raise ValueError(
                    f"DQ rule 'min_value' failed for column '{c}': "
                    f"{fail_count} row(s) are below the minimum value of {min_val}."
                )
        else:
            flag_col = f"DQFlag_min_value_{c}"
            df = df.withColumn(
                flag_col,
                col(c).isNull() | (col(c) >= lit(min_val))
            )
            if fail_count > 0:
                print(f"  ⚠ DQ rule 'min_value' flagged {fail_count} row(s) in column '{c}' (min={min_val})")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def max_value(df: DataFrame, columns, args, action):
    """
    Validates that numeric column values are less than or equal to a maximum.

    Args:
      - max: numeric maximum value (inclusive)

    Action:
      - "reject": raises a ValueError if any values exceed the maximum
      - "flag": adds a boolean column DQFlag_max_value_<column> (True = passed, False = failed)
    """
    max_val = args.get('max') if args else None
    if max_val is None:
        raise ValueError("DQ rule 'max_value' requires a 'max' parameter.")

    for c in columns:
        failing = df.filter(
            col(c).isNotNull() & (col(c) > lit(max_val))
        )
        fail_count = failing.count()

        if action == "reject":
            if fail_count > 0:
                raise ValueError(
                    f"DQ rule 'max_value' failed for column '{c}': "
                    f"{fail_count} row(s) exceed the maximum value of {max_val}."
                )
        else:
            flag_col = f"DQFlag_max_value_{c}"
            df = df.withColumn(
                flag_col,
                col(c).isNull() | (col(c) <= lit(max_val))
            )
            if fail_count > 0:
                print(f"  ⚠ DQ rule 'max_value' flagged {fail_count} row(s) in column '{c}' (max={max_val})")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def allowed_values(df: DataFrame, columns, args, action):
    """
    Validates that column values belong to a predefined set of allowed values.

    Args:
      - values: list of allowed values

    Action:
      - "reject": raises a ValueError if any values are not in the allowed set
      - "flag": adds a boolean column DQFlag_allowed_values_<column> (True = passed, False = failed)
    """
    values_list = args.get('values') if args else None
    if not values_list:
        raise ValueError("DQ rule 'allowed_values' requires a non-empty 'values' parameter.")

    for c in columns:
        failing = df.filter(
            col(c).isNotNull() & ~col(c).isin(values_list)
        )
        fail_count = failing.count()

        if action == "reject":
            if fail_count > 0:
                raise ValueError(
                    f"DQ rule 'allowed_values' failed for column '{c}': "
                    f"{fail_count} row(s) contain values not in the allowed set {values_list}."
                )
        else:
            flag_col = f"DQFlag_allowed_values_{c}"
            df = df.withColumn(
                flag_col,
                col(c).isNull() | col(c).isin(values_list)
            )
            if fail_count > 0:
                print(f"  ⚠ DQ rule 'allowed_values' flagged {fail_count} row(s) in column '{c}'")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def regex_pattern(df: DataFrame, columns, args, action):
    """
    Validates that string column values match a given regular expression pattern.

    Args:
      - pattern: regular expression pattern string

    Action:
      - "reject": raises a ValueError if any values do not match the pattern
      - "flag": adds a boolean column DQFlag_regex_pattern_<column> (True = passed, False = failed)
    """
    pattern = args.get('pattern') if args else None
    if not pattern:
        raise ValueError("DQ rule 'regex_pattern' requires a 'pattern' parameter.")

    for c in columns:
        failing = df.filter(
            col(c).isNotNull() & (regexp_extract(col(c), pattern, 0) == lit(""))
        )
        fail_count = failing.count()

        if action == "reject":
            if fail_count > 0:
                raise ValueError(
                    f"DQ rule 'regex_pattern' failed for column '{c}': "
                    f"{fail_count} row(s) do not match the pattern '{pattern}'."
                )
        else:
            flag_col = f"DQFlag_regex_pattern_{c}"
            df = df.withColumn(
                flag_col,
                col(c).isNull() | (regexp_extract(col(c), pattern, 0) != lit(""))
            )
            if fail_count > 0:
                print(f"  ⚠ DQ rule 'regex_pattern' flagged {fail_count} row(s) in column '{c}'")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def min_length(df: DataFrame, columns, args, action):
    """
    Validates that string column values have at least a minimum length.

    Args:
      - length: minimum number of characters (inclusive)

    Action:
      - "reject": raises a ValueError if any values are shorter than the minimum
      - "flag": adds a boolean column DQFlag_min_length_<column> (True = passed, False = failed)
    """
    min_len = args.get('length') if args else None
    if min_len is None:
        raise ValueError("DQ rule 'min_length' requires a 'length' parameter.")

    for c in columns:
        failing = df.filter(
            col(c).isNotNull() & (length(col(c)) < lit(min_len))
        )
        fail_count = failing.count()

        if action == "reject":
            if fail_count > 0:
                raise ValueError(
                    f"DQ rule 'min_length' failed for column '{c}': "
                    f"{fail_count} row(s) are shorter than the minimum length of {min_len}."
                )
        else:
            flag_col = f"DQFlag_min_length_{c}"
            df = df.withColumn(
                flag_col,
                col(c).isNull() | (length(col(c)) >= lit(min_len))
            )
            if fail_count > 0:
                print(f"  ⚠ DQ rule 'min_length' flagged {fail_count} row(s) in column '{c}' (min_length={min_len})")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def max_length(df: DataFrame, columns, args, action):
    """
    Validates that string column values do not exceed a maximum length.

    Args:
      - length: maximum number of characters (inclusive)

    Action:
      - "reject": raises a ValueError if any values exceed the maximum length
      - "flag": adds a boolean column DQFlag_max_length_<column> (True = passed, False = failed)
    """
    max_len = args.get('length') if args else None
    if max_len is None:
        raise ValueError("DQ rule 'max_length' requires a 'length' parameter.")

    for c in columns:
        failing = df.filter(
            col(c).isNotNull() & (length(col(c)) > lit(max_len))
        )
        fail_count = failing.count()

        if action == "reject":
            if fail_count > 0:
                raise ValueError(
                    f"DQ rule 'max_length' failed for column '{c}': "
                    f"{fail_count} row(s) exceed the maximum length of {max_len}."
                )
        else:
            flag_col = f"DQFlag_max_length_{c}"
            df = df.withColumn(
                flag_col,
                col(c).isNull() | (length(col(c)) <= lit(max_len))
            )
            if fail_count > 0:
                print(f"  ⚠ DQ rule 'max_length' flagged {fail_count} row(s) in column '{c}' (max_length={max_len})")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
