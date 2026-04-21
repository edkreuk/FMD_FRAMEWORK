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

# ## Data Quality Rule functions using Great Expectations
# 
# This notebook provides a **Great Expectations (GX)**-powered alternative to `NB_FMD_DQ_RULES`.
# It accepts the same `dq_rules` JSON metadata format and can be used as a drop-in replacement
# by swapping `%run NB_FMD_DQ_RULES` for `%run NB_FMD_DQ_GX` in the pipeline notebooks.
# 
# Use `handle_dq_rules_gx(df, dq_rules)` — or the alias `handle_dq_rules(df, dq_rules)` — in place of the standard implementation.
# 
# Great Expectations validates the data and produces a structured validation report. For each rule:
# - `"action": "reject"` raises a `ValueError` (halting the pipeline) when rows fail.
# - `"action": "flag"` (default) adds a boolean `DQFlag_<rule>_<column>` column to the DataFrame.
# 
# ### Rule → Great Expectations mapping
# 
# | FMD rule         | Great Expectations expectation                               |
# |------------------|--------------------------------------------------------------|
# | `not_null`       | `ExpectColumnValuesToNotBeNull`                               |
# | `not_empty`      | `ExpectColumnValuesToNotMatchRegex` (pattern `^\s*$`)         |
# | `min_value`      | `ExpectColumnValuesToBeBetween` (lower bound only)            |
# | `max_value`      | `ExpectColumnValuesToBeBetween` (upper bound only)            |
# | `allowed_values` | `ExpectColumnValuesToBeInSet`                                 |
# | `regex_pattern`  | `ExpectColumnValuesToMatchRegex`                              |
# | `min_length`     | `ExpectColumnValueLengthsToBeBetween` (lower bound only)      |
# | `max_length`     | `ExpectColumnValueLengthsToBeBetween` (upper bound only)      |
# 
# ### Example JSON (same format as `NB_FMD_DQ_RULES`)
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
#    {"rule": "regex_pattern",
#     "columns": "Email",
#     "parameters": {"pattern": "^[^@]+@[^@]+\\.[^@]+$"},
#     "action": "flag"}
# ]
# ```
# More info https://github.com/edkreuk/FMD_FRAMEWORK/wiki/Data-Quality-Rules
# 
# ### Prerequisites
# 
# `great-expectations` must be installed. Add it to the Fabric Environment used by this notebook,
# or install it at notebook startup (see the install cell below).
# 


# CELL ********************

%pip install great-expectations --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToNotMatchRegex,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValueLengthsToBeBetween,
)
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, length, lit, regexp_extract

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Core functions


# CELL ********************

def normalize_dq_rules(dq_rules):
    if dq_rules is None:
        return []

    # JSON string -> Python object
    if isinstance(dq_rules, str):
        dq_rules = dq_rules.strip()
        if not dq_rules:
            return []
        dq_rules = json.loads(dq_rules)

    # Single dict -> wrap in list
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

def _build_gx_expectation(rule_name, column, parameters):
    """
    Converts a single FMD DQ rule into a Great Expectations Expectation object.

    Args:
      - rule_name: FMD rule identifier (e.g. 'not_null', 'min_value')
      - column: target column name
      - parameters: dict of rule-specific parameters (may be None)

    Returns:
      A Great Expectations Expectation instance ready to add to an ExpectationSuite.
    """
    parameters = parameters or {}

    if rule_name == "not_null":
        return ExpectColumnValuesToNotBeNull(column=column)

    elif rule_name == "not_empty":
        # Flag values that are empty strings or contain only whitespace
        return ExpectColumnValuesToNotMatchRegex(column=column, regex=r"^\s*$")

    elif rule_name == "min_value":
        min_val = parameters.get("min")
        if min_val is None:
            raise ValueError("DQ rule 'min_value' requires a 'min' parameter.")
        return ExpectColumnValuesToBeBetween(column=column, min_value=min_val, max_value=None)

    elif rule_name == "max_value":
        max_val = parameters.get("max")
        if max_val is None:
            raise ValueError("DQ rule 'max_value' requires a 'max' parameter.")
        return ExpectColumnValuesToBeBetween(column=column, min_value=None, max_value=max_val)

    elif rule_name == "allowed_values":
        values = parameters.get("values")
        if not values:
            raise ValueError("DQ rule 'allowed_values' requires a non-empty 'values' parameter.")
        return ExpectColumnValuesToBeInSet(column=column, value_set=values)

    elif rule_name == "regex_pattern":
        pattern = parameters.get("pattern")
        if not pattern:
            raise ValueError("DQ rule 'regex_pattern' requires a 'pattern' parameter.")
        return ExpectColumnValuesToMatchRegex(column=column, regex=pattern)

    elif rule_name == "min_length":
        min_len = parameters.get("length")
        if min_len is None:
            raise ValueError("DQ rule 'min_length' requires a 'length' parameter.")
        return ExpectColumnValueLengthsToBeBetween(column=column, min_value=min_len, max_value=None)

    elif rule_name == "max_length":
        max_len = parameters.get("length")
        if max_len is None:
            raise ValueError("DQ rule 'max_length' requires a 'length' parameter.")
        return ExpectColumnValueLengthsToBeBetween(column=column, min_value=None, max_value=max_len)

    else:
        raise ValueError(
            f"Unknown DQ rule '{rule_name}'. "
            "Supported rules: not_null, not_empty, min_value, max_value, "
            "allowed_values, regex_pattern, min_length, max_length."
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _add_flag_column(df, rule_name, column, parameters):
    """
    Adds a boolean DQFlag_<rule>_<column> column to the DataFrame for row-level traceability.
    True = row passed the rule, False = row failed the rule.

    Used in "flag" mode to annotate the DataFrame after GX has confirmed a failure exists.
    """
    parameters = parameters or {}
    flag_col = f"DQFlag_{rule_name}_{column}"

    if rule_name == "not_null":
        flag_expr = col(column).isNotNull()

    elif rule_name == "not_empty":
        flag_expr = col(column).isNotNull() & (trim(col(column)) != lit(""))

    elif rule_name == "min_value":
        min_val = parameters.get("min")
        flag_expr = col(column).isNull() | (col(column) >= lit(min_val))

    elif rule_name == "max_value":
        max_val = parameters.get("max")
        flag_expr = col(column).isNull() | (col(column) <= lit(max_val))

    elif rule_name == "allowed_values":
        values = parameters.get("values", [])
        flag_expr = col(column).isNull() | col(column).isin(values)

    elif rule_name == "regex_pattern":
        pattern = parameters.get("pattern", "")
        flag_expr = col(column).isNull() | (regexp_extract(col(column), pattern, 0) != lit(""))

    elif rule_name == "min_length":
        min_len = parameters.get("length")
        flag_expr = col(column).isNull() | (length(col(column)) >= lit(min_len))

    elif rule_name == "max_length":
        max_len = parameters.get("length")
        flag_expr = col(column).isNull() | (length(col(column)) <= lit(max_len))

    else:
        return df

    return df.withColumn(flag_col, flag_expr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def handle_dq_rules_gx(df: DataFrame, dq_rules):
    """
    Validates a PySpark DataFrame against metadata-defined DQ rules using Great Expectations.

    Accepts the same dq_rules JSON format as handle_dq_rules() in NB_FMD_DQ_RULES and can
    be used as a drop-in replacement by swapping %run NB_FMD_DQ_RULES for %run NB_FMD_DQ_GX.

    All expectations are applied in a single GX validation pass, producing a structured report.

    For each rule:
      - action "reject": raises a ValueError if any rows fail the expectation.
      - action "flag" (default): adds a boolean DQFlag_<rule>_<column> column (True = passed).

    Returns the DataFrame (potentially with DQFlag columns added for "flag" rules).
    """
    dq_rules = normalize_dq_rules(dq_rules)

    if not dq_rules:
        return df

    # Create an ephemeral (in-memory) GX context — no filesystem configuration required
    context = gx.get_context(mode="ephemeral")

    # Register the Spark datasource using the active Spark session
    datasource = context.data_sources.add_spark(name="fmd_spark_datasource")
    asset = datasource.add_dataframe_asset(name="fmd_dataframe_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(name="fmd_batch_definition")

    # Build the ExpectationSuite from the dq_rules metadata
    suite = context.suites.add(gx.ExpectationSuite(name="fmd_dq_suite"))

    # Track per-rule context in the same order as suite expectations
    rule_metadata = []

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
            f"\nDQ Rule (GX): {rule_name}"
            f"\nAction: {action}"
            f"\nParameters: {parameters}"
            f"\nColumns: {columns}"
        )

        for column in columns:
            expectation = _build_gx_expectation(rule_name, column, parameters)
            suite.add_expectation(expectation)
            rule_metadata.append((rule_name, column, parameters or {}, action))

    # Run all expectations in a single GX validation pass
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="fmd_validation",
            data=batch_definition,
            suite=suite,
        )
    )
    result = validation_definition.run(batch_parameters={"dataframe": df})

    # Print the validation summary
    print(f"\n{'='*60}")
    print("Great Expectations Validation Summary")
    print(f"  Overall success : {result.success}")
    print(f"  Expectations    : {len(result.results)}")
    print(f"{'='*60}")

    # Process each expectation result
    reject_errors = []

    for i, exp_result in enumerate(result.results):
        rule_name, column, parameters, action = rule_metadata[i]

        passed = exp_result.success
        unexpected_count = (exp_result.result or {}).get("unexpected_count") or 0
        unexpected_pct = (exp_result.result or {}).get("unexpected_percent") or 0.0

        status = "✅ PASSED" if passed else "❌ FAILED"
        detail = (
            f" | Failing rows: {unexpected_count} ({unexpected_pct:.2f}%)"
            if not passed else ""
        )
        print(f"  {status} | Rule: {rule_name} | Column: {column}{detail}")

        if action == "reject" and not passed:
            reject_errors.append(
                f"DQ rule '{rule_name}' (Great Expectations) failed for column '{column}': "
                f"{unexpected_count} row(s) ({unexpected_pct:.2f}%) did not pass the expectation."
            )

        if action == "flag":
            df = _add_flag_column(df, rule_name, column, parameters)
            if not passed:
                print(f"    ⚠ Flagged {unexpected_count} row(s) in DQFlag_{rule_name}_{column}")

    # Raise after processing all results so every failure is reported at once
    if reject_errors:
        raise ValueError("\n".join(reject_errors))

    return df


# Alias so this notebook is a drop-in replacement for NB_FMD_DQ_RULES
handle_dq_rules = handle_dq_rules_gx

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
