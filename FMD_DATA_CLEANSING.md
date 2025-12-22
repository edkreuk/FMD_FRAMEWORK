---
Title: Data cleansing examples for the FMD Framework
Description: Learn how to define data cleansing operations using metadata-driven JSON for the Fabric Metadata-Driven Framework (FMD).
Topic: how-to
Date: 07/2025
Author: edkreuk
---

## Data cleansing

Data cleansing is an essential step in the deployment process to ensure that ingested data is standardized, accurate, and ready for downstream analytics. The FMD Framework allows you to define custom cleansing rules for the Bronze and Silver layers, helping automate data quality improvements during pipeline execution.

You can define data cleansing rules for the Bronze and Silver layers. Cleansing rules are specified as a JSON array, where each object defines a function, target columns, and optional parameters.

- `function`: Name of the cleansing function
- `columns`: Semicolon-separated list of columns
- `parameters`: (Optional) JSON object with function parameters

**Example:**

```json
[
    {"function": "to_upper", "columns": "TransactionTypeName"},
    {"function": "custom_function_with_params", "columns": "TransactionTypeName;LastEditedBy", "parameters": {"param1": "abc", "param2": "123"}}
]
```

# Data cleansing examples 

This article provides sample metadata-driven JSON configurations for common data cleansing operations in the Fabric Metadata-Driven Framework (FMD). Use these examples to define cleansing steps in your data pipelines.

Custom functions can be added in `NB_FMD_DQ_CLEANSING`. Each function should use the following structure:

> [!NOTE]
> This notebook will be created after the first initial run and will not be overwritten if you run the deployment again

```python
def <function_name>(df, columns, args):
    # Access custom parameters
    print(args['<custom parameter name>'])

    # Apply logic to each column
    for column in columns:
        df = df.<custom logic>

    return df  # Always return the DataFrame
```

## Remove duplicates

Removes duplicate records based on the specified column.

```json
[
    {
        "function": "remove_duplicates",
        "columns": "TransactionID"
    }
]
```

## Trim whitespace

Removes leading and trailing whitespace from the specified column.

```json
[
    {
        "function": "trim_whitespace",
        "columns": "CustomerName"
    }
]
```

## Replace null values

Replaces null values in the specified column with a defined replacement value.

```json
[
    {
        "function": "replace_nulls",
        "columns": "TransactionAmount",
        "parameters": {
            "replacement_value": "0"
        }
    }
]
```

## Standardize date formats

Converts date values in the specified column to a standard format.

```json
[
    {
        "function": "standardize_date_format",
        "columns": "TransactionDate",
        "parameters": {
            "format": "YYYY-MM-DD"
        }
    }
]
```

## Convert data types

Converts the data type of the specified column.

```json
[
    {
        "function": "convert_data_type",
        "columns": "TransactionAmount",
        "parameters": {
            "data_type": "float"
        }
    }
]
```

## Remove special characters

Removes special characters from the specified column.

```json
[
    {
        "function": "remove_special_characters",
        "columns": "CustomerName"
    }