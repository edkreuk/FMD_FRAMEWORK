---
Title: Data cleansing examples for the FMD Framework
Description: Learn how to define data cleansing operations using metadata-driven JSON for the Fabric Metadata-Driven (FMD) Framework.
Topic: how-to
Date: 07/2025
Author: edkreuk
---

# Data cleansing examples for the FMD Framework

This article provides sample metadata-driven JSON configurations for common data cleansing operations in the Fabric Metadata-Driven (FMD) Framework. Use these examples to define cleansing steps in your data pipelines.

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