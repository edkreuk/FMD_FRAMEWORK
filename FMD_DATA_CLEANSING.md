**Data Cleansing Examples**

### Removing Duplicates
```json
[
    {
        "function": "remove_duplicates",
        "columns": "TransactionID"
    }
]
```

### Trimming Whitespace
```json
[
    {
        "function": "trim_whitespace",
        "columns": "CustomerName"
    }
]
```

### Replacing Null Values
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

### Standardizing Date Formats
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

### Converting Data Types
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

### Removing Special Characters
```json
[
    {
        "function": "remove_special_characters",
        "columns": "CustomerName"
    }
]
```
