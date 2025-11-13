# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Utility Functions
# This notebook contains function declarations for use in other notebooks.

# CELL ********************

import struct, pyodbc
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_exec_statement(proc_name, **params):
    param_strs = []
    for key, value in params.items():
        if value is not None:
            if isinstance(value, str):
                param_strs.append(f"@{key}='{value}'")
            else:
                param_strs.append(f"@{key}={value}")
    
    if param_strs:
        return f"EXEC {proc_name}, " + ", ".join(param_strs)
    else:
        return f"EXEC {proc_name}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def execute_with_logging(exec_statement, driver, connstring, database, **params):

    # Get token for Azure SQL authentication
    token = notebookutils.credentials.getToken('https://analysis.windows.net/powerbi/api').encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token)}s', len(token), token)

    # Build connection
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={connstring};PORT=1433;DATABASE={database};",
        attrs_before={1256: token_struct},
        timeout=12
    )

    exec_statement = build_exec_statement(exec_statement, **params)

    start_time = datetime.utcnow()
    status = "Success"
    error_message = None

    try:
        with conn.cursor() as cursor:
            # Warm-up query
            cursor.execute("SELECT 1")
            cursor.fetchone()
            conn.timeout = 10


            # Build EXEC statement dynamically

            print(f"Executing: {exec_statement}")

            cursor.execute(exec_statement)
            cursor.commit()

    except pyodbc.OperationalError as e:
        print(e) 
    except Exception as e:
        status = "Failed"
        error_message = str(e)
        print(f"Error: {error_message}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
