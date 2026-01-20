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

def execute_with_outputs(exec_statement, driver, connstring, database, **params):
    """
    Runs the given T-SQL (optionally wrapping to capture return code).
    Returns a dict with:
      - result_sets: list[list[dict]]
      - return_code: int or None
      - out_params: dict (if you selected them)
      - messages: list[str]
    """
    # Get token for Azure SQL authentication
    token = notebookutils.credentials.getToken('https://analysis.windows.net/powerbi/api').encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token)}s', len(token), token)

    # Build connection
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={connstring};PORT=1433;DATABASE={database};",
        attrs_before={1256: token_struct},
        timeout=12
    )
    if exec_statement:
        # Use the safe builder for stored procedures
        sql_to_run = build_exec_statement(exec_statement, **params)
        use_wrapper = True   # we know we appended a return code / out params trailer
    else:
        if not exec_statement:
            raise ValueError("Provide either proc_name+params or exec_statement.")
        trimmed = exec_statement.strip().upper()
        use_wrapper = trimmed.startswith("EXEC ") or trimmed.startswith("EXECUTE ")
        if use_wrapper and include_return_code:
            # Add return code wrapper if it's a bare EXEC
            sql_to_run = f"""
            SET NOCOUNT ON;
            DECLARE @__ret INT;
            {exec_statement.rstrip(';')};
            SELECT @__ret AS __return_code__;
            """
        else:
            sql_to_run = exec_statement


    result_sets = []
    messages = []
    return_code = None
    out_params = {}

    try:
        with conn.cursor() as cursor:
            # Warm-up
            cursor.execute("SELECT 1")
            cursor.fetchone()
            conn.timeout = 10

            cursor.execute(sql_to_run)

            # Collect result sets
            while True:
                if cursor.description:
                    cols = [d[0] for d in cursor.description]
                    rows = cursor.fetchall()
                    result_sets.append([dict(zip(cols, r)) for r in rows])
                if not cursor.nextset():
                    break

            # If wrapped, pick return code from the last set (and remove it from result_sets)
            if use_wrapper and result_sets:
                last = result_sets[-1]
                if len(last) == 1 and "__return_code__" in last[0]:
                    return_code = last[0]["__return_code__"]
                    result_sets = result_sets[:-1]  # remove synthetic RC set

            # If you also SELECT’ed OUTPUT params (e.g., SELECT @p AS p)
            # you can parse them from another final small result set:
            # Example pattern:
            #   SELECT @out1 AS __out_out1, @out2 AS __out_out2;
            if result_sets:
                # Heuristic: if the final set looks like a single-row out-param bag, peel it off
                maybe = result_sets[-1]
                if len(maybe) == 1 and any(k.startswith("__out_") for k in maybe[0].keys()):
                    out_params = {k.replace("__out_", ""): v for k, v in maybe[0].items()}
                    result_sets = result_sets[:-1]

            try:
                cursor.commit()
            except:
                pass

    finally:
        try:
            conn.close()
        except:
            pass

    return {
        "result_sets": result_sets,
        "return_code": return_code,
        "out_params": out_params,
        "messages": messages
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
