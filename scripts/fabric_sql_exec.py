"""
Execute SQL against SQL_FMD_FRAMEWORK (Fabric SQL Database) from local machine.
Uses ODBC Driver 18 built-in ActiveDirectoryInteractive auth — NO az CLI needed.
A browser window pops up once for login, then it's cached.

Usage:
    python scripts/fabric_sql_exec.py                    # Run full registration
    python scripts/fabric_sql_exec.py --query "SELECT 1" # Run a single query
    python scripts/fabric_sql_exec.py --check             # Just check what's registered
"""

import sys
import pyodbc

# Fabric SQL Database config (from config/item_config.yaml)
SERVER = "nl7yhqnbrscude3yv6mas6bxpq-tndhi54vbs4urpccunwiydulzi.datawarehouse.fabric.microsoft.com,1433"
DATABASE = "SQL_INTEGRATION_FRAMEWORK"
DRIVER = "{ODBC Driver 18 for SQL Server}"


def connect():
    """Connect to Fabric SQL Database using ODBC Driver 18 built-in Entra ID auth.
    This pops up a browser login window — no az CLI or pip installs needed."""
    conn_str = (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Authentication=ActiveDirectoryInteractive;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        return conn
    except pyodbc.Error as e:
        print(f"CONNECTION ERROR: {e}")
        print("\nTroubleshooting:")
        print("  1. Make sure you have ODBC Driver 18 for SQL Server installed")
        print("  2. A browser window should pop up for Microsoft login")
        print("  3. Use your IP Corp / Entra ID credentials")
        print("  4. Check that your account has access to SQL_FMD_FRAMEWORK")
        sys.exit(1)


def execute_query(conn, sql, description=""):
    """Execute a SQL query and print results."""
    if description:
        print(f"\n{'='*60}")
        print(f"  {description}")
        print(f"{'='*60}")
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        if cursor.description:
            cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            if rows:
                widths = [max(len(str(c)), max(len(str(r[i])) for r in rows)) for i, c in enumerate(cols)]
                header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
                print(header)
                print("-+-".join("-" * w for w in widths))
                for row in rows:
                    print(" | ".join(str(v).ljust(w) for v, w in zip(row, widths)))
                print(f"({len(rows)} rows)")
            else:
                print("(0 rows)")
        else:
            conn.commit()
            print("OK (no result set)")
    except pyodbc.Error as e:
        print(f"SQL ERROR: {e}")


def check_state(conn):
    """Show current state of all registration tables."""
    execute_query(conn,
        "SELECT ConnectionId, ConnectionGuid, Name, Type, IsActive FROM integration.Connection ORDER BY ConnectionId",
        "Connections")
    execute_query(conn,
        """SELECT ds.DataSourceId, ds.Name, ds.Namespace, ds.Type, ds.IsActive, c.Name AS ConnectionName
           FROM integration.DataSource ds
           JOIN integration.Connection c ON ds.ConnectionId = c.ConnectionId
           ORDER BY ds.DataSourceId""",
        "Data Sources")
    execute_query(conn,
        "SELECT LakehouseId, Name FROM integration.Lakehouse ORDER BY LakehouseId",
        "Lakehouses")
    execute_query(conn,
        "SELECT WorkspaceId, Name FROM integration.Workspace ORDER BY WorkspaceId",
        "Workspaces")
    execute_query(conn,
        """SELECT le.LandingzoneEntityId, le.SourceSchema, le.SourceName, le.FileName, le.IsActive, ds.Name AS DataSourceName
           FROM integration.LandingzoneEntity le
           JOIN integration.DataSource ds ON le.DataSourceId = ds.DataSourceId
           ORDER BY le.LandingzoneEntityId""",
        "Landing Zone Entities")
    execute_query(conn,
        "SELECT PipelineId, Name FROM integration.Pipeline ORDER BY PipelineId",
        "Pipelines")


def register_m3cloud_connection(conn):
    """Register the M3 Cloud connection + DI_PRD_Staging data source."""
    print("\n>>> Registering M3 Cloud connection...")
    execute_query(conn,
        """EXEC [integration].[sp_UpsertConnection]
            @ConnectionGuid = '1187a5d7-5d6e-4a23-8c54-2a0734350629',
            @Name = 'CON_FMD_SQL2016LIVE_M3CLOUD',
            @Type = 'SqlServer',
            @IsActive = 1""",
        "Register Connection: M3 Cloud (sql2016live)")

    print("\n>>> Registering DI_PRD_Staging data source...")
    execute_query(conn,
        """DECLARE @ConnId INT = (SELECT ConnectionId FROM integration.Connection WHERE Name = 'CON_FMD_SQL2016LIVE_M3CLOUD')
           DECLARE @DSId INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = 'DI_PRD_Staging' AND Type = 'ASQL_01')
           EXEC [integration].[sp_UpsertDataSource]
            @ConnectionId = @ConnId,
            @DataSourceId = @DSId,
            @Name = 'DI_PRD_Staging',
            @Namespace = 'M3CLOUD',
            @Type = 'ASQL_01',
            @Description = 'M3 Cloud DI_PRD_Staging on sql2016live via PowerBIGateway',
            @IsActive = 1""",
        "Register Data Source: DI_PRD_Staging")


if __name__ == "__main__":
    args = sys.argv[1:]

    print("Connecting to Fabric SQL Database...")
    print(f"  Server: {SERVER}")
    print(f"  Database: {DATABASE}")
    print(f"  Auth: ActiveDirectoryInteractive (browser login)")
    print()

    conn = connect()
    print("  Connected!\n")

    if "--check" in args:
        check_state(conn)
    elif "--query" in args:
        idx = args.index("--query")
        if idx + 1 < len(args):
            execute_query(conn, args[idx + 1], "Custom Query")
        else:
            print("ERROR: --query requires a SQL string argument")
    elif "--register" in args:
        register_m3cloud_connection(conn)
        print("\n>>> Checking final state...")
        check_state(conn)
    else:
        print("STEP 0: Current state")
        check_state(conn)
        print("\n" + "=" * 60)
        print("  STEP 1+2: Register Connection + Data Source")
        print("=" * 60)
        register_m3cloud_connection(conn)
        print("\n" + "=" * 60)
        print("  VERIFICATION")
        print("=" * 60)
        check_state(conn)

    conn.close()
    print("\nDone.")
