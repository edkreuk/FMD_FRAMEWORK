"""
FMD Operations Dashboard — Production Server
Serves the React dashboard (static files) and API from a single process.
Connects live to Fabric SQL DB + Fabric REST API via service principal.

Dependencies: pyodbc, ODBC Driver 18 for SQL Server
Everything else is Python stdlib.

Usage:
  Development:  python server.py                    (uses config.json next to this file)
  Production:   python server.py --config /path/to/config.json
"""
import json
import os
import struct
import sys
import logging
import mimetypes
import urllib.request
import urllib.parse
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

# ── Configuration ──

def _resolve_env_vars(obj):
    """Recursively resolve ${ENV_VAR} placeholders from environment variables."""
    if isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
        var_name = obj[2:-1]
        val = os.environ.get(var_name)
        if val is None:
            raise EnvironmentError(f'Required environment variable {var_name} is not set')
        return val
    elif isinstance(obj, dict):
        return {k: _resolve_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_resolve_env_vars(v) for v in obj]
    return obj


def load_config(config_path: str = None) -> dict:
    """Load configuration from JSON file, resolving ${ENV_VAR} placeholders."""
    if config_path is None:
        config_path = Path(__file__).parent / 'config.json'
    with open(config_path, 'r') as f:
        raw = json.load(f)
    return _resolve_env_vars(raw)

CONFIG = load_config(sys.argv[2] if len(sys.argv) > 2 and sys.argv[1] == '--config' else None)

TENANT_ID = CONFIG['fabric']['tenant_id']
CLIENT_ID = CONFIG['fabric']['client_id']
CLIENT_SECRET = CONFIG['fabric']['client_secret']
SQL_SERVER = CONFIG['sql']['server']
SQL_DATABASE = CONFIG['sql']['database']
SQL_DRIVER = CONFIG['sql'].get('driver', 'ODBC Driver 18 for SQL Server')
PORT = CONFIG['server']['port']
HOST = CONFIG['server'].get('host', '127.0.0.1')
STATIC_DIR = Path(__file__).parent / CONFIG['server'].get('static_dir', '../dist')

# ── Logging ──

log_cfg = CONFIG.get('logging', {})
log_file = log_cfg.get('file')
log_level = getattr(logging, log_cfg.get('level', 'INFO').upper(), logging.INFO)

handlers = [logging.StreamHandler()]
if log_file:
    log_path = Path(__file__).parent / log_file
    handlers.append(logging.FileHandler(log_path, encoding='utf-8'))

logging.basicConfig(
    level=log_level,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=handlers,
)
log = logging.getLogger('fmd-dashboard')


# ── Fabric Auth ──

_token_cache: dict = {}

def get_fabric_token(scope: str) -> str:
    """Get an OAuth2 token from Entra ID, with caching."""
    cached = _token_cache.get(scope)
    if cached and cached['expires'] > datetime.now().timestamp():
        return cached['token']

    token_url = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token'
    data = urllib.parse.urlencode({
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': scope,
        'grant_type': 'client_credentials',
    }).encode()
    req = urllib.request.Request(token_url, data=data,
                                headers={'Content-Type': 'application/x-www-form-urlencoded'})
    resp = urllib.request.urlopen(req)
    result = json.loads(resp.read())

    _token_cache[scope] = {
        'token': result['access_token'],
        'expires': datetime.now().timestamp() + result.get('expires_in', 3600) - 60,
    }
    log.info(f'Token refreshed for scope: {scope[:50]}...')
    return result['access_token']


# ── SQL Database ──

def get_sql_connection():
    """Connect to Fabric SQL Database using SP token."""
    import pyodbc
    token = get_fabric_token('https://database.windows.net/.default')
    token_bytes = token.encode('UTF-16-LE')
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    conn_str = (
        f'DRIVER={{{SQL_DRIVER}}};'
        f'SERVER={SQL_SERVER};'
        f'DATABASE={SQL_DATABASE};'
        f'Encrypt=yes;TrustServerCertificate=no;'
    )
    return pyodbc.connect(conn_str, attrs_before={1256: token_struct})


def query_sql(sql: str) -> list[dict]:
    """Execute SQL and return list of dicts."""
    conn = get_sql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        if not cursor.description:
            conn.commit()
            return []
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        return [{c: (str(v) if v is not None else None) for c, v in zip(cols, row)} for row in rows]
    finally:
        conn.close()


# ── Fabric REST API ──

def get_gateway_connections() -> list[dict]:
    """Get all gateway connections from Fabric REST API."""
    token = get_fabric_token('https://analysis.windows.net/powerbi/api/.default')
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    req = urllib.request.Request('https://api.fabric.microsoft.com/v1/connections', headers=headers)
    resp = urllib.request.urlopen(req)
    data = json.loads(resp.read())
    connections = []
    for c in data.get('value', []):
        path = c.get('connectionDetails', {}).get('path', '')
        parts = path.split(';', 1)
        connections.append({
            'id': c.get('id', ''),
            'displayName': c.get('displayName', ''),
            'server': parts[0] if parts else '',
            'database': parts[1] if len(parts) > 1 else '',
            'authType': c.get('credentialDetails', {}).get('credentialType', ''),
            'encryption': c.get('credentialDetails', {}).get('connectionEncryption', ''),
            'connectivityType': c.get('connectivityType', ''),
            'gatewayId': c.get('gatewayId', ''),
        })
    return connections


# ── Data Queries ──

def get_registered_connections() -> list[dict]:
    return query_sql(
        'SELECT ConnectionId, ConnectionGuid, Name, Type, IsActive '
        'FROM integration.Connection ORDER BY ConnectionId'
    )

def get_registered_datasources() -> list[dict]:
    return query_sql(
        'SELECT ds.DataSourceId, ds.Name, ds.Namespace, ds.Type, ds.Description, '
        'ds.IsActive, c.Name AS ConnectionName '
        'FROM integration.DataSource ds '
        'JOIN integration.Connection c ON ds.ConnectionId = c.ConnectionId '
        'ORDER BY ds.DataSourceId'
    )

def get_registered_entities() -> list[dict]:
    return query_sql(
        'SELECT le.LandingzoneEntityId, le.SourceSchema, le.SourceName, '
        'le.FileName, le.FilePath, le.FileType, le.IsIncremental, le.IsActive, '
        'le.SourceCustomSelect, le.IsIncrementalColumn, le.CustomNotebookName, '
        'ds.Name AS DataSourceName '
        'FROM integration.LandingzoneEntity le '
        'JOIN integration.DataSource ds ON le.DataSourceId = ds.DataSourceId '
        'ORDER BY le.LandingzoneEntityId'
    )

def get_pipeline_view() -> list[dict]:
    return query_sql('SELECT * FROM [execution].[vw_LoadSourceToLandingzone]')

def get_pipeline_executions() -> list[dict]:
    return query_sql(
        'SELECT * FROM logging.PipelineExecution ORDER BY 1 DESC'
    )

def get_copy_executions() -> list[dict]:
    return query_sql(
        'SELECT * FROM logging.CopyActivityExecution ORDER BY 1 DESC'
    )

def get_notebook_executions() -> list[dict]:
    return query_sql(
        'SELECT * FROM logging.NotebookExecution ORDER BY 1 DESC'
    )

def get_bronze_view() -> list[dict]:
    return query_sql('SELECT * FROM [execution].[vw_LoadToBronzeLayer]')

def get_silver_view() -> list[dict]:
    return query_sql('SELECT * FROM [execution].[vw_LoadToSilverLayer]')

def get_pipelines() -> list[dict]:
    return query_sql('SELECT * FROM integration.Pipeline ORDER BY PipelineId')

def get_workspaces() -> list[dict]:
    return query_sql('SELECT * FROM integration.Workspace ORDER BY WorkspaceId')

def get_lakehouses() -> list[dict]:
    return query_sql('SELECT * FROM integration.Lakehouse ORDER BY LakehouseId')

def get_bronze_entities() -> list[dict]:
    return query_sql('SELECT * FROM integration.BronzeLayerEntity ORDER BY BronzeLayerEntityId')

def get_silver_entities() -> list[dict]:
    return query_sql('SELECT * FROM integration.SilverLayerEntity ORDER BY SilverLayerEntityId')

def get_schema_info() -> list[dict]:
    """Discover all tables and views in the database."""
    return query_sql(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
        "FROM INFORMATION_SCHEMA.TABLES "
        "ORDER BY TABLE_SCHEMA, TABLE_TYPE, TABLE_NAME"
    )

def get_dashboard_stats() -> dict:
    """Aggregate stats for the dashboard from live metadata tables."""
    connections = query_sql('SELECT COUNT(*) AS cnt FROM integration.Connection WHERE IsActive = 1')
    datasources = query_sql('SELECT COUNT(*) AS cnt FROM integration.DataSource WHERE IsActive = 1')
    entities = query_sql('SELECT COUNT(*) AS cnt FROM integration.LandingzoneEntity WHERE IsActive = 1')
    lakehouses = query_sql('SELECT COUNT(*) AS cnt FROM integration.Lakehouse')

    # Entity counts by datasource (proxy for "by layer" until we have layer metadata)
    entity_breakdown = query_sql(
        'SELECT ds.Name AS DataSourceName, ds.Type AS DataSourceType, COUNT(*) AS EntityCount '
        'FROM integration.LandingzoneEntity le '
        'JOIN integration.DataSource ds ON le.DataSourceId = ds.DataSourceId '
        'WHERE le.IsActive = 1 '
        'GROUP BY ds.Name, ds.Type '
        'ORDER BY EntityCount DESC'
    )

    return {
        'activeConnections': int(connections[0]['cnt']) if connections else 0,
        'activeDataSources': int(datasources[0]['cnt']) if datasources else 0,
        'activeEntities': int(entities[0]['cnt']) if entities else 0,
        'lakehouses': int(lakehouses[0]['cnt']) if lakehouses else 0,
        'entityBreakdown': entity_breakdown,
    }


# ── Registration (POST handlers) ──

def register_connection(body: dict) -> dict:
    guid = body.get('connectionGuid', '')
    name = body.get('name', '')
    conn_type = body.get('type', 'SqlServer')
    sql = (
        f"EXEC [integration].[sp_UpsertConnection] "
        f"@ConnectionGuid = '{guid}', "
        f"@Name = '{name}', "
        f"@Type = '{conn_type}', "
        f"@IsActive = 1"
    )
    query_sql(sql)
    log.info(f'Connection registered: {name} ({guid[:8]}...)')
    return {'success': True, 'message': f'Connection {name} registered'}

def register_datasource(body: dict) -> dict:
    conn_name = body.get('connectionName', '')
    name = body.get('name', '')
    namespace = body.get('namespace', '')
    ds_type = body.get('type', 'ASQL_01')
    description = body.get('description', '')
    sql = (
        f"DECLARE @ConnId INT = (SELECT ConnectionId FROM integration.Connection WHERE Name = '{conn_name}') "
        f"DECLARE @DSId INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = '{name}' AND Type = '{ds_type}') "
        f"EXEC [integration].[sp_UpsertDataSource] "
        f"@ConnectionId = @ConnId, "
        f"@DataSourceId = @DSId, "
        f"@Name = '{name}', "
        f"@Namespace = '{namespace}', "
        f"@Type = '{ds_type}', "
        f"@Description = '{description}', "
        f"@IsActive = 1"
    )
    query_sql(sql)
    log.info(f'Data source registered: {name} (type={ds_type})')
    return {'success': True, 'message': f'Data source {name} registered'}

def register_entity(body: dict) -> dict:
    ds_name = body.get('dataSourceName', '')
    ds_type = body.get('dataSourceType', 'ASQL_01')
    schema = body.get('sourceSchema', 'dbo')
    table = body.get('sourceName', '')
    file_name = body.get('fileName', table)
    file_path = body.get('filePath', '')
    is_inc = 1 if body.get('isIncremental', False) else 0
    inc_col = body.get('incrementalColumn', '')
    custom_nb = body.get('customNotebookName', '')
    sql = (
        f"DECLARE @DSId INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = '{ds_name}' AND Type = '{ds_type}') "
        f"DECLARE @LHId INT = (SELECT TOP 1 LakehouseId FROM integration.Lakehouse WHERE Name = 'LH_DATA_LANDINGZONE') "
        f"DECLARE @LEId INT = (SELECT LandingzoneEntityId FROM integration.LandingzoneEntity WHERE SourceSchema = '{schema}' AND SourceName = '{table}') "
        f"EXEC [integration].[sp_UpsertLandingzoneEntity] "
        f"@LandingzoneEntityId = @LEId, "
        f"@DataSourceId = @DSId, "
        f"@LakehouseId = @LHId, "
        f"@SourceSchema = '{schema}', "
        f"@SourceName = '{table}', "
        f"@SourceCustomSelect = '', "
        f"@FileName = '{file_name}', "
        f"@FilePath = '{file_path}', "
        f"@FileType = 'parquet', "
        f"@IsIncremental = {is_inc}, "
        f"@IsIncrementalColumn = '{inc_col}', "
        f"@IsActive = 1, "
        f"@CustomNotebookName = '{custom_nb}'"
    )
    query_sql(sql)
    log.info(f'Entity registered: {schema}.{table}')
    return {'success': True, 'message': f'Entity {schema}.{table} registered'}


# ── Static File Serving ──

MIME_TYPES = {
    '.html': 'text/html',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.ttf': 'font/ttf',
    '.map': 'application/json',
}

def serve_static(handler, url_path: str) -> bool:
    """Serve a static file from STATIC_DIR. Returns True if served, False if not found."""
    if not STATIC_DIR.exists():
        return False

    # Clean URL path
    clean = url_path.split('?')[0].split('#')[0]
    if clean == '/':
        clean = '/index.html'

    file_path = STATIC_DIR / clean.lstrip('/')

    # Security: prevent directory traversal
    try:
        file_path.resolve().relative_to(STATIC_DIR.resolve())
    except ValueError:
        return False

    if file_path.is_file():
        ext = file_path.suffix.lower()
        content_type = MIME_TYPES.get(ext, mimetypes.guess_type(str(file_path))[0] or 'application/octet-stream')
        handler.send_response(200)
        handler.send_header('Content-Type', content_type)
        # Cache static assets (hashed filenames) for 1 year, html for 0
        if ext in ('.js', '.css', '.woff', '.woff2', '.ttf', '.png', '.jpg', '.svg'):
            handler.send_header('Cache-Control', 'public, max-age=31536000, immutable')
        else:
            handler.send_header('Cache-Control', 'no-cache')
        handler.end_headers()
        handler.wfile.write(file_path.read_bytes())
        return True

    # SPA fallback: serve index.html for client-side routing
    index = STATIC_DIR / 'index.html'
    if index.is_file():
        handler.send_response(200)
        handler.send_header('Content-Type', 'text/html')
        handler.send_header('Cache-Control', 'no-cache')
        handler.end_headers()
        handler.wfile.write(index.read_bytes())
        return True

    return False


# ── HTTP Handler ──

class DashboardHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _json_response(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self._cors()
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def _error_response(self, message, status=500):
        log.error(f'{self.path} → {status}: {message}')
        self._json_response({'error': message}, status)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        try:
            # API routes
            if self.path == '/api/gateway-connections':
                self._json_response(get_gateway_connections())
            elif self.path == '/api/connections':
                self._json_response(get_registered_connections())
            elif self.path == '/api/datasources':
                self._json_response(get_registered_datasources())
            elif self.path == '/api/entities':
                self._json_response(get_registered_entities())
            elif self.path == '/api/pipeline-view':
                self._json_response(get_pipeline_view())
            elif self.path == '/api/bronze-view':
                self._json_response(get_bronze_view())
            elif self.path == '/api/silver-view':
                self._json_response(get_silver_view())
            elif self.path == '/api/pipelines':
                self._json_response(get_pipelines())
            elif self.path == '/api/workspaces':
                self._json_response(get_workspaces())
            elif self.path == '/api/lakehouses':
                self._json_response(get_lakehouses())
            elif self.path == '/api/bronze-entities':
                self._json_response(get_bronze_entities())
            elif self.path == '/api/silver-entities':
                self._json_response(get_silver_entities())
            elif self.path == '/api/pipeline-executions':
                self._json_response(get_pipeline_executions())
            elif self.path == '/api/copy-executions':
                self._json_response(get_copy_executions())
            elif self.path == '/api/notebook-executions':
                self._json_response(get_notebook_executions())
            elif self.path == '/api/schema':
                self._json_response(get_schema_info())
            elif self.path == '/api/stats':
                self._json_response(get_dashboard_stats())
            elif self.path == '/api/health':
                self._json_response({
                    'status': 'ok',
                    'sql': SQL_SERVER,
                    'mode': 'production' if STATIC_DIR.exists() else 'api-only',
                    'static_dir': str(STATIC_DIR),
                })
            elif self.path.startswith('/api/'):
                self._error_response('Not found', 404)
            else:
                # Static file serving (production mode)
                if not serve_static(self, self.path):
                    self._error_response('Not found', 404)
        except Exception as e:
            log.exception(f'Error handling GET {self.path}')
            self._error_response(str(e))

    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(content_length)) if content_length else {}

            if self.path == '/api/connections':
                self._json_response(register_connection(body), 201)
            elif self.path == '/api/datasources':
                self._json_response(register_datasource(body), 201)
            elif self.path == '/api/entities':
                self._json_response(register_entity(body), 201)
            else:
                self._error_response('Not found', 404)
        except Exception as e:
            log.exception(f'Error handling POST {self.path}')
            self._error_response(str(e))

    def log_message(self, format, *args):
        log.debug(f'{self.client_address[0]} {args[0]}')


# ── Entry Point ──

if __name__ == '__main__':
    mode = 'production' if STATIC_DIR.exists() else 'api-only'

    log.info('=' * 60)
    log.info('FMD Operations Dashboard')
    log.info('=' * 60)
    log.info(f'  Mode:       {mode}')
    log.info(f'  Server:     http://{HOST}:{PORT}')
    log.info(f'  SQL:        {SQL_SERVER[:40]}...')
    log.info(f'  Database:   {SQL_DATABASE[:40]}...')
    if mode == 'production':
        log.info(f'  Static:     {STATIC_DIR}')
    log.info(f'  Log file:   {log_cfg.get("file", "console only")}')
    log.info('')
    log.info('API Endpoints:')
    log.info('  GET  /api/gateway-connections')
    log.info('  GET  /api/connections')
    log.info('  GET  /api/datasources')
    log.info('  GET  /api/entities')
    log.info('  GET  /api/pipeline-view')
    log.info('  GET  /api/health')
    log.info('  POST /api/connections')
    log.info('  POST /api/datasources')
    log.info('  POST /api/entities')
    if mode == 'production':
        log.info('')
        log.info('  /*   → Static files from dist/')
    log.info('=' * 60)

    server = HTTPServer((HOST, PORT), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info('Shutting down.')
        server.server_close()
