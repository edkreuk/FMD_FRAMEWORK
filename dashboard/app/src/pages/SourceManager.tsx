import { useState, useEffect, useCallback } from 'react';
import {
  Database,
  Server,
  Plus,
  CheckCircle,
  Circle,
  ChevronRight,
  ChevronDown,
  Cable,
  TableProperties,
  FolderInput,
  Copy,
  RefreshCw,
  Search,
  ExternalLink,
  Loader2,
  AlertTriangle,
  Info,
} from 'lucide-react';

// ── Types matching API responses ──

interface GatewayConnection {
  id: string;
  displayName: string;
  server: string;
  database: string;
  authType: string;
  encryption: string;
  connectivityType: string;
  gatewayId: string;
}

interface RegisteredConnection {
  ConnectionId: string;
  ConnectionGuid: string;
  Name: string;
  Type: string;
  IsActive: string;
}

interface RegisteredDataSource {
  DataSourceId: string;
  Name: string;
  Namespace: string;
  Type: string;
  Description: string;
  ConnectionName: string;
  IsActive: string;
}

interface RegisteredEntity {
  LandingzoneEntityId: string;
  SourceSchema: string;
  SourceName: string;
  FileName: string;
  FilePath: string;
  FileType: string;
  IsIncremental: string;
  IsActive: string;
  DataSourceName: string;
}

const DATA_SOURCE_TYPES = [
  { value: 'ASQL_01', label: 'Azure SQL / On-Prem SQL', pipeline: 'PL_FMD_LDZ_COPY_FROM_ASQL_01' },
  { value: 'ONELAKE_TABLES_01', label: 'OneLake Tables', pipeline: 'PL_FMD_LDZ_COPY_FROM_ONELAKE_TABLES_01' },
  { value: 'ONELAKE_FILES_01', label: 'OneLake Files', pipeline: 'PL_FMD_LDZ_COPY_FROM_ONELAKE_FILES_01' },
  { value: 'ADLS_01', label: 'Azure Data Lake', pipeline: 'PL_FMD_LDZ_COPY_FROM_ADLS_01' },
  { value: 'FTP_01', label: 'FTP', pipeline: 'PL_FMD_LDZ_COPY_FROM_FTP_01' },
  { value: 'SFTP_01', label: 'SFTP', pipeline: 'PL_FMD_LDZ_COPY_FROM_SFTP_01' },
  { value: 'ORACLE_01', label: 'Oracle', pipeline: 'PL_FMD_LDZ_COPY_FROM_ORACLE_01' },
  { value: 'NOTEBOOK', label: 'Custom Notebook', pipeline: 'PL_FMD_LDZ_COPY_FROM_CUSTOM_NB' },
];

export default function SourceManager() {
  // ── Data state (live from API) ──
  const [gatewayConnections, setGatewayConnections] = useState<GatewayConnection[]>([]);
  const [registeredConnections, setRegisteredConnections] = useState<RegisteredConnection[]>([]);
  const [registeredDataSources, setRegisteredDataSources] = useState<RegisteredDataSource[]>([]);
  const [registeredEntities, setRegisteredEntities] = useState<RegisteredEntity[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionStatus, setActionStatus] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // ── UI state ──
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedConnection, setExpandedConnection] = useState<string | null>(null);
  const [showAddSource, setShowAddSource] = useState(false);
  const [showAddEntity, setShowAddEntity] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  // ── Form states ──
  const [newSource, setNewSource] = useState({
    connectionName: '',
    name: '',
    namespace: '',
    type: 'ASQL_01',
    description: '',
  });

  const [newEntity, setNewEntity] = useState({
    dataSourceName: '',
    dataSourceType: 'ASQL_01',
    sourceSchema: 'dbo',
    sourceName: '',
    fileName: '',
    filePath: 'm3cloud',
    isIncremental: false,
    incrementalColumn: '',
  });

  // ── Data loading ──
  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [gwRes, connRes, dsRes, entRes] = await Promise.all([
        fetch('/api/gateway-connections'),
        fetch('/api/connections'),
        fetch('/api/datasources'),
        fetch('/api/entities'),
      ]);

      if (!gwRes.ok || !connRes.ok || !dsRes.ok || !entRes.ok) {
        throw new Error('API server not responding. Run: python dashboard/app/api/server.py');
      }

      const [gw, conn, ds, ent] = await Promise.all([
        gwRes.json(), connRes.json(), dsRes.json(), entRes.json(),
      ]);

      setGatewayConnections(gw);
      setRegisteredConnections(conn);
      setRegisteredDataSources(ds);
      setRegisteredEntities(ent);

      // Set default data source for entity form
      const sqlSources = (ds as RegisteredDataSource[]).filter(d => d.Type === 'ASQL_01');
      if (sqlSources.length > 0 && !newEntity.dataSourceName) {
        setNewEntity(prev => ({ ...prev, dataSourceName: sqlSources[0].Name }));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  // ── Check if a gateway connection is registered ──
  const isRegistered = (gwConn: GatewayConnection): RegisteredConnection | undefined => {
    return registeredConnections.find(
      rc => rc.ConnectionGuid.toLowerCase() === gwConn.id.toLowerCase()
    );
  };

  // ── Get data sources for a registered connection ──
  const getDataSourcesForConnection = (connName: string): RegisteredDataSource[] => {
    return registeredDataSources.filter(ds => ds.ConnectionName === connName);
  };

  // ── Actions ──
  const registerConnection = async (gwConn: GatewayConnection) => {
    setSubmitting(true);
    setActionStatus(null);
    // Generate FMD name from server + database
    const serverShort = gwConn.server.split('.')[0].toUpperCase().replace(/-/g, '');
    const dbShort = gwConn.database.toUpperCase().replace(/[^A-Z0-9]/g, '');
    const fmdName = `CON_FMD_${serverShort}_${dbShort}`;

    try {
      const res = await fetch('/api/connections', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connectionGuid: gwConn.id,
          name: fmdName,
          type: 'SqlServer',
        }),
      });
      const data = await res.json();
      if (res.ok) {
        setActionStatus({ type: 'success', message: data.message || `Registered ${fmdName}` });
        await loadData();
      } else {
        setActionStatus({ type: 'error', message: data.error || 'Registration failed' });
      }
    } catch (e) {
      setActionStatus({ type: 'error', message: e instanceof Error ? e.message : 'Network error' });
    } finally {
      setSubmitting(false);
    }
  };

  const registerDataSource = async () => {
    setSubmitting(true);
    setActionStatus(null);
    try {
      const res = await fetch('/api/datasources', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newSource),
      });
      const data = await res.json();
      if (res.ok) {
        setActionStatus({ type: 'success', message: data.message || 'Data source registered' });
        setNewSource({ connectionName: '', name: '', namespace: '', type: 'ASQL_01', description: '' });
        setShowAddSource(false);
        await loadData();
      } else {
        setActionStatus({ type: 'error', message: data.error || 'Registration failed' });
      }
    } catch (e) {
      setActionStatus({ type: 'error', message: e instanceof Error ? e.message : 'Network error' });
    } finally {
      setSubmitting(false);
    }
  };

  const registerEntity = async () => {
    setSubmitting(true);
    setActionStatus(null);
    try {
      const res = await fetch('/api/entities', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newEntity),
      });
      const data = await res.json();
      if (res.ok) {
        setActionStatus({ type: 'success', message: data.message || 'Entity registered' });
        setNewEntity({ dataSourceName: '', dataSourceType: 'ASQL_01', sourceSchema: 'dbo', sourceName: '', fileName: '', filePath: 'm3cloud', isIncremental: false, incrementalColumn: '' });
        setShowAddEntity(false);
        await loadData();
      } else {
        setActionStatus({ type: 'error', message: data.error || 'Registration failed' });
      }
    } catch (e) {
      setActionStatus({ type: 'error', message: e instanceof Error ? e.message : 'Network error' });
    } finally {
      setSubmitting(false);
    }
  };

  // ── Derived data ──
  const filteredConnections = gatewayConnections.filter(c =>
    c.displayName.toLowerCase().includes(searchTerm.toLowerCase()) ||
    c.server.toLowerCase().includes(searchTerm.toLowerCase()) ||
    c.database.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const registeredCount = gatewayConnections.filter(c => isRegistered(c)).length;
  const externalSources = registeredDataSources.filter(ds => ds.Type === 'ASQL_01');
  const sqlConnections = registeredConnections.filter(c => c.Type === 'SqlServer');

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  // ── Loading / Error states ──
  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin text-primary mx-auto mb-4" />
          <p className="text-muted-foreground">Connecting to Fabric SQL Database...</p>
          <p className="text-xs text-muted-foreground mt-1">Loading gateway connections and registered sources</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center max-w-md">
          <AlertTriangle className="h-8 w-8 text-amber-500 mx-auto mb-4" />
          <p className="text-foreground font-medium mb-2">API Server Not Running</p>
          <p className="text-sm text-muted-foreground mb-4">{error}</p>
          <div className="bg-muted rounded-lg p-4 text-left">
            <p className="text-xs text-muted-foreground mb-2">Start the API server:</p>
            <code className="text-sm font-mono text-foreground">python dashboard/app/api/server.py</code>
          </div>
          <button onClick={loadData} className="mt-4 flex items-center gap-2 px-4 py-2 text-sm bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors mx-auto">
            <RefreshCw className="h-4 w-4" />
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-display font-bold tracking-tight text-foreground">Source Manager</h1>
        <p className="text-muted-foreground mt-1">
          Register gateway connections, configure data sources, and manage landing zone entities
        </p>
      </div>

      {/* Action Status Banner */}
      {actionStatus && (
        <div className={`rounded-lg p-4 flex items-center justify-between ${
          actionStatus.type === 'success'
            ? 'bg-emerald-50 dark:bg-emerald-950/20 border border-emerald-200 dark:border-emerald-800'
            : 'bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800'
        }`}>
          <div className="flex items-center gap-3">
            {actionStatus.type === 'success' ? (
              <CheckCircle className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />
            ) : (
              <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" />
            )}
            <p className={`text-sm font-medium ${
              actionStatus.type === 'success' ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-300'
            }`}>{actionStatus.message}</p>
          </div>
          <button onClick={() => setActionStatus(null)} className="text-muted-foreground hover:text-foreground text-sm">Dismiss</button>
        </div>
      )}

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-card rounded-xl border border-border p-5">
          <div className="flex items-center gap-3 mb-3">
            <div className="w-10 h-10 bg-blue-100 dark:bg-blue-950/30 rounded-lg flex items-center justify-center">
              <Cable className="w-5 h-5 text-blue-600 dark:text-blue-400" />
            </div>
          </div>
          <p className="text-3xl font-bold text-foreground">{gatewayConnections.length}</p>
          <p className="text-sm text-muted-foreground mt-1">Gateway Connections</p>
          <p className="text-xs text-muted-foreground mt-2">
            <span className="text-emerald-600 dark:text-emerald-400 font-medium">{registeredCount} registered</span> · {gatewayConnections.length - registeredCount} available
          </p>
        </div>

        <div className="bg-card rounded-xl border border-border p-5">
          <div className="flex items-center gap-3 mb-3">
            <div className="w-10 h-10 bg-purple-100 dark:bg-purple-950/30 rounded-lg flex items-center justify-center">
              <Database className="w-5 h-5 text-purple-600 dark:text-purple-400" />
            </div>
          </div>
          <p className="text-3xl font-bold text-foreground">{registeredDataSources.length}</p>
          <p className="text-sm text-muted-foreground mt-1">Data Sources</p>
          <p className="text-xs text-muted-foreground mt-2">
            <span className="text-purple-600 dark:text-purple-400 font-medium">{externalSources.length} external SQL</span> · {registeredDataSources.length - externalSources.length} internal
          </p>
        </div>

        <div className="bg-card rounded-xl border border-border p-5">
          <div className="flex items-center gap-3 mb-3">
            <div className="w-10 h-10 bg-amber-100 dark:bg-amber-950/30 rounded-lg flex items-center justify-center">
              <TableProperties className="w-5 h-5 text-amber-600 dark:text-amber-400" />
            </div>
          </div>
          <p className="text-3xl font-bold text-foreground">{registeredEntities.length}</p>
          <p className="text-sm text-muted-foreground mt-1">Landing Zone Entities</p>
          <p className="text-xs text-muted-foreground mt-2">Tables configured for ingestion</p>
        </div>

        <div className="bg-card rounded-xl border border-border p-5">
          <div className="flex items-center gap-3 mb-3">
            <div className="w-10 h-10 bg-emerald-100 dark:bg-emerald-950/30 rounded-lg flex items-center justify-center">
              <FolderInput className="w-5 h-5 text-emerald-600 dark:text-emerald-400" />
            </div>
          </div>
          <p className="text-3xl font-bold text-foreground">{sqlConnections.length}</p>
          <p className="text-sm text-muted-foreground mt-1">SQL Connections</p>
          <p className="text-xs text-muted-foreground mt-2">On-prem via gateway</p>
        </div>
      </div>

      {/* Gateway Connections */}
      <div className="bg-card rounded-xl border border-border p-6">
        <div className="flex items-center justify-between mb-5">
          <div>
            <h2 className="text-lg font-semibold text-foreground">Gateway Connections</h2>
            <p className="text-sm text-muted-foreground mt-0.5">
              All on-premises SQL connections via PowerBIGateway — live from Fabric API
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <input
                type="text"
                placeholder="Search connections..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-9 pr-4 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground w-64"
              />
            </div>
            <button
              onClick={loadData}
              className="flex items-center gap-2 px-3 py-2 text-sm bg-muted hover:bg-muted/80 border border-border rounded-lg text-muted-foreground transition-colors"
            >
              <RefreshCw className="h-4 w-4" />
              Refresh
            </button>
          </div>
        </div>

        <div className="space-y-2">
          {filteredConnections.map((conn) => {
            const isExpanded = expandedConnection === conn.id;
            const regConn = isRegistered(conn);
            const connDataSources = regConn ? getDataSourcesForConnection(regConn.Name) : [];
            return (
              <div key={conn.id} className="border border-border rounded-lg overflow-hidden">
                <button
                  onClick={() => setExpandedConnection(isExpanded ? null : conn.id)}
                  className="w-full flex items-center justify-between p-4 hover:bg-muted/50 transition-colors text-left"
                >
                  <div className="flex items-center gap-4">
                    <div className="flex items-center gap-2">
                      {regConn ? (
                        <CheckCircle className="h-5 w-5 text-emerald-500" />
                      ) : (
                        <Circle className="h-5 w-5 text-muted-foreground/40" />
                      )}
                    </div>
                    <div className="w-10 h-10 bg-muted rounded-lg border border-border flex items-center justify-center">
                      <Server className="w-5 h-5 text-muted-foreground" />
                    </div>
                    <div>
                      <p className="font-medium text-foreground">{conn.displayName}</p>
                      <p className="text-sm text-muted-foreground font-mono">{conn.server} → {conn.database}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <div className="text-right">
                      <span className={`text-xs px-2 py-1 rounded-full font-medium ${
                        regConn
                          ? 'bg-emerald-50 dark:bg-emerald-950/20 text-emerald-600 dark:text-emerald-400'
                          : 'bg-muted text-muted-foreground'
                      }`}>
                        {regConn ? 'Registered' : 'Available'}
                      </span>
                    </div>
                    <span className="text-xs text-muted-foreground bg-muted px-2 py-1 rounded">{conn.authType}</span>
                    {isExpanded ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
                  </div>
                </button>

                {isExpanded && (
                  <div className="border-t border-border bg-muted/30 p-5">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                      <div>
                        <p className="text-muted-foreground text-xs uppercase tracking-wider mb-1">Connection GUID</p>
                        <div className="flex items-center gap-2">
                          <code className="text-xs font-mono text-foreground bg-muted px-2 py-1 rounded border border-border break-all">{conn.id}</code>
                          <button onClick={() => copyToClipboard(conn.id)} className="text-muted-foreground hover:text-foreground transition-colors shrink-0">
                            <Copy className="h-3.5 w-3.5" />
                          </button>
                        </div>
                      </div>
                      <div>
                        <p className="text-muted-foreground text-xs uppercase tracking-wider mb-1">Server</p>
                        <p className="font-mono text-foreground">{conn.server}</p>
                      </div>
                      <div>
                        <p className="text-muted-foreground text-xs uppercase tracking-wider mb-1">Database</p>
                        <p className="font-mono text-foreground">{conn.database}</p>
                      </div>
                      <div>
                        <p className="text-muted-foreground text-xs uppercase tracking-wider mb-1">Encryption</p>
                        <p className="text-foreground">{conn.encryption}</p>
                      </div>
                    </div>

                    {regConn && (
                      <div className="mt-4 pt-4 border-t border-border">
                        <div className="flex items-center gap-2 mb-2">
                          <p className="text-muted-foreground text-xs uppercase tracking-wider">FMD Framework Name</p>
                          <div className="relative group">
                            <Info className="h-3.5 w-3.5 text-muted-foreground/60 cursor-help" />
                            <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 w-72 p-3 bg-popover text-popover-foreground text-xs rounded-lg border border-border shadow-lg opacity-0 pointer-events-none group-hover:opacity-100 group-hover:pointer-events-auto transition-opacity z-50">
                              <p className="font-medium mb-1">Naming convention: CON_FMD_{'{SERVER}'}_{'{SOURCE}'}</p>
                              <ul className="space-y-1 text-muted-foreground">
                                <li><span className="font-mono text-foreground">CON_FMD</span> — prefix for all framework connections</li>
                                <li><span className="font-mono text-foreground">{'{SERVER}'}</span> — the SQL Server hostname</li>
                                <li><span className="font-mono text-foreground">{'{SOURCE}'}</span> — the Fabric display name (spaces removed)</li>
                              </ul>
                              <p className="mt-2 text-muted-foreground">This is the identifier used in pipeline expressions like <span className="font-mono">@item().ConnectionGuid</span></p>
                            </div>
                          </div>
                        </div>
                        <code className="text-sm font-mono text-emerald-600 dark:text-emerald-400">{regConn.Name}</code>

                        {connDataSources.length > 0 && (
                          <div className="mt-3">
                            <p className="text-muted-foreground text-xs uppercase tracking-wider mb-2">Linked Data Sources</p>
                            <div className="space-y-2">
                              {connDataSources.map(ds => (
                                <div key={ds.DataSourceId} className="flex items-center gap-3 bg-card border border-border rounded-lg p-3">
                                  <Database className="h-4 w-4 text-purple-500" />
                                  <div>
                                    <p className="text-sm font-medium text-foreground">{ds.Name}</p>
                                    <p className="text-xs text-muted-foreground">Namespace: {ds.Namespace} · Type: {ds.Type} · {ds.Description}</p>
                                  </div>
                                  <span className={`ml-auto text-xs px-2 py-0.5 rounded-full ${
                                    ds.IsActive === 'True'
                                      ? 'bg-emerald-50 dark:bg-emerald-950/20 text-emerald-600 dark:text-emerald-400'
                                      : 'bg-red-50 dark:bg-red-950/20 text-red-600 dark:text-red-400'
                                  }`}>{ds.IsActive === 'True' ? 'Active' : 'Inactive'}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    )}

                    {!regConn && (
                      <div className="mt-4 pt-4 border-t border-border">
                        <p className="text-sm text-muted-foreground mb-3">This connection is available in the Fabric gateway but not yet registered in the FMD framework.</p>
                        <div className="flex items-center gap-3">
                          <button
                            onClick={() => registerConnection(conn)}
                            disabled={submitting}
                            className="flex items-center gap-2 px-4 py-2 text-sm bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors font-medium disabled:opacity-50"
                          >
                            {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
                            Register Connection
                          </button>
                          <a
                            href="https://app.fabric.microsoft.com/connections"
                            target="_blank"
                            rel="noreferrer"
                            className="flex items-center gap-2 px-4 py-2 text-sm bg-muted hover:bg-muted/80 border border-border rounded-lg text-muted-foreground transition-colors"
                          >
                            <ExternalLink className="h-3.5 w-3.5" />
                            Open in Fabric
                          </a>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Add Data Source Form */}
      <div className="bg-card rounded-xl border border-border p-6">
        <button
          onClick={() => setShowAddSource(!showAddSource)}
          className="w-full flex items-center justify-between"
        >
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-purple-100 dark:bg-purple-950/30 rounded-lg flex items-center justify-center">
              <Plus className="w-5 h-5 text-purple-600 dark:text-purple-400" />
            </div>
            <div className="text-left">
              <h2 className="text-lg font-semibold text-foreground">Add Data Source</h2>
              <p className="text-sm text-muted-foreground">Link a registered connection to a specific database</p>
            </div>
          </div>
          {showAddSource ? <ChevronDown className="h-5 w-5 text-muted-foreground" /> : <ChevronRight className="h-5 w-5 text-muted-foreground" />}
        </button>

        {showAddSource && (
          <div className="mt-6 pt-6 border-t border-border">
            <div className="bg-muted/50 rounded-lg p-4 mb-6 text-sm text-muted-foreground">
              <p><strong className="text-foreground">How it works:</strong> A Data Source links a registered Connection to a specific database. The <code className="bg-muted px-1 rounded">Type</code> field determines which pipeline runs. For SQL Server connections, use <code className="bg-muted px-1 rounded">ASQL_01</code>.</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Connection</label>
                <select
                  value={newSource.connectionName}
                  onChange={(e) => setNewSource({ ...newSource, connectionName: e.target.value })}
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground"
                >
                  <option value="">Select a registered connection...</option>
                  {registeredConnections.filter(c => c.Type === 'SqlServer').map(c => (
                    <option key={c.ConnectionId} value={c.Name}>{c.Name}</option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Database Name</label>
                <input
                  type="text"
                  value={newSource.name}
                  onChange={(e) => setNewSource({ ...newSource, name: e.target.value })}
                  placeholder="e.g. DI_PRD_Staging"
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                />
                <p className="text-xs text-muted-foreground mt-1">This becomes <code>@item().DatasourceName</code> in the pipeline</p>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Namespace</label>
                <input
                  type="text"
                  value={newSource.namespace}
                  onChange={(e) => setNewSource({ ...newSource, namespace: e.target.value })}
                  placeholder="e.g. M3CLOUD"
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                />
                <p className="text-xs text-muted-foreground mt-1">Logical grouping label for organization</p>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Source Type</label>
                <select
                  value={newSource.type}
                  onChange={(e) => setNewSource({ ...newSource, type: e.target.value })}
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground"
                >
                  {DATA_SOURCE_TYPES.map(t => (
                    <option key={t.value} value={t.value}>{t.label} ({t.value})</option>
                  ))}
                </select>
                <p className="text-xs text-muted-foreground mt-1">Routes to: {DATA_SOURCE_TYPES.find(t => t.value === newSource.type)?.pipeline}</p>
              </div>

              <div className="md:col-span-2">
                <label className="block text-sm font-medium text-foreground mb-1.5">Description</label>
                <input
                  type="text"
                  value={newSource.description}
                  onChange={(e) => setNewSource({ ...newSource, description: e.target.value })}
                  placeholder="e.g. M3 Cloud DI_PRD_Staging on sql2016live via PowerBIGateway"
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                />
              </div>
            </div>

            <div className="mt-4 flex items-center gap-3">
              <button
                onClick={registerDataSource}
                disabled={submitting || !newSource.connectionName || !newSource.name}
                className="flex items-center gap-2 px-4 py-2 text-sm bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors font-medium disabled:opacity-50"
              >
                {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
                Register Data Source
              </button>
              <button
                onClick={() => setShowAddSource(false)}
                className="px-4 py-2 text-sm bg-muted hover:bg-muted/80 border border-border rounded-lg text-muted-foreground transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Add Entity Form */}
      <div className="bg-card rounded-xl border border-border p-6">
        <button
          onClick={() => setShowAddEntity(!showAddEntity)}
          className="w-full flex items-center justify-between"
        >
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-amber-100 dark:bg-amber-950/30 rounded-lg flex items-center justify-center">
              <Plus className="w-5 h-5 text-amber-600 dark:text-amber-400" />
            </div>
            <div className="text-left">
              <h2 className="text-lg font-semibold text-foreground">Add Landing Zone Entity</h2>
              <p className="text-sm text-muted-foreground">Register a source table for ingestion into the lakehouse</p>
            </div>
          </div>
          {showAddEntity ? <ChevronDown className="h-5 w-5 text-muted-foreground" /> : <ChevronRight className="h-5 w-5 text-muted-foreground" />}
        </button>

        {showAddEntity && (
          <div className="mt-6 pt-6 border-t border-border">
            <div className="bg-muted/50 rounded-lg p-4 mb-6 text-sm text-muted-foreground">
              <p><strong className="text-foreground">How it works:</strong> Each entity = one source table. The pipeline loops through all active entities for a data source and copies each as a parquet file into <code className="bg-muted px-1 rounded">LH_DATA_LANDINGZONE</code>.</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Data Source</label>
                <select
                  value={newEntity.dataSourceName}
                  onChange={(e) => {
                    const ds = registeredDataSources.find(d => d.Name === e.target.value);
                    setNewEntity({ ...newEntity, dataSourceName: e.target.value, dataSourceType: ds?.Type || 'ASQL_01' });
                  }}
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground"
                >
                  <option value="">Select a data source...</option>
                  {registeredDataSources.filter(ds => ds.Type === 'ASQL_01').map(ds => (
                    <option key={ds.DataSourceId} value={ds.Name}>{ds.Name} ({ds.Namespace})</option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Source Schema</label>
                <input
                  type="text"
                  value={newEntity.sourceSchema}
                  onChange={(e) => setNewEntity({ ...newEntity, sourceSchema: e.target.value })}
                  placeholder="dbo"
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Source Table Name</label>
                <input
                  type="text"
                  value={newEntity.sourceName}
                  onChange={(e) => {
                    const val = e.target.value;
                    setNewEntity({ ...newEntity, sourceName: val, fileName: val });
                  }}
                  placeholder="e.g. CustomerMaster"
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Output File Name</label>
                <input
                  type="text"
                  value={newEntity.fileName}
                  onChange={(e) => setNewEntity({ ...newEntity, fileName: e.target.value })}
                  placeholder="Auto-filled from table name"
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                />
                <p className="text-xs text-muted-foreground mt-1">Parquet file name in lakehouse (no extension)</p>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Folder Path</label>
                <input
                  type="text"
                  value={newEntity.filePath}
                  onChange={(e) => setNewEntity({ ...newEntity, filePath: e.target.value })}
                  placeholder="m3cloud"
                  className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                />
                <p className="text-xs text-muted-foreground mt-1">Folder in lakehouse Files area</p>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1.5">Load Type</label>
                <div className="flex items-center gap-4 mt-2">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="radio"
                      checked={!newEntity.isIncremental}
                      onChange={() => setNewEntity({ ...newEntity, isIncremental: false, incrementalColumn: '' })}
                      className="text-primary"
                    />
                    <span className="text-sm text-foreground">Full Load</span>
                  </label>
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="radio"
                      checked={newEntity.isIncremental}
                      onChange={() => setNewEntity({ ...newEntity, isIncremental: true })}
                      className="text-primary"
                    />
                    <span className="text-sm text-foreground">Incremental</span>
                  </label>
                </div>
              </div>

              {newEntity.isIncremental && (
                <div className="md:col-span-3">
                  <label className="block text-sm font-medium text-foreground mb-1.5">Incremental Column (Watermark)</label>
                  <input
                    type="text"
                    value={newEntity.incrementalColumn}
                    onChange={(e) => setNewEntity({ ...newEntity, incrementalColumn: e.target.value })}
                    placeholder="e.g. ModifiedDate"
                    className="w-full px-3 py-2 text-sm bg-muted border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground md:max-w-sm"
                  />
                </div>
              )}
            </div>

            {/* SQL Preview */}
            {newEntity.sourceName && (
              <div className="mt-4 bg-muted rounded-lg p-4 border border-border">
                <p className="text-xs text-muted-foreground uppercase tracking-wider mb-2">SQL that will execute</p>
                <pre className="text-xs font-mono text-foreground whitespace-pre-wrap">{`EXEC [integration].[sp_UpsertLandingzoneEntity]
    @SourceSchema = '${newEntity.sourceSchema}',
    @SourceName = '${newEntity.sourceName}',
    @FileName = '${newEntity.fileName}',
    @FilePath = '${newEntity.filePath}',
    @FileType = 'parquet',
    @IsIncremental = ${newEntity.isIncremental ? 1 : 0},
    @IsIncrementalColumn = '${newEntity.incrementalColumn}',
    @IsActive = 1;`}</pre>
              </div>
            )}

            <div className="mt-4 flex items-center gap-3">
              <button
                onClick={registerEntity}
                disabled={submitting || !newEntity.dataSourceName || !newEntity.sourceName}
                className="flex items-center gap-2 px-4 py-2 text-sm bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors font-medium disabled:opacity-50"
              >
                {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
                Register Entity
              </button>
              <button
                onClick={() => setShowAddEntity(false)}
                className="px-4 py-2 text-sm bg-muted hover:bg-muted/80 border border-border rounded-lg text-muted-foreground transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Registered Entities Table */}
      <div className="bg-card rounded-xl border border-border p-6">
        <h2 className="text-lg font-semibold text-foreground mb-4">Registered Entities</h2>
        {registeredEntities.length === 0 ? (
          <p className="text-sm text-muted-foreground py-8 text-center">No entities registered yet. Add one above to start ingesting data.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-3 text-xs uppercase tracking-wider text-muted-foreground font-medium">Source</th>
                  <th className="text-left py-3 px-3 text-xs uppercase tracking-wider text-muted-foreground font-medium">Schema.Table</th>
                  <th className="text-left py-3 px-3 text-xs uppercase tracking-wider text-muted-foreground font-medium">Output File</th>
                  <th className="text-left py-3 px-3 text-xs uppercase tracking-wider text-muted-foreground font-medium">Path</th>
                  <th className="text-left py-3 px-3 text-xs uppercase tracking-wider text-muted-foreground font-medium">Load Type</th>
                  <th className="text-left py-3 px-3 text-xs uppercase tracking-wider text-muted-foreground font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {registeredEntities.map(entity => (
                  <tr key={entity.LandingzoneEntityId} className="border-b border-border last:border-0">
                    <td className="py-3 px-3 text-foreground">{entity.DataSourceName}</td>
                    <td className="py-3 px-3 font-mono text-foreground">{entity.SourceSchema}.{entity.SourceName}</td>
                    <td className="py-3 px-3 font-mono text-muted-foreground">{entity.FileName}.{entity.FileType}</td>
                    <td className="py-3 px-3 font-mono text-muted-foreground">{entity.FilePath}</td>
                    <td className="py-3 px-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                        entity.IsIncremental === 'True'
                          ? 'bg-blue-50 dark:bg-blue-950/20 text-blue-600 dark:text-blue-400'
                          : 'bg-muted text-muted-foreground'
                      }`}>
                        {entity.IsIncremental === 'True' ? 'Incremental' : 'Full'}
                      </span>
                    </td>
                    <td className="py-3 px-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                        entity.IsActive === 'True'
                          ? 'bg-emerald-50 dark:bg-emerald-950/20 text-emerald-600 dark:text-emerald-400'
                          : 'bg-red-50 dark:bg-red-950/20 text-red-600 dark:text-red-400'
                      }`}>
                        {entity.IsActive === 'True' ? 'Active' : 'Inactive'}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
