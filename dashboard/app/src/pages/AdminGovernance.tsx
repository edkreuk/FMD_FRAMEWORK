import { useState, useEffect, useCallback } from 'react';
import {
  XCircle,
  RefreshCw,
  ArrowRight,
  Layers,
  Loader2,
} from 'lucide-react';
import { Button } from '@/components/ui/button';

// ── Types for API responses ──

interface Connection {
  ConnectionId: string;
  ConnectionGuid: string;
  Name: string;
  Type: string;
  IsActive: string;
}

interface DataSource {
  DataSourceId: string;
  Name: string;
  Namespace: string;
  Type: string;
  Description: string | null;
  IsActive: string;
  ConnectionName: string;
}

interface Entity {
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

interface Pipeline {
  PipelineId: string;
  PipelineGuid: string;
  WorkspaceGuid: string;
  Name: string;
  IsActive: string;
}

interface Workspace {
  WorkspaceId: string;
  WorkspaceGuid: string;
  Name: string;
}

interface Lakehouse {
  LakehouseId: string;
  LakehouseGuid: string;
  WorkspaceGuid: string;
  Name: string;
  IsActive: string;
}

interface BronzeEntity {
  BronzeLayerEntityId: string;
  LandingzoneEntityId: string;
  LakehouseId: string;
  Schema: string;
  Name: string;
  PrimaryKeys: string;
  FileType: string;
  IsActive: string;
}

interface SilverEntity {
  SilverLayerEntityId: string;
  BronzeLayerEntityId: string;
  LakehouseId: string;
  Schema: string;
  Name: string;
  FileType: string;
  IsActive: string;
}

interface DashboardStats {
  activeConnections: number;
  activeDataSources: number;
  activeEntities: number;
  lakehouses: number;
  entityBreakdown: { DataSourceName: string; DataSourceType: string; EntityCount: string }[];
}

// ── Icon component for Fabric SVGs ──

function FabricIcon({ name, className = 'w-5 h-5' }: { name: string; className?: string }) {
  return <img src={`/icons/${name}.svg`} alt={name} className={className} />;
}

// ── Layer colors ──

const layerColors: Record<string, string> = {
  source: '#64748b',
  landing: '#3b82f6',
  bronze: '#f59e0b',
  silver: '#8b5cf6',
  gold: '#10b981',
};

// ── API fetch helper ──

const API = 'http://localhost:8787/api';

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export default function AdminGovernance() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [dataSources, setDataSources] = useState<DataSource[]>([]);
  const [entities, setEntities] = useState<Entity[]>([]);
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [lakehouses, setLakehouses] = useState<Lakehouse[]>([]);
  const [bronzeEntities, setBronzeEntities] = useState<BronzeEntity[]>([]);
  const [silverEntities, setSilverEntities] = useState<SilverEntity[]>([]);
  const [stats, setStats] = useState<DashboardStats | null>(null);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [conn, ds, ent, pipes, ws, lh, bronze, silver, st] = await Promise.all([
        fetchJson<Connection[]>('/connections'),
        fetchJson<DataSource[]>('/datasources'),
        fetchJson<Entity[]>('/entities'),
        fetchJson<Pipeline[]>('/pipelines'),
        fetchJson<Workspace[]>('/workspaces'),
        fetchJson<Lakehouse[]>('/lakehouses'),
        fetchJson<BronzeEntity[]>('/bronze-entities'),
        fetchJson<SilverEntity[]>('/silver-entities'),
        fetchJson<DashboardStats>('/stats'),
      ]);
      setConnections(conn);
      setDataSources(ds);
      setEntities(ent);
      setPipelines(pipes);
      setWorkspaces(ws);
      setLakehouses(lh);
      setBronzeEntities(bronze);
      setSilverEntities(silver);
      setStats(st);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  // Derived data
  const activePipelines = pipelines.filter(p => p.IsActive === 'True');
  const pipelinesByCategory = {
    landingZone: activePipelines.filter(p => p.Name.includes('_LDZ_')),
    bronze: activePipelines.filter(p => p.Name.includes('_BRONZE_') || p.Name.includes('_BRZ_')),
    silver: activePipelines.filter(p => p.Name.includes('_SILVER_') || p.Name.includes('_SLV_')),
    orchestration: activePipelines.filter(p => p.Name.includes('_LOAD_') && !p.Name.includes('_LDZ_')),
    utility: activePipelines.filter(p =>
      !p.Name.includes('_LDZ_') && !p.Name.includes('_BRONZE_') && !p.Name.includes('_BRZ_') &&
      !p.Name.includes('_SILVER_') && !p.Name.includes('_SLV_') &&
      !(p.Name.includes('_LOAD_') && !p.Name.includes('_LDZ_'))
    ),
  };

  const devWorkspaces = workspaces.filter(w => w.Name.includes('(D)'));
  const prodWorkspaces = workspaces.filter(w => w.Name.includes('(P)'));
  const configWorkspaces = workspaces.filter(w => !w.Name.includes('(D)') && !w.Name.includes('(P)'));

  // Build lineage from real data
  const lineageNodes: { id: string; name: string; layer: string }[] = [];
  const lineageEdges: { source: string; target: string }[] = [];

  // Sources (connections)
  connections.filter(c => c.IsActive === 'True' && c.Name !== 'ONELAKE').forEach(c => {
    lineageNodes.push({ id: `conn-${c.ConnectionId}`, name: c.Name, layer: 'source' });
  });

  // Data sources → linked to connections
  dataSources.filter(ds => ds.IsActive === 'True').forEach(ds => {
    const connNode = connections.find(c => c.Name === ds.ConnectionName);
    if (connNode) {
      const dsId = `ds-${ds.DataSourceId}`;
      if (!lineageNodes.find(n => n.id === dsId)) {
        lineageNodes.push({ id: dsId, name: ds.Name, layer: 'landing' });
      }
      lineageEdges.push({ source: `conn-${connNode.ConnectionId}`, target: dsId });
    }
  });

  // Landing zone entities → linked to datasources
  entities.forEach(e => {
    const ds = dataSources.find(d => d.Name === e.DataSourceName);
    if (ds) {
      const entId = `lz-${e.LandingzoneEntityId}`;
      lineageNodes.push({ id: entId, name: `${e.SourceSchema}.${e.SourceName}`, layer: 'landing' });
      lineageEdges.push({ source: `ds-${ds.DataSourceId}`, target: entId });
    }
  });

  // Bronze entities → linked to landing zone
  bronzeEntities.forEach(b => {
    const bronzeId = `brz-${b.BronzeLayerEntityId}`;
    lineageNodes.push({ id: bronzeId, name: `${b.Schema}.${b.Name}`, layer: 'bronze' });
    lineageEdges.push({ source: `lz-${b.LandingzoneEntityId}`, target: bronzeId });
  });

  // Silver entities → linked to bronze
  silverEntities.forEach(s => {
    const silverId = `slv-${s.SilverLayerEntityId}`;
    lineageNodes.push({ id: silverId, name: `${s.Schema}.${s.Name}`, layer: 'silver' });
    lineageEdges.push({ source: `brz-${s.BronzeLayerEntityId}`, target: silverId });
  });

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
        <span className="ml-3 text-muted-foreground">Loading framework metadata...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-96 gap-4">
        <XCircle className="w-12 h-12 text-destructive" />
        <p className="text-destructive font-medium">{error}</p>
        <Button onClick={loadData} variant="outline" className="gap-2">
          <RefreshCw className="w-4 h-4" /> Retry
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-display font-bold tracking-tight text-foreground">Admin & Governance</h1>
          <p className="text-muted-foreground mt-1">
            Live framework metadata from Fabric SQL Database
          </p>
        </div>
        <Button onClick={loadData} variant="outline" size="sm" className="gap-2">
          <RefreshCw className="h-4 w-4" />
          Refresh
        </Button>
      </div>

      {/* Health Scorecard — LIVE DATA */}
      <div className="bg-card rounded-xl border border-border p-6">
        <h2 className="text-lg font-semibold text-foreground mb-6 flex items-center gap-2">
          <FabricIcon name="fabric" />
          Framework Health
        </h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
          <div className="bg-muted rounded-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <div className="w-10 h-10 bg-blue-100 dark:bg-blue-950/30 rounded-lg flex items-center justify-center">
                <FabricIcon name="pipeline" />
              </div>
              <span className="text-xs font-medium text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-950/20 px-2 py-1 rounded-full">
                {activePipelines.length} active
              </span>
            </div>
            <p className="text-3xl font-bold text-foreground">{pipelines.length}</p>
            <p className="text-sm text-muted-foreground mt-1">Pipelines</p>
            <div className="flex items-center mt-2 space-x-3 text-xs text-muted-foreground">
              <span>{pipelinesByCategory.landingZone.length} LDZ</span>
              <span>{pipelinesByCategory.bronze.length} Bronze</span>
              <span>{pipelinesByCategory.silver.length} Silver</span>
            </div>
          </div>

          <div className="bg-muted rounded-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <div className="w-10 h-10 bg-emerald-100 dark:bg-emerald-950/30 rounded-lg flex items-center justify-center">
                <FabricIcon name="sql_database" />
              </div>
              <span className="text-xs font-medium text-emerald-600 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950/20 px-2 py-1 rounded-full">
                Live
              </span>
            </div>
            <p className="text-3xl font-bold text-foreground">{stats?.activeConnections ?? 0}</p>
            <p className="text-sm text-muted-foreground mt-1">Connections</p>
            <p className="text-xs text-muted-foreground mt-2">{stats?.activeDataSources ?? 0} data sources</p>
          </div>

          <div className="bg-muted rounded-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <div className="w-10 h-10 bg-purple-100 dark:bg-purple-950/30 rounded-lg flex items-center justify-center">
                <FabricIcon name="lakehouse" />
              </div>
            </div>
            <p className="text-3xl font-bold text-foreground">{stats?.lakehouses ?? 0}</p>
            <p className="text-sm text-muted-foreground mt-1">Lakehouses</p>
            <div className="flex items-center mt-2 space-x-4 text-xs">
              <span className="text-blue-600 dark:text-blue-400">
                {lakehouses.filter(l => workspaces.find(w => w.WorkspaceGuid === l.WorkspaceGuid)?.Name.includes('(D)')).length} Dev
              </span>
              <span className="text-emerald-600 dark:text-emerald-400">
                {lakehouses.filter(l => workspaces.find(w => w.WorkspaceGuid === l.WorkspaceGuid)?.Name.includes('(P)')).length} Prod
              </span>
            </div>
          </div>

          <div className="bg-muted rounded-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <div className="w-10 h-10 bg-amber-100 dark:bg-amber-950/30 rounded-lg flex items-center justify-center">
                <FabricIcon name="sql_database" className="w-5 h-5" />
              </div>
            </div>
            <div className="flex items-baseline space-x-2">
              <p className="text-3xl font-bold text-foreground">{stats?.activeEntities ?? 0}</p>
              <p className="text-sm text-muted-foreground">entities</p>
            </div>
            <div className="flex items-center mt-2 space-x-4 text-xs">
              <span className="flex items-center text-blue-600 dark:text-blue-400">
                <div className="w-2 h-2 bg-blue-500 rounded-full mr-1.5"></div>
                {entities.length} Landing
              </span>
              <span className="flex items-center text-amber-600 dark:text-amber-400">
                <div className="w-2 h-2 bg-amber-500 rounded-full mr-1.5"></div>
                {bronzeEntities.length} Bronze
              </span>
              <span className="flex items-center text-purple-600 dark:text-purple-400">
                <div className="w-2 h-2 bg-purple-500 rounded-full mr-1.5"></div>
                {silverEntities.length} Silver
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Workspaces & Lakehouses + Pipeline Inventory */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Workspaces */}
        <div className="bg-card rounded-xl border border-border p-6">
          <h2 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2">
            <FabricIcon name="folder" />
            Workspaces & Lakehouses
          </h2>

          {/* Dev */}
          {devWorkspaces.length > 0 && (
            <div className="mb-4">
              <h3 className="text-xs uppercase tracking-wider text-muted-foreground font-semibold mb-2">Development</h3>
              <div className="space-y-2">
                {devWorkspaces.map(ws => (
                  <div key={ws.WorkspaceId} className="p-3 bg-muted rounded-lg border border-border">
                    <div className="flex items-center gap-2 mb-2">
                      <FabricIcon name="fabric" className="w-4 h-4" />
                      <span className="font-medium text-sm text-foreground">{ws.Name}</span>
                    </div>
                    <div className="flex flex-wrap gap-2 ml-6">
                      {lakehouses.filter(l => l.WorkspaceGuid === ws.WorkspaceGuid).map(lh => (
                        <span key={lh.LakehouseId} className="text-xs bg-blue-50 dark:bg-blue-950/20 text-blue-700 dark:text-blue-300 px-2 py-1 rounded-full flex items-center gap-1">
                          <FabricIcon name="lakehouse" className="w-3 h-3" />
                          {lh.Name}
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Prod */}
          {prodWorkspaces.length > 0 && (
            <div className="mb-4">
              <h3 className="text-xs uppercase tracking-wider text-muted-foreground font-semibold mb-2">Production</h3>
              <div className="space-y-2">
                {prodWorkspaces.map(ws => (
                  <div key={ws.WorkspaceId} className="p-3 bg-muted rounded-lg border border-border">
                    <div className="flex items-center gap-2 mb-2">
                      <FabricIcon name="fabric" className="w-4 h-4" />
                      <span className="font-medium text-sm text-foreground">{ws.Name}</span>
                    </div>
                    <div className="flex flex-wrap gap-2 ml-6">
                      {lakehouses.filter(l => l.WorkspaceGuid === ws.WorkspaceGuid).map(lh => (
                        <span key={lh.LakehouseId} className="text-xs bg-emerald-50 dark:bg-emerald-950/20 text-emerald-700 dark:text-emerald-300 px-2 py-1 rounded-full flex items-center gap-1">
                          <FabricIcon name="lakehouse" className="w-3 h-3" />
                          {lh.Name}
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Config */}
          {configWorkspaces.length > 0 && (
            <div>
              <h3 className="text-xs uppercase tracking-wider text-muted-foreground font-semibold mb-2">Config</h3>
              <div className="space-y-2">
                {configWorkspaces.map(ws => (
                  <div key={ws.WorkspaceId} className="p-3 bg-muted rounded-lg border border-border">
                    <div className="flex items-center gap-2">
                      <FabricIcon name="fabric" className="w-4 h-4" />
                      <span className="font-medium text-sm text-foreground">{ws.Name}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Pipeline Inventory */}
        <div className="bg-card rounded-xl border border-border p-6">
          <h2 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2">
            <FabricIcon name="pipeline" />
            Pipeline Inventory ({activePipelines.length} active)
          </h2>

          {[
            { label: 'Landing Zone', pipes: pipelinesByCategory.landingZone, color: 'blue' },
            { label: 'Bronze Layer', pipes: pipelinesByCategory.bronze, color: 'amber' },
            { label: 'Silver Layer', pipes: pipelinesByCategory.silver, color: 'purple' },
            { label: 'Orchestration', pipes: pipelinesByCategory.orchestration, color: 'emerald' },
            { label: 'Utility', pipes: pipelinesByCategory.utility, color: 'slate' },
          ].filter(cat => cat.pipes.length > 0).map(cat => (
            <div key={cat.label} className="mb-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">{cat.label}</h3>
                <span className={`text-xs font-medium text-${cat.color}-600 dark:text-${cat.color}-400 bg-${cat.color}-50 dark:bg-${cat.color}-950/20 px-2 py-0.5 rounded-full`}>
                  {cat.pipes.length}
                </span>
              </div>
              <div className="space-y-1">
                {cat.pipes.map(p => (
                  <div key={p.PipelineId} className="flex items-center gap-2 px-3 py-1.5 bg-muted/50 rounded text-sm hover:bg-muted transition-colors">
                    <FabricIcon name="pipeline" className="w-3.5 h-3.5 opacity-60" />
                    <span className="text-foreground/80 font-mono text-xs">{p.Name}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Entity Inventory by Layer */}
      <div className="bg-card rounded-xl border border-border p-6">
        <h2 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2">
          <Layers className="w-5 h-5" />
          Entity Inventory by Layer
        </h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[
            { label: 'Connections', count: connections.filter(c => c.IsActive === 'True').length, color: layerColors.source, icon: 'sql_database' },
            { label: 'Landing Zone', count: entities.length, color: layerColors.landing, icon: 'lakehouse' },
            { label: 'Bronze', count: bronzeEntities.length, color: layerColors.bronze, icon: 'lakehouse' },
            { label: 'Silver', count: silverEntities.length, color: layerColors.silver, icon: 'lakehouse' },
          ].map((item, index) => (
            <div key={item.label} className="relative bg-muted rounded-lg p-5 border border-border overflow-hidden">
              <div className="absolute top-0 left-0 w-1 h-full" style={{ backgroundColor: item.color }}></div>
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center">
                  <FabricIcon name={item.icon} className="w-5 h-5 mr-2" />
                  <span className="font-medium text-foreground">{item.label}</span>
                </div>
                <span className="text-xs px-2 py-1 rounded-full font-medium" style={{
                  backgroundColor: `${item.color}15`,
                  color: item.color,
                }}>
                  Layer {index}
                </span>
              </div>
              <p className="text-3xl font-bold text-foreground">{item.count}</p>
              <p className="text-sm text-muted-foreground mt-1">
                {item.count === 0 ? 'Not yet registered' : `${item.count} registered`}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Data Lineage — LIVE from registered entities */}
      <div className="bg-card rounded-xl border border-border p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-lg font-semibold text-foreground">Data Lineage</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Live lineage built from registered connections, data sources, and entities
            </p>
          </div>
        </div>

        {/* Legend */}
        <div className="flex items-center space-x-6 mb-6 pb-4 border-b border-border">
          <span className="text-sm font-medium text-muted-foreground">Layers:</span>
          {[
            { label: 'Source', color: layerColors.source },
            { label: 'Landing', color: layerColors.landing },
            { label: 'Bronze', color: layerColors.bronze },
            { label: 'Silver', color: layerColors.silver },
            { label: 'Gold', color: layerColors.gold },
          ].map((item) => (
            <div key={item.label} className="flex items-center">
              <div className="w-3 h-3 rounded-full mr-1.5" style={{ backgroundColor: item.color }}></div>
              <span className="text-sm text-muted-foreground">{item.label}</span>
            </div>
          ))}
        </div>

        {lineageNodes.length === 0 ? (
          <div className="text-center py-12 text-muted-foreground">
            <FabricIcon name="databases" className="w-12 h-12 mx-auto mb-3 opacity-30" />
            <p className="font-medium">No lineage data yet</p>
            <p className="text-sm mt-1">Register connections, data sources, and entities in Source Manager to build the lineage graph</p>
          </div>
        ) : (
          <>
            {/* Lineage as a horizontal flow diagram using CSS grid */}
            <div className="overflow-x-auto">
              <div className="flex items-start gap-6 min-w-[700px] py-4">
                {/* Sources column */}
                <div className="flex flex-col gap-2 min-w-[140px]">
                  <div className="text-xs uppercase tracking-wider text-muted-foreground font-semibold mb-1 text-center">Sources</div>
                  {lineageNodes.filter(n => n.layer === 'source').map(node => (
                    <div key={node.id} className="px-3 py-2 rounded-lg border-2 text-center text-xs font-medium" style={{ borderColor: layerColors.source, backgroundColor: `${layerColors.source}15`, color: layerColors.source }}>
                      {node.name}
                    </div>
                  ))}
                </div>

                <div className="flex items-center self-center text-muted-foreground">
                  <ArrowRight className="w-5 h-5" />
                </div>

                {/* Landing column */}
                <div className="flex flex-col gap-2 min-w-[160px]">
                  <div className="text-xs uppercase tracking-wider text-muted-foreground font-semibold mb-1 text-center">Landing Zone</div>
                  {lineageNodes.filter(n => n.layer === 'landing').map(node => (
                    <div key={node.id} className="px-3 py-2 rounded-lg border-2 text-center text-xs font-medium" style={{ borderColor: layerColors.landing, backgroundColor: `${layerColors.landing}15`, color: layerColors.landing }}>
                      {node.name}
                    </div>
                  ))}
                </div>

                <div className="flex items-center self-center text-muted-foreground">
                  <ArrowRight className="w-5 h-5" />
                </div>

                {/* Bronze column */}
                <div className="flex flex-col gap-2 min-w-[160px]">
                  <div className="text-xs uppercase tracking-wider text-muted-foreground font-semibold mb-1 text-center">Bronze</div>
                  {lineageNodes.filter(n => n.layer === 'bronze').length > 0 ? (
                    lineageNodes.filter(n => n.layer === 'bronze').map(node => (
                      <div key={node.id} className="px-3 py-2 rounded-lg border-2 text-center text-xs font-medium" style={{ borderColor: layerColors.bronze, backgroundColor: `${layerColors.bronze}15`, color: layerColors.bronze }}>
                        {node.name}
                      </div>
                    ))
                  ) : (
                    <div className="px-3 py-2 rounded-lg border-2 border-dashed text-center text-xs text-muted-foreground" style={{ borderColor: layerColors.bronze }}>
                      No entities yet
                    </div>
                  )}
                </div>

                <div className="flex items-center self-center text-muted-foreground">
                  <ArrowRight className="w-5 h-5" />
                </div>

                {/* Silver column */}
                <div className="flex flex-col gap-2 min-w-[160px]">
                  <div className="text-xs uppercase tracking-wider text-muted-foreground font-semibold mb-1 text-center">Silver</div>
                  {lineageNodes.filter(n => n.layer === 'silver').length > 0 ? (
                    lineageNodes.filter(n => n.layer === 'silver').map(node => (
                      <div key={node.id} className="px-3 py-2 rounded-lg border-2 text-center text-xs font-medium" style={{ borderColor: layerColors.silver, backgroundColor: `${layerColors.silver}15`, color: layerColors.silver }}>
                        {node.name}
                      </div>
                    ))
                  ) : (
                    <div className="px-3 py-2 rounded-lg border-2 border-dashed text-center text-xs text-muted-foreground" style={{ borderColor: layerColors.silver }}>
                      No entities yet
                    </div>
                  )}
                </div>
              </div>
            </div>

            {/* Flow Summary */}
            <div className="mt-6 pt-4 border-t border-border">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Data Flow Summary</span>
                <div className="flex items-center space-x-6">
                  <span className="text-muted-foreground">
                    <span className="font-medium text-foreground">{connections.filter(c => c.IsActive === 'True').length}</span> connections
                  </span>
                  <ArrowRight className="w-4 h-4 text-muted-foreground" />
                  <span className="text-muted-foreground">
                    <span className="font-medium text-foreground">{entities.length}</span> landing entities
                  </span>
                  <ArrowRight className="w-4 h-4 text-muted-foreground" />
                  <span className="text-muted-foreground">
                    <span className="font-medium text-foreground">{bronzeEntities.length}</span> bronze
                  </span>
                  <ArrowRight className="w-4 h-4 text-muted-foreground" />
                  <span className="text-muted-foreground">
                    <span className="font-medium text-foreground">{silverEntities.length}</span> silver
                  </span>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
