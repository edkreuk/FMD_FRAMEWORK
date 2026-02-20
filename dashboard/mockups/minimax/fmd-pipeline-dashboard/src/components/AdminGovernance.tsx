// FMD Pipeline Dashboard - Admin & Governance View

import React from 'react';
import {
  Activity,
  Clock,
  Database,
  Server,
  CheckCircle,
  AlertTriangle,
  XCircle,
  RefreshCw,
  ArrowRight,
  Layers,
} from 'lucide-react';
import type {
  HealthScorecard,
  SourceSystem,
  EntityInventory,
  DataLineageNode,
  DataLineageConnection,
  ConnectionHealth,
} from '../types/dashboard';

const healthConfig: Record<ConnectionHealth, { label: string; color: string; icon: React.ElementType }> = {
  healthy: { label: 'Healthy', color: 'text-emerald-600', icon: CheckCircle },
  degraded: { label: 'Degraded', color: 'text-amber-600', icon: AlertTriangle },
  critical: { label: 'Critical', color: 'text-red-600', icon: XCircle },
};

const layerColors: Record<string, string> = {
  source: '#64748b',
  landing: '#3b82f6',
  bronze: '#f59e0b',
  silver: '#8b5cf6',
  gold: '#10b981',
};

interface AdminGovernanceProps {
  healthScorecard: HealthScorecard;
  sourceSystems: SourceSystem[];
  entityInventory: EntityInventory[];
  dataLineage: {
    nodes: DataLineageNode[];
    connections: DataLineageConnection[];
  };
}

export const AdminGovernance: React.FC<AdminGovernanceProps> = ({
  healthScorecard,
  sourceSystems,
  entityInventory,
  dataLineage,
}) => {
  const formatTimeAgo = (date: Date): string => {
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const hours = Math.floor(diff / (1000 * 60 * 60));
    const minutes = Math.floor(diff / (1000 * 60));

    if (hours > 0) return `${hours}h ago`;
    if (minutes > 0) return `${minutes}m ago`;
    return 'Just now';
  };

  const formatNumber = (num: number): string => {
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toString();
  };

  // Calculate positions for lineage nodes
  const getNodePosition = (node: DataLineageNode) => {
    const positions: Record<string, { x: number; y: number }> = {
      'src-sap': { x: 50, y: 80 },
      'src-salesforce': { x: 50, y: 180 },
      'src-oracle': { x: 50, y: 280 },
      'src-workday': { x: 50, y: 380 },
      'src-s3': { x: 50, y: 480 },
      'landing-revenue': { x: 200, y: 80 },
      'landing-customer': { x: 200, y: 230 },
      'landing-expense': { x: 200, y: 380 },
      'bronze-revenue': { x: 350, y: 100 },
      'bronze-customer': { x: 350, y: 280 },
      'silver-revenue': { x: 500, y: 120 },
      'silver-customer': { x: 500, y: 260 },
      'gold-revenue': { x: 650, y: 140 },
      'gold-customer': { x: 650, y: 240 },
    };
    return positions[node.id] || { x: 0, y: 0 };
  };

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Admin & Governance</h1>
        <p className="text-slate-500 mt-1">
          System health monitoring, source management, and data lineage tracking
        </p>
      </div>

      {/* Health Scorecard */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-6">Health Scorecard</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
          {/* Success Rate */}
          <div className="bg-slate-50 rounded-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <div className="w-10 h-10 bg-emerald-100 rounded-lg flex items-center justify-center">
                <Activity className="w-5 h-5 text-emerald-600" />
              </div>
              <span className="text-xs font-medium text-emerald-600 bg-emerald-50 px-2 py-1 rounded-full">
                +2.3%
              </span>
            </div>
            <p className="text-3xl font-bold text-slate-900">{healthScorecard.successRate}%</p>
            <p className="text-sm text-slate-500 mt-1">Success Rate</p>
            <div className="mt-3">
              <div className="w-full bg-slate-200 rounded-full h-2">
                <div
                  className="bg-emerald-500 h-2 rounded-full transition-all"
                  style={{ width: `${healthScorecard.successRate}%` }}
                ></div>
              </div>
            </div>
          </div>

          {/* Data Freshness */}
          <div className="bg-slate-50 rounded-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                <Clock className="w-5 h-5 text-blue-600" />
              </div>
              <span className="text-xs font-medium text-blue-600 bg-blue-50 px-2 py-1 rounded-full">
                Live
              </span>
            </div>
            <p className="text-3xl font-bold text-slate-900">{healthScorecard.dataFreshness}h</p>
            <p className="text-sm text-slate-500 mt-1">Data Freshness</p>
            <p className="text-xs text-slate-400 mt-2">Since last update</p>
          </div>

          {/* Average Runtime */}
          <div className="bg-slate-50 rounded-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <div className="w-10 h-10 bg-purple-100 rounded-lg flex items-center justify-center">
                <RefreshCw className="w-5 h-5 text-purple-600" />
              </div>
              <span className="text-xs font-medium text-purple-600 bg-purple-50 px-2 py-1 rounded-full">
                -5m
              </span>
            </div>
            <p className="text-3xl font-bold text-slate-900">{healthScorecard.averageRuntime}m</p>
            <p className="text-sm text-slate-500 mt-1">Average Runtime</p>
            <p className="text-xs text-slate-400 mt-2">Per pipeline</p>
          </div>

          {/* Pipeline Stats */}
          <div className="bg-slate-50 rounded-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <div className="w-10 h-10 bg-slate-100 rounded-lg flex items-center justify-center">
                <Database className="w-5 h-5 text-slate-600" />
              </div>
            </div>
            <div className="flex items-baseline space-x-2">
              <p className="text-3xl font-bold text-slate-900">{healthScorecard.totalPipelines}</p>
              <p className="text-sm text-slate-500">pipelines</p>
            </div>
            <div className="flex items-center mt-2 space-x-4 text-sm">
              <span className="flex items-center text-emerald-600">
                <div className="w-2 h-2 bg-emerald-500 rounded-full mr-1.5"></div>
                {healthScorecard.activePipelines} active
              </span>
              <span className="flex items-center text-red-600">
                <div className="w-2 h-2 bg-red-500 rounded-full mr-1.5"></div>
                {healthScorecard.failedPipelines} failed
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Source System Registry & Entity Inventory */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Source System Registry */}
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h2 className="text-lg font-semibold text-slate-900 mb-4">Source System Registry</h2>
          <div className="space-y-3">
            {sourceSystems.map((system) => {
              const config = healthConfig[system.connectionHealth];
              const HealthIcon = config.icon;

              return (
                <div
                  key={system.id}
                  className="flex items-center justify-between p-4 bg-slate-50 rounded-lg border border-slate-200"
                >
                  <div className="flex items-center space-x-4">
                    <div className="w-10 h-10 bg-white rounded-lg border border-slate-200 flex items-center justify-center">
                      <Server className="w-5 h-5 text-slate-600" />
                    </div>
                    <div>
                      <p className="font-medium text-slate-900">{system.name}</p>
                      <p className="text-sm text-slate-500">{system.type}</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-4">
                    <div className="text-right">
                      <p className="text-sm text-slate-500">
                        {formatNumber(system.recordCount)} records
                      </p>
                      <p className="text-xs text-slate-400">
                        Synced {formatTimeAgo(system.lastSyncTime)}
                      </p>
                    </div>
                    <div className={`flex items-center ${config.color}`}>
                      <HealthIcon className="w-4 h-4 mr-1" />
                      <span className="text-sm font-medium">{config.label}</span>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Entity Inventory */}
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h2 className="text-lg font-semibold text-slate-900 mb-4">Entity Inventory</h2>
          <div className="grid grid-cols-2 gap-4">
            {entityInventory.map((inventory, index) => (
              <div
                key={inventory.layer}
                className="relative bg-slate-50 rounded-lg p-5 border border-slate-200 overflow-hidden"
              >
                {/* Layer Indicator */}
                <div
                  className="absolute top-0 left-0 w-1 h-full"
                  style={{ backgroundColor: layerColors[inventory.layer.toLowerCase()] }}
                ></div>

                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center">
                    <Layers
                      className="w-5 h-5 mr-2"
                      style={{ color: layerColors[inventory.layer.toLowerCase()] }}
                    />
                    <span className="font-medium text-slate-900">{inventory.layer}</span>
                  </div>
                  <span
                    className="text-xs px-2 py-1 rounded-full font-medium"
                    style={{
                      backgroundColor: `${layerColors[inventory.layer.toLowerCase()]}15`,
                      color: layerColors[inventory.layer.toLowerCase()],
                    }}
                  >
                    Layer {index + 1}
                  </span>
                </div>

                <p className="text-3xl font-bold text-slate-900">{inventory.entityCount}</p>
                <p className="text-sm text-slate-500 mt-1">entities</p>

                <div className="mt-3 pt-3 border-t border-slate-200">
                  <p className="text-xs text-slate-400">
                    Updated {formatTimeAgo(inventory.lastUpdated)}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Data Lineage */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Data Lineage</h2>
            <p className="text-sm text-slate-500 mt-1">
              Visual flow from source systems to analytics-ready data
            </p>
          </div>
        </div>

        {/* Legend */}
        <div className="flex items-center space-x-6 mb-6 pb-4 border-b border-slate-200">
          <span className="text-sm font-medium text-slate-500">Layers:</span>
          {[
            { label: 'Source', color: layerColors.source },
            { label: 'Landing', color: layerColors.landing },
            { label: 'Bronze', color: layerColors.bronze },
            { label: 'Silver', color: layerColors.silver },
            { label: 'Gold', color: layerColors.gold },
          ].map((item) => (
            <div key={item.label} className="flex items-center">
              <div
                className="w-3 h-3 rounded-full mr-1.5"
                style={{ backgroundColor: item.color }}
              ></div>
              <span className="text-sm text-slate-600">{item.label}</span>
            </div>
          ))}
        </div>

        {/* Lineage Visualization */}
        <div className="relative overflow-x-auto">
          <svg
            viewBox="0 0 700 550"
            className="w-full min-w-[600px] h-auto"
            style={{ minHeight: '400px' }}
          >
            {/* Connection Lines */}
            {dataLineage.connections.map((connection, index) => {
              const sourceNode = dataLineage.nodes.find((n) => n.id === connection.source);
              const targetNode = dataLineage.nodes.find((n) => n.id === connection.target);

              if (!sourceNode || !targetNode) return null;

              const sourcePos = getNodePosition(sourceNode);
              const targetPos = getNodePosition(targetNode);

              // Calculate control points for curved line
              const midX = (sourcePos.x + targetPos.x) / 2;

              return (
                <g key={index}>
                  <path
                    d={`M ${sourcePos.x + 40} ${sourcePos.y + 15} C ${midX} ${sourcePos.y + 15}, ${midX} ${targetPos.y + 15}, ${targetPos.x} ${targetPos.y + 15}`}
                    fill="none"
                    stroke={layerColors[sourceNode.layer]}
                    strokeWidth={2}
                    strokeOpacity={0.4}
                    className="transition-all hover:stroke-opacity-70"
                  />
                  <polygon
                    points={`${targetPos.x - 5},${targetPos.y + 10} ${targetPos.x},${targetPos.y + 15} ${targetPos.x - 5},${targetPos.y + 20}`}
                    fill={layerColors[sourceNode.layer]}
                    opacity={0.4}
                  />
                </g>
              );
            })}

            {/* Nodes */}
            {dataLineage.nodes.map((node) => {
              const pos = getNodePosition(node);
              const color = layerColors[node.layer] || '#64748b';

              return (
                <g
                  key={node.id}
                  className="cursor-pointer transition-all hover:opacity-80"
                >
                  <rect
                    x={pos.x}
                    y={pos.y}
                    width={80}
                    height={30}
                    rx={6}
                    fill={color}
                    fillOpacity={0.15}
                    stroke={color}
                    strokeWidth={2}
                  />
                  <text
                    x={pos.x + 40}
                    y={pos.y + 20}
                    textAnchor="middle"
                    className="text-xs font-medium fill-slate-700"
                    style={{ fontSize: '11px' }}
                  >
                    {node.name.length > 12 ? node.name.substring(0, 12) + '...' : node.name}
                  </text>
                </g>
              );
            })}
          </svg>
        </div>

        {/* Flow Summary */}
        <div className="mt-6 pt-4 border-t border-slate-200">
          <div className="flex items-center justify-between text-sm">
            <span className="text-slate-500">Data Flow Summary</span>
            <div className="flex items-center space-x-6">
              <span className="text-slate-600">
                <span className="font-medium text-slate-900">5</span> source systems
              </span>
              <ArrowRight className="w-4 h-4 text-slate-400" />
              <span className="text-slate-600">
                <span className="font-medium text-slate-900">3</span> landing tables
              </span>
              <ArrowRight className="w-4 h-4 text-slate-400" />
              <span className="text-slate-600">
                <span className="font-medium text-slate-900">2</span> gold tables
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AdminGovernance;
