import { useEffect, useRef, useState, useCallback } from "react";
import cytoscape from "cytoscape";
import dagre from "cytoscape-dagre";
import { Search, RotateCcw, Maximize, Layers } from "lucide-react";

// Register dagre layout
cytoscape.use(dagre);

// ============================================================================
// NODE TYPE DEFINITIONS
// ============================================================================

const NODE_TYPES: Record<string, { label: string; color: string; shape: string }> = {
  source:     { label: "SQL Source",            color: "#3B82F6", shape: "cut-rectangle" },
  connection: { label: "Connection",            color: "#8B5CF6", shape: "diamond" },
  datasource: { label: "Data Source",           color: "#F59E0B", shape: "round-hexagon" },
  orch:       { label: "Orchestrator Pipeline", color: "#EF4444", shape: "octagon" },
  command:    { label: "Command Pipeline",      color: "#F97316", shape: "round-pentagon" },
  copy:       { label: "Copy Pipeline",         color: "#06B6D4", shape: "round-rectangle" },
  lakehouse:  { label: "Lakehouse",             color: "#10B981", shape: "barrel" },
  notebook:   { label: "Notebook",              color: "#A855F7", shape: "round-diamond" },
  config:     { label: "Config / Variable Lib", color: "#64748B", shape: "ellipse" },
  tooling:    { label: "Tooling Pipeline",      color: "#22C55E", shape: "tag" },
};

// ============================================================================
// GRAPH DATA
// ============================================================================

interface NodeDef {
  id: string;
  type: string;
  techLabel: string;
  execLabel: string;
  desc: string;
  layer: string;
}

interface EdgeDef {
  source: string;
  target: string;
  label?: string;
  style?: string;
}

const nodes: NodeDef[] = [
  // Sources
  { id: "src_powerdata", type: "source", techLabel: "SQL2019Dev\nIPC_PowerData", execLabel: "PowerData\nDatabase", desc: "Production PowerData database on SQL Server 2019. Contains core business intelligence and reporting data.", layer: "Source" },
  { id: "src_m3fdb", type: "source", techLabel: "M3Dev-DB1\nM3FDBTST", execLabel: "M3 ERP\nDatabase", desc: "M3 ERP (Infor) foundation database. Master data for customers, vendors, items, and transactions.", layer: "Source" },
  { id: "src_m3etl", type: "source", techLabel: "SQL2016Dev\nM3TST-ETL", execLabel: "M3 ETL\nStaging DB", desc: "M3 ETL staging database. Pre-processed extracts optimized for downstream analytics.", layer: "Source" },
  { id: "src_mes", type: "source", techLabel: "SQL2012Test\nMES", execLabel: "Manufacturing\n(MES)", desc: "Manufacturing Execution System. Production orders, work centers, quality metrics, shop floor data.", layer: "Source" },
  // Gateway Connections
  { id: "con_powerdata", type: "connection", techLabel: "CON_FMD_\nSQL2019DEV_POWERDATA", execLabel: "PowerData\nGateway Link", desc: "On-premises data gateway connection to the PowerData SQL Server instance.", layer: "Connection" },
  { id: "con_m3fdb", type: "connection", techLabel: "CON_FMD_\nM3DEV_M3FDBTST", execLabel: "M3 ERP\nGateway Link", desc: "On-premises data gateway connection to the M3 ERP SQL Server instance.", layer: "Connection" },
  { id: "con_m3etl", type: "connection", techLabel: "CON_FMD_\nSQL2016DEV_M3TSTETL", execLabel: "M3 ETL\nGateway Link", desc: "On-premises data gateway connection to the M3 ETL SQL Server instance.", layer: "Connection" },
  { id: "con_mes", type: "connection", techLabel: "CON_FMD_\nSQL2012TEST_MES", execLabel: "MES\nGateway Link", desc: "On-premises data gateway connection to the MES SQL Server instance.", layer: "Connection" },
  // Framework Connections
  { id: "con_fabric_sql", type: "connection", techLabel: "CON_FMD_\nFABRIC_SQL", execLabel: "Config DB\nConnection", desc: "Internal Fabric connection to the framework metadata database.", layer: "Connection" },
  { id: "con_fabric_pipelines", type: "connection", techLabel: "CON_FMD_\nFABRIC_PIPELINES", execLabel: "Pipeline\nInvoker", desc: "Internal connection used by pipelines to call other pipelines.", layer: "Connection" },
  { id: "con_fabric_notebooks", type: "connection", techLabel: "CON_FMD_\nFABRIC_NOTEBOOKS", execLabel: "Notebook\nInvoker", desc: "Internal connection used by pipelines to launch notebooks.", layer: "Connection" },
  { id: "con_onelake", type: "connection", techLabel: "CON_FMD_\nONELAKE", execLabel: "OneLake\nInternal", desc: "Internal OneLake connection for lakehouse-to-lakehouse data movement.", layer: "Connection" },
  // Data Sources
  { id: "ds_powerdata", type: "datasource", techLabel: "DS: IPC_PowerData\n(ASQL_01)", execLabel: "PowerData\nSQL Source", desc: "Registered data source for PowerData. Routes to the SQL Server copy pipeline at runtime.", layer: "DataSource" },
  { id: "ds_m3fdb", type: "datasource", techLabel: "DS: M3FDBTST\n(ASQL_01)", execLabel: "M3 ERP\nSQL Source", desc: "Registered data source for M3 ERP. Routes to the SQL Server copy pipeline at runtime.", layer: "DataSource" },
  { id: "ds_m3etl", type: "datasource", techLabel: "DS: M3TST-ETL\n(ASQL_01)", execLabel: "M3 ETL\nSQL Source", desc: "Registered data source for M3 ETL. Routes to the SQL Server copy pipeline at runtime.", layer: "DataSource" },
  { id: "ds_mes", type: "datasource", techLabel: "DS: MES\n(ASQL_01)", execLabel: "MES\nSQL Source", desc: "Registered data source for MES. Routes to the SQL Server copy pipeline at runtime.", layer: "DataSource" },
  { id: "ds_onelake_t", type: "datasource", techLabel: "DS: OneLake\n(TABLES)", execLabel: "OneLake\nTable Source", desc: "OneLake Tables data source for internal Delta table transfers between lakehouses.", layer: "DataSource" },
  { id: "ds_onelake_f", type: "datasource", techLabel: "DS: OneLake\n(FILES)", execLabel: "OneLake\nFile Source", desc: "OneLake Files data source for file-based transfers between lakehouses.", layer: "DataSource" },
  // Orchestration Pipelines
  { id: "pl_load_all", type: "orch", techLabel: "PL_FMD_\nLOAD_ALL", execLabel: "Run Everything\n(Master)", desc: "Master orchestrator. Runs the entire pipeline chain end-to-end: ingest from sources, process to Bronze, transform to Silver.", layer: "Orchestration" },
  { id: "pl_load_ldz", type: "orch", techLabel: "PL_FMD_\nLOAD_LANDINGZONE", execLabel: "Ingest from\nSources", desc: "Reads the metadata database to find all active entities, then routes each one to the correct source connector (SQL, Oracle, FTP, etc.).", layer: "Orchestration" },
  { id: "pl_load_brz", type: "orch", techLabel: "PL_FMD_\nLOAD_BRONZE", execLabel: "Process to\nBronze", desc: "Takes raw ingested files from Landing Zone and converts them into structured Bronze Delta tables. No business logic yet.", layer: "Orchestration" },
  { id: "pl_load_slv", type: "orch", techLabel: "PL_FMD_\nLOAD_SILVER", execLabel: "Transform to\nSilver", desc: "Applies business rules, data quality checks, and transformations to produce clean, analytics-ready Silver tables.", layer: "Orchestration" },
  // Command Pipelines
  { id: "pl_cmd_asql", type: "command", techLabel: "PL_FMD_LDZ_\nCOMMAND_ASQL", execLabel: "Route SQL\nServer Data", desc: "Routes all SQL Server entities (PowerData, M3, MES) to the SQL copy pipeline.", layer: "Command" },
  { id: "pl_cmd_oracle", type: "command", techLabel: "PL_FMD_LDZ_\nCOMMAND_ORACLE", execLabel: "Route Oracle\nData", desc: "Routes Oracle database entities to the Oracle copy pipeline.", layer: "Command" },
  { id: "pl_cmd_adls", type: "command", techLabel: "PL_FMD_LDZ_\nCOMMAND_ADLS", execLabel: "Route Azure\nStorage Data", desc: "Routes Azure Data Lake Storage files to the ADLS copy pipeline.", layer: "Command" },
  { id: "pl_cmd_ftp", type: "command", techLabel: "PL_FMD_LDZ_\nCOMMAND_FTP", execLabel: "Route FTP\nFiles", desc: "Routes FTP server files to the FTP copy pipeline.", layer: "Command" },
  { id: "pl_cmd_sftp", type: "command", techLabel: "PL_FMD_LDZ_\nCOMMAND_SFTP", execLabel: "Route Secure\nFTP Files", desc: "Routes SFTP server files to the SFTP copy pipeline.", layer: "Command" },
  { id: "pl_cmd_onelake", type: "command", techLabel: "PL_FMD_LDZ_\nCOMMAND_ONELAKE", execLabel: "Route OneLake\nData", desc: "Routes internal OneLake data to the OneLake copy pipeline.", layer: "Command" },
  { id: "pl_cmd_adf", type: "command", techLabel: "PL_FMD_LDZ_\nCOMMAND_ADF", execLabel: "Route Azure\nData Factory", desc: "Routes ADF pipeline outputs to the ADF copy pipeline.", layer: "Command" },
  { id: "pl_cmd_nb", type: "command", techLabel: "PL_FMD_LDZ_\nCOMMAND_NOTEBOOK", execLabel: "Route Custom\nNotebook", desc: "Routes custom notebook-based ingestion to the notebook pipeline.", layer: "Command" },
  // Copy Pipelines
  { id: "pl_copy_asql", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_ASQL_01", execLabel: "Copy SQL\nTables", desc: "Connects to SQL Server via gateway, runs SELECT queries, writes Parquet files to Landing Zone.", layer: "Copy" },
  { id: "pl_copy_oracle", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_ORACLE_01", execLabel: "Copy Oracle\nTables", desc: "Connects to Oracle database, extracts table data into Landing Zone.", layer: "Copy" },
  { id: "pl_copy_adls", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_ADLS_01", execLabel: "Copy Azure\nStorage Files", desc: "Downloads files from Azure Data Lake Storage Gen2 into Landing Zone.", layer: "Copy" },
  { id: "pl_copy_ftp", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_FTP_01", execLabel: "Copy FTP\nFiles", desc: "Downloads files from FTP servers into Landing Zone.", layer: "Copy" },
  { id: "pl_copy_sftp", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_SFTP_01", execLabel: "Copy Secure\nFTP Files", desc: "Downloads files from SFTP servers into Landing Zone.", layer: "Copy" },
  { id: "pl_copy_onelake_t", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_ONELAKE_TABLES", execLabel: "Copy OneLake\nTables", desc: "Copies Delta tables from other OneLake lakehouse locations.", layer: "Copy" },
  { id: "pl_copy_onelake_f", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_ONELAKE_FILES", execLabel: "Copy OneLake\nFiles", desc: "Copies raw files from other OneLake file locations.", layer: "Copy" },
  { id: "pl_copy_adf", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_ADF", execLabel: "Pull from\nData Factory", desc: "Triggers an Azure Data Factory pipeline and captures its output.", layer: "Copy" },
  { id: "pl_copy_nb", type: "copy", techLabel: "PL_FMD_LDZ_COPY_\nFROM_CUSTOM_NB", execLabel: "Run Custom\nIngestion", desc: "Executes a user-defined Spark notebook for custom data ingestion logic.", layer: "Copy" },
  // Lakehouses
  { id: "lh_ldz", type: "lakehouse", techLabel: "LH_DATA_\nLANDINGZONE", execLabel: "Raw Data\nLanding Zone", desc: "Where source data first arrives. Raw Parquet files organized by source system. No transformations applied.", layer: "Landing Zone" },
  { id: "lh_bronze", type: "lakehouse", techLabel: "LH_BRONZE_\nLAYER", execLabel: "Structured Data\n(Bronze)", desc: "Raw data converted to structured Delta Lake tables. Schema enforced, data typed, but no business logic applied yet.", layer: "Bronze" },
  { id: "lh_silver", type: "lakehouse", techLabel: "LH_SILVER_\nLAYER", execLabel: "Clean Data\n(Silver)", desc: "Business-ready data. Transformations applied, quality validated, ready for reporting and analytics.", layer: "Silver" },
  // Notebooks
  { id: "nb_ldz_brz", type: "notebook", techLabel: "NB_FMD_LOAD_\nLANDING_BRONZE", execLabel: "Convert Raw\nto Structured", desc: "Reads Parquet files from Landing Zone, applies schema, writes Delta tables to Bronze lakehouse.", layer: "Bronze" },
  { id: "nb_brz_slv", type: "notebook", techLabel: "NB_FMD_LOAD_\nBRONZE_SILVER", execLabel: "Apply Business\nRules", desc: "Reads Bronze tables, applies business transformations, and writes cleansed data to Silver lakehouse.", layer: "Silver" },
  { id: "nb_dq", type: "notebook", techLabel: "NB_FMD_\nDQ_CLEANSING", execLabel: "Data Quality\nChecks", desc: "Runs data validation rules, flags bad records, applies cleansing logic, and writes quality metrics.", layer: "Silver" },
  { id: "nb_parallel", type: "notebook", techLabel: "NB_FMD_PROCESSING_\nPARALLEL_MAIN", execLabel: "Parallel\nProcessor", desc: "Runs multiple entities concurrently for faster throughput. Used by both Bronze and Silver pipelines.", layer: "Processing" },
  { id: "nb_ldz_main", type: "notebook", techLabel: "NB_FMD_PROCESSING_\nLDZ_MAIN", execLabel: "Batch Ingestion\nCoordinator", desc: "Coordinates multi-entity batch ingestion into Landing Zone.", layer: "Processing" },
  { id: "nb_utility", type: "notebook", techLabel: "NB_FMD_\nUTILITY_FUNCTIONS", execLabel: "Shared\nHelper Code", desc: "Common functions used by all notebooks: connection management, Spark utilities, logging, etc.", layer: "Shared" },
  // Configuration
  { id: "sql_fmd", type: "config", techLabel: "SQL_FMD_\nFRAMEWORK", execLabel: "Framework\nConfig DB", desc: "The brain of the framework. Stores all metadata: which sources to connect to, which tables to pull, how to transform them, and where to put them.", layer: "Config" },
  { id: "var_fmd", type: "config", techLabel: "VAR_FMD", execLabel: "Security\nSecrets", desc: "Stores sensitive configuration: Key Vault URI, tenant ID, client credentials. Never hardcoded in pipelines.", layer: "Config" },
  { id: "var_cfg_fmd", type: "config", techLabel: "VAR_CONFIG_\nFMD", execLabel: "Framework\nSettings", desc: "Framework runtime settings: database connection string, workspace GUIDs, database GUIDs.", layer: "Config" },
  { id: "env_fmd", type: "config", techLabel: "ENV_FMD", execLabel: "Spark\nEnvironment", desc: "Defines the Spark runtime: Python version, Spark version, and required library dependencies.", layer: "Config" },
  // Tooling
  { id: "pl_tooling", type: "tooling", techLabel: "PL_TOOLING_\nPOST_ASQL_TO_FMD", execLabel: "SQL Query\nUtility", desc: "Utility pipeline for ad-hoc SQL query execution against the framework database.", layer: "Tooling" },
];

const edges: EdgeDef[] = [
  { source: "src_powerdata", target: "con_powerdata", label: "gateway" },
  { source: "src_m3fdb", target: "con_m3fdb", label: "gateway" },
  { source: "src_m3etl", target: "con_m3etl", label: "gateway" },
  { source: "src_mes", target: "con_mes", label: "gateway" },
  { source: "con_powerdata", target: "ds_powerdata" },
  { source: "con_m3fdb", target: "ds_m3fdb" },
  { source: "con_m3etl", target: "ds_m3etl" },
  { source: "con_mes", target: "ds_mes" },
  { source: "con_onelake", target: "ds_onelake_t" },
  { source: "con_onelake", target: "ds_onelake_f" },
  { source: "pl_load_all", target: "pl_load_ldz", label: "step 1" },
  { source: "pl_load_all", target: "pl_load_brz", label: "step 2" },
  { source: "pl_load_all", target: "pl_load_slv", label: "step 3" },
  { source: "sql_fmd", target: "pl_load_ldz", label: "metadata", style: "dashed" },
  { source: "sql_fmd", target: "pl_load_brz", label: "metadata", style: "dashed" },
  { source: "sql_fmd", target: "pl_load_slv", label: "metadata", style: "dashed" },
  { source: "pl_load_ldz", target: "pl_cmd_asql" },
  { source: "pl_load_ldz", target: "pl_cmd_oracle" },
  { source: "pl_load_ldz", target: "pl_cmd_adls" },
  { source: "pl_load_ldz", target: "pl_cmd_ftp" },
  { source: "pl_load_ldz", target: "pl_cmd_sftp" },
  { source: "pl_load_ldz", target: "pl_cmd_onelake" },
  { source: "pl_load_ldz", target: "pl_cmd_adf" },
  { source: "pl_load_ldz", target: "pl_cmd_nb" },
  { source: "pl_cmd_asql", target: "pl_copy_asql" },
  { source: "pl_cmd_oracle", target: "pl_copy_oracle" },
  { source: "pl_cmd_adls", target: "pl_copy_adls" },
  { source: "pl_cmd_ftp", target: "pl_copy_ftp" },
  { source: "pl_cmd_sftp", target: "pl_copy_sftp" },
  { source: "pl_cmd_onelake", target: "pl_copy_onelake_t" },
  { source: "pl_cmd_onelake", target: "pl_copy_onelake_f" },
  { source: "pl_cmd_adf", target: "pl_copy_adf" },
  { source: "pl_cmd_nb", target: "pl_copy_nb" },
  { source: "ds_powerdata", target: "pl_copy_asql", label: "feeds", style: "dashed" },
  { source: "ds_m3fdb", target: "pl_copy_asql", label: "feeds", style: "dashed" },
  { source: "ds_m3etl", target: "pl_copy_asql", label: "feeds", style: "dashed" },
  { source: "ds_mes", target: "pl_copy_asql", label: "feeds", style: "dashed" },
  { source: "ds_onelake_t", target: "pl_copy_onelake_t", label: "feeds", style: "dashed" },
  { source: "ds_onelake_f", target: "pl_copy_onelake_f", label: "feeds", style: "dashed" },
  { source: "pl_copy_asql", target: "lh_ldz" },
  { source: "pl_copy_oracle", target: "lh_ldz" },
  { source: "pl_copy_adls", target: "lh_ldz" },
  { source: "pl_copy_ftp", target: "lh_ldz" },
  { source: "pl_copy_sftp", target: "lh_ldz" },
  { source: "pl_copy_onelake_t", target: "lh_ldz" },
  { source: "pl_copy_onelake_f", target: "lh_ldz" },
  { source: "pl_copy_adf", target: "lh_ldz" },
  { source: "pl_copy_nb", target: "lh_ldz" },
  { source: "pl_load_brz", target: "nb_ldz_brz", label: "invokes" },
  { source: "pl_load_brz", target: "nb_parallel", label: "invokes" },
  { source: "lh_ldz", target: "nb_ldz_brz", label: "reads" },
  { source: "nb_ldz_brz", target: "lh_bronze", label: "writes" },
  { source: "pl_load_slv", target: "nb_brz_slv", label: "invokes" },
  { source: "pl_load_slv", target: "nb_dq", label: "invokes" },
  { source: "pl_load_slv", target: "nb_parallel", label: "invokes" },
  { source: "lh_bronze", target: "nb_brz_slv", label: "reads" },
  { source: "nb_brz_slv", target: "lh_silver", label: "writes" },
  { source: "nb_dq", target: "lh_silver", label: "validates" },
  { source: "nb_utility", target: "nb_ldz_brz", label: "%run", style: "dashed" },
  { source: "nb_utility", target: "nb_brz_slv", label: "%run", style: "dashed" },
  { source: "nb_utility", target: "nb_dq", label: "%run", style: "dashed" },
  { source: "var_fmd", target: "pl_load_all", label: "config", style: "dashed" },
  { source: "var_cfg_fmd", target: "pl_load_all", label: "config", style: "dashed" },
  { source: "con_fabric_sql", target: "sql_fmd", style: "dashed" },
  { source: "con_fabric_pipelines", target: "pl_load_all", label: "invoke", style: "dashed" },
  { source: "con_fabric_notebooks", target: "nb_ldz_brz", label: "invoke", style: "dashed" },
  { source: "pl_tooling", target: "sql_fmd", label: "writes" },
];

// ============================================================================
// HELPERS
// ============================================================================

function buildCyElements() {
  const elements: cytoscape.ElementDefinition[] = [];

  nodes.forEach((n) => {
    const t = NODE_TYPES[n.type];
    elements.push({
      group: "nodes",
      data: {
        id: n.id,
        label: n.techLabel,
        techLabel: n.techLabel,
        execLabel: n.execLabel,
        nodeType: n.type,
        color: t.color,
        nodeShape: t.shape,
        desc: n.desc,
        layer: n.layer,
      },
    });
  });

  edges.forEach((e, i) => {
    elements.push({
      group: "edges",
      data: {
        id: `e${i}`,
        source: e.source,
        target: e.target,
        label: e.label || "",
        edgeStyle: e.style || "solid",
      },
    });
  });

  return elements;
}

function getOrderedPath(cy: cytoscape.Core, nodeId: string): string[] {
  const visitedUp = new Set<string>();
  const visitedDown = new Set<string>();
  const upstream: string[] = [];
  const downstream: string[] = [];

  function walkUp(id: string) {
    if (visitedUp.has(id)) return;
    visitedUp.add(id);
    cy.getElementById(id).predecessors("node").forEach((p: cytoscape.NodeSingular) => {
      upstream.push(p.id());
      walkUp(p.id());
    });
  }
  walkUp(nodeId);

  function walkDown(id: string) {
    if (visitedDown.has(id)) return;
    visitedDown.add(id);
    cy.getElementById(id).successors("node").forEach((s: cytoscape.NodeSingular) => {
      downstream.push(s.id());
      walkDown(s.id());
    });
  }
  walkDown(nodeId);

  const ordered = [...[...new Set(upstream)].reverse(), nodeId, ...new Set(downstream)];
  const seen = new Set<string>();
  return ordered.filter((id) => {
    if (seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

// ============================================================================
// CYTOSCAPE STYLE (theme-aware via CSS class detection)
// ============================================================================

function getCyStyle(): any[] {
  const isDark = document.documentElement.classList.contains("dark");
  const edgeColor = isDark ? "rgba(255,255,255,0.22)" : "rgba(0,0,0,0.15)";
  const edgeLabelColor = isDark ? "rgba(255,255,255,0.5)" : "rgba(0,0,0,0.4)";
  const textOutline = isDark ? "#2B2A27" : "#F5F5F0";

  const selectColor = isDark ? "#F8FAFC" : "#0F172A";

  return [
    {
      selector: "node",
      style: {
        label: "data(label)",
        "text-wrap": "wrap",
        "text-valign": "center",
        "text-halign": "center",
        "font-size": "11px",
        "font-weight": 600,
        "font-family": "DM Sans, -apple-system, system-ui, sans-serif",
        color: "#fff",
        "text-outline-color": "data(color)",
        "text-outline-width": "2px",
        "background-color": "data(color)",
        "background-opacity": 0.95,
        "border-width": "2px",
        "border-color": isDark ? "rgba(255,255,255,0.2)" : "rgba(0,0,0,0.1)",
        "border-opacity": 1,
        shape: "data(nodeShape)" as unknown as cytoscape.Css.NodeShape,
        width: 160,
        height: 64,
        padding: "8px",
        "transition-property": "background-opacity, border-opacity, border-width, opacity, width, height",
        "transition-duration": "0.3s",
      },
    },
    // Per-type sizing & styling
    { selector: 'node[nodeType="source"]', style: { width: 155, height: 62, "font-size": "10px" } },
    { selector: 'node[nodeType="connection"]', style: { width: 145, height: 70, "font-size": "9px" } },
    { selector: 'node[nodeType="datasource"]', style: { width: 155, height: 64, "font-size": "10px" } },
    { selector: 'node[nodeType="orch"]', style: { width: 175, height: 72, "font-size": "12px", "font-weight": 700, "border-width": "3px", "border-color": isDark ? "rgba(255,255,255,0.3)" : "rgba(0,0,0,0.15)" } },
    { selector: 'node[nodeType="command"]', style: { width: 150, height: 64, "font-size": "10px" } },
    { selector: 'node[nodeType="copy"]', style: { width: 155, height: 60, "font-size": "10px" } },
    { selector: 'node[nodeType="lakehouse"]', style: { width: 180, height: 78, "font-size": "13px", "font-weight": 700, "border-width": "3px", "border-color": isDark ? "rgba(255,255,255,0.35)" : "rgba(0,0,0,0.18)" } },
    { selector: 'node[nodeType="notebook"]', style: { width: 165, height: 68, "font-size": "11px" } },
    { selector: 'node[nodeType="config"]', style: { width: 135, height: 56, "font-size": "9px", "background-opacity": 0.8 } },
    { selector: 'node[nodeType="tooling"]', style: { width: 140, height: 58, "font-size": "9px" } },
    {
      selector: "edge",
      style: {
        width: 2,
        "line-color": edgeColor,
        "target-arrow-color": edgeColor,
        "target-arrow-shape": "triangle",
        "arrow-scale": 0.9,
        "curve-style": "bezier",
        label: "data(label)",
        "font-size": "8px",
        color: edgeLabelColor,
        "text-rotation": "autorotate",
        "text-outline-color": textOutline,
        "text-outline-width": "2px",
        "transition-property": "line-color, target-arrow-color, width, opacity",
        "transition-duration": "0.3s",
      },
    },
    {
      selector: 'edge[edgeStyle="dashed"]',
      style: { "line-style": "dashed", "line-dash-pattern": [6, 4] as unknown as string },
    },
    // Highlighted
    {
      selector: "node.highlighted",
      style: { "background-opacity": 1, "border-width": "3px", "border-color": isDark ? "rgba(255,255,255,0.4)" : "rgba(0,0,0,0.2)", "z-index": 10 },
    },
    {
      selector: "node.selected-node",
      style: {
        "background-opacity": 1,
        "border-color": selectColor,
        "border-width": "3px",
        "z-index": 20,
        "overlay-color": selectColor,
        "overlay-opacity": 0.08,
        "overlay-padding": 8,
      },
    },
    {
      selector: "edge.highlighted",
      style: {
        "line-color": isDark ? "rgba(255,255,255,0.6)" : "rgba(0,0,0,0.5)",
        "target-arrow-color": isDark ? "rgba(255,255,255,0.6)" : "rgba(0,0,0,0.5)",
        width: 2.5,
        "z-index": 10,
        color: isDark ? "rgba(255,255,255,0.7)" : "rgba(0,0,0,0.6)",
        "text-outline-color": textOutline,
      },
    },
    {
      selector: "edge.flow-animated",
      style: { "line-color": isDark ? "rgba(255,255,255,0.6)" : "rgba(0,0,0,0.5)", "target-arrow-color": isDark ? "rgba(255,255,255,0.6)" : "rgba(0,0,0,0.5)", width: 2.5, "z-index": 10 },
    },
    // Isolated path — larger nodes
    {
      selector: "node.isolated-node",
      style: {
        width: 220,
        height: 90,
        "font-size": "14px",
        "border-width": "3px",
        "background-opacity": 1,
        "text-outline-width": "2px",
      },
    },
    // Dimmed
    { selector: "node.dimmed", style: { opacity: 0.06 } },
    { selector: "edge.dimmed", style: { opacity: 0.02 } },
    // Search
    {
      selector: "node.search-match",
      style: { "border-color": "#FBBF24", "border-width": "3px", "background-opacity": 1 },
    },
  ] as any[];
}

// ============================================================================
// FLOW DOT ANIMATION
// ============================================================================

interface FlowDot {
  srcX: number;
  srcY: number;
  tgtX: number;
  tgtY: number;
  progress: number;
  speed: number;
  edge: cytoscape.EdgeSingular;
}

function startFlowAnimation(
  container: HTMLElement,
  highlightedEdges: cytoscape.EdgeCollection,
  animIdRef: React.MutableRefObject<number | null>,
  dotsRef: React.MutableRefObject<FlowDot[]>
) {
  stopFlowAnimation(animIdRef, dotsRef);

  const canvas = document.createElement("canvas");
  canvas.id = "flowCanvas";
  canvas.style.cssText = "position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:4;";
  canvas.width = container.offsetWidth * window.devicePixelRatio;
  canvas.height = container.offsetHeight * window.devicePixelRatio;
  canvas.style.width = container.offsetWidth + "px";
  canvas.style.height = container.offsetHeight + "px";
  container.appendChild(canvas);

  const ctx = canvas.getContext("2d")!;
  ctx.scale(window.devicePixelRatio, window.devicePixelRatio);

  const dots: FlowDot[] = [];
  highlightedEdges.forEach((edge) => {
    const srcPos = edge.source().renderedPosition();
    const tgtPos = edge.target().renderedPosition();
    for (let i = 0; i < 3; i++) {
      dots.push({
        srcX: srcPos.x, srcY: srcPos.y,
        tgtX: tgtPos.x, tgtY: tgtPos.y,
        progress: (i * 0.33) % 1,
        speed: 0.004 + Math.random() * 0.002,
        edge,
      });
    }
  });
  dotsRef.current = dots;

  function animate() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    dotsRef.current.forEach((dot) => {
      const src = dot.edge.source().renderedPosition();
      const tgt = dot.edge.target().renderedPosition();
      dot.srcX = src.x;
      dot.srcY = src.y;
      dot.tgtX = tgt.x;
      dot.tgtY = tgt.y;

      dot.progress += dot.speed;
      if (dot.progress > 1) dot.progress -= 1;

      const x = dot.srcX + (dot.tgtX - dot.srcX) * dot.progress;
      const y = dot.srcY + (dot.tgtY - dot.srcY) * dot.progress;

      const gradient = ctx.createRadialGradient(x, y, 0, x, y, 10);
      gradient.addColorStop(0, "rgba(174,86,48,1)");
      gradient.addColorStop(0.35, "rgba(174,86,48,0.35)");
      gradient.addColorStop(1, "rgba(174,86,48,0)");
      ctx.beginPath();
      ctx.arc(x, y, 10, 0, Math.PI * 2);
      ctx.fillStyle = gradient;
      ctx.fill();

      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fillStyle = "#AE5630";
      ctx.fill();
      ctx.beginPath();
      ctx.arc(x, y, 1.5, 0, Math.PI * 2);
      ctx.fillStyle = "#fff";
      ctx.fill();
    });

    animIdRef.current = requestAnimationFrame(animate);
  }

  animate();
}

function stopFlowAnimation(
  animIdRef: React.MutableRefObject<number | null>,
  dotsRef: React.MutableRefObject<FlowDot[]>
) {
  if (animIdRef.current) {
    cancelAnimationFrame(animIdRef.current);
    animIdRef.current = null;
  }
  dotsRef.current = [];
  const canvas = document.getElementById("flowCanvas");
  if (canvas) canvas.remove();
}

// ============================================================================
// DETAIL PANEL COMPONENT
// ============================================================================

interface NodeDetail {
  id: string;
  execLabel: string;
  techLabel: string;
  desc: string;
  layer: string;
  nodeType: string;
  upstreamCount: number;
  downstreamCount: number;
  path: { id: string; label: string; color: string; isCurrent: boolean }[];
}

function DetailPanel({
  detail,
  labelMode,
  onSelectNode,
}: {
  detail: NodeDetail | null;
  labelMode: "tech" | "exec";
  onSelectNode: (id: string) => void;
}) {
  if (!detail) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-center text-muted-foreground gap-2 px-6">
        <Layers className="w-12 h-12 opacity-20" />
        <p className="text-[13px] leading-relaxed">
          Select a node to isolate its path
          <br />
          and see the full data flow
        </p>
      </div>
    );
  }

  const t = NODE_TYPES[detail.nodeType];

  return (
    <div className="animate-[fadeIn_0.2s_var(--ease-claude)]">
      <div className="mb-4">
        <div
          className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-[var(--radius-sm)] text-[10px] font-semibold uppercase tracking-wider mb-2"
          style={{ background: `${t.color}12`, color: t.color, border: `1px solid ${t.color}25` }}
        >
          <span className="w-1.5 h-1.5 rounded-full" style={{ background: t.color }} />
          {t.label}
        </div>
        <div className="text-base font-semibold tracking-tight text-foreground">
          {detail.execLabel.replace(/\n/g, " ")}
        </div>
        <div className="text-[11px] text-muted-foreground font-mono mt-1 px-1.5 py-0.5 bg-muted/50 rounded-[var(--radius-sm)] inline-block border border-border/50">
          {detail.techLabel.replace(/\n/g, " ")}
        </div>
        <div className="text-[13px] text-muted-foreground mt-2 leading-relaxed">{detail.desc}</div>
      </div>

      <div className="mt-4">
        <div className="grid grid-cols-2 gap-2">
          <div className="p-2.5 rounded-[var(--radius-md)] bg-muted/50 border border-border/50">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium">Layer</div>
            <div className="text-sm font-semibold mt-0.5">{detail.layer}</div>
          </div>
          <div className="p-2.5 rounded-[var(--radius-md)] bg-muted/50 border border-border/50">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium">Type</div>
            <div className="text-sm font-semibold mt-0.5">{t.label.split(" ")[0]}</div>
          </div>
          <div className="p-2.5 rounded-[var(--radius-md)] bg-muted/50 border border-border/50">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium">Upstream</div>
            <div className="text-sm font-semibold mt-0.5" style={{ color: "#3D8C5C" }}>{detail.upstreamCount}</div>
          </div>
          <div className="p-2.5 rounded-[var(--radius-md)] bg-muted/50 border border-border/50">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium">Downstream</div>
            <div className="text-sm font-semibold mt-0.5" style={{ color: "#3D7BB8" }}>{detail.downstreamCount}</div>
          </div>
        </div>
      </div>

      <div className="mt-4">
        <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground mb-2">
          Full Data Path ({detail.path.length} nodes)
        </div>
        <div className="flex flex-col gap-0.5">
          {detail.path.map((p, i) => (
            <div key={p.id}>
              <div
                className={`flex items-center gap-2 px-2.5 py-1.5 rounded-[var(--radius-md)] text-xs cursor-pointer transition-all duration-[var(--duration-fast)] border ${
                  p.isCurrent
                    ? "border-primary bg-primary/10 text-primary font-semibold"
                    : "border-transparent bg-muted/30 text-muted-foreground hover:bg-muted/60 hover:border-border hover:text-foreground"
                }`}
                onClick={() => onSelectNode(p.id)}
              >
                <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ background: p.color }} />
                <span>{p.label}</span>
              </div>
              {i < detail.path.length - 1 && (
                <div className="text-center text-muted-foreground/40 text-[10px] py-0">&#x25BC;</div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// AUDIT MATRIX COMPONENT
// ============================================================================

function AuditMatrix({
  cy: cyInstance,
  labelMode,
  onSelectNode,
}: {
  cy: cytoscape.Core | null;
  labelMode: "tech" | "exec";
  onSelectNode: (id: string) => void;
}) {
  if (!cyInstance) return null;

  return (
    <table className="w-full border-collapse text-[11px]">
      <thead>
        <tr>
          {["Type", "Name", "Layer", "In", "Out"].map((h) => (
            <th
              key={h}
              className="text-left px-2 py-1.5 font-semibold text-[10px] uppercase tracking-wider text-muted-foreground border-b border-border sticky top-0 bg-card z-[1]"
            >
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {nodes.map((n) => {
          const t = NODE_TYPES[n.type];
          const preds = cyInstance.getElementById(n.id).predecessors("node").length;
          const succs = cyInstance.getElementById(n.id).successors("node").length;
          const displayLabel = labelMode === "exec" ? n.execLabel : n.techLabel;
          return (
            <tr
              key={n.id}
              className="cursor-pointer hover:bg-muted/40 transition-colors duration-[var(--duration-fast)]"
              onClick={() => onSelectNode(n.id)}
            >
              <td className="px-2 py-1 border-b border-border/30">
                <span
                  className="inline-block px-1.5 py-px rounded-[var(--radius-sm)] text-[9px] font-semibold"
                  style={{ background: `${t.color}15`, color: t.color }}
                >
                  {t.label}
                </span>
              </td>
              <td className="px-2 py-1 border-b border-border/30 font-semibold text-foreground text-[11px]">
                {displayLabel.replace(/\n/g, " ")}
              </td>
              <td className="px-2 py-1 border-b border-border/30 text-muted-foreground">{n.layer}</td>
              <td className="px-2 py-1 border-b border-border/30 text-center text-muted-foreground">{preds}</td>
              <td className="px-2 py-1 border-b border-border/30 text-center text-muted-foreground">{succs}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ============================================================================
// MAIN COMPONENT
// ============================================================================

export default function FlowExplorer() {
  const cyRef = useRef<cytoscape.Core | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const graphWrapperRef = useRef<HTMLDivElement>(null);
  const flowAnimIdRef = useRef<number | null>(null);
  const flowDotsRef = useRef<FlowDot[]>([]);

  const [labelMode, setLabelMode] = useState<"tech" | "exec">("tech");
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [isIsolated, setIsIsolated] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [panelTab, setPanelTab] = useState<"detail" | "matrix">("detail");
  const [nodeDetail, setNodeDetail] = useState<NodeDetail | null>(null);
  const [stats, setStats] = useState({ nodes: 0, edges: 0, inPath: 0 });
  const [highlightedType, setHighlightedType] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<{ x: number; y: number; type: string; label: string; desc: string } | null>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);

  // Initialize Cytoscape
  useEffect(() => {
    if (!containerRef.current) return;

    const cy = cytoscape({
      container: containerRef.current,
      elements: buildCyElements(),
      style: getCyStyle(),
      layout: {
        name: "dagre",
        rankDir: "LR",
        nodeSep: 50,
        rankSep: 120,
        edgeSep: 25,
        animate: true,
        animationDuration: 600,
        fit: true,
        padding: 50,
      } as cytoscape.LayoutOptions,
      minZoom: 0.15,
      maxZoom: 3,
      wheelSensitivity: 0.3,
    });

    cyRef.current = cy;
    setStats({ nodes: cy.nodes().length, edges: cy.edges().length, inPath: 0 });

    // Node click
    cy.on("tap", "node", (e) => {
      selectNode(e.target.id());
    });

    // Background click
    cy.on("tap", (e) => {
      if (e.target === cy) restoreFullView();
    });

    // Hover tooltip + cursor
    cy.on("mouseover", "node", (e) => {
      const node = e.target;
      const data = node.data();
      const nt = NODE_TYPES[data.nodeType];
      const renderedPos = node.renderedPosition();
      const container = containerRef.current;
      if (!container) return;
      container.style.cursor = "pointer";
      const rect = container.getBoundingClientRect();
      setTooltip({
        x: rect.left + renderedPos.x,
        y: rect.top + renderedPos.y - node.renderedHeight() / 2 - 12,
        type: nt?.label || data.nodeType,
        label: data.techLabel?.replace(/\n/g, " ") || data.label,
        desc: data.desc || "",
      });
    });
    cy.on("mouseout", "node", () => {
      setTooltip(null);
      if (containerRef.current) containerRef.current.style.cursor = "default";
    });
    cy.on("viewport", () => {
      setTooltip(null);
    });

    return () => {
      stopFlowAnimation(flowAnimIdRef, flowDotsRef);
      cy.destroy();
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Theme observer — update styles when dark/light changes
  useEffect(() => {
    const observer = new MutationObserver(() => {
      if (cyRef.current) {
        cyRef.current.style(getCyStyle());
      }
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => observer.disconnect();
  }, []);

  const buildNodeDetail = useCallback(
    (nodeId: string): NodeDetail | null => {
      const cy = cyRef.current;
      if (!cy) return null;

      const node = cy.getElementById(nodeId);
      const data = node.data();
      const path = getOrderedPath(cy, nodeId);

      return {
        id: nodeId,
        execLabel: data.execLabel,
        techLabel: data.techLabel,
        desc: data.desc,
        layer: data.layer,
        nodeType: data.nodeType,
        upstreamCount: node.predecessors("node").length,
        downstreamCount: node.successors("node").length,
        path: path.map((id) => {
          const n = cy.getElementById(id).data();
          const nt = NODE_TYPES[n.nodeType];
          const displayLabel = labelMode === "exec" ? n.execLabel : n.techLabel;
          return {
            id,
            label: displayLabel.replace(/\n/g, " "),
            color: nt.color,
            isCurrent: id === nodeId,
          };
        }),
      };
    },
    [labelMode]
  );

  const selectNode = useCallback(
    (nodeId: string) => {
      const cy = cyRef.current;
      if (!cy || !graphWrapperRef.current) return;

      cy.elements().removeClass("highlighted selected-node dimmed flow-animated isolated-node");
      stopFlowAnimation(flowAnimIdRef, flowDotsRef);

      const node = cy.getElementById(nodeId);
      const predecessors = node.predecessors();
      const successors = node.successors();
      const connected = predecessors.union(successors).union(node);
      const uniquePath = getOrderedPath(cy, nodeId);

      cy.elements().addClass("dimmed");
      connected.removeClass("dimmed");
      connected.nodes().addClass("highlighted isolated-node");
      connected.edges().addClass("highlighted flow-animated");
      node.addClass("selected-node").removeClass("highlighted");

      const spacing = 220;
      const totalWidth = (uniquePath.length - 1) * spacing;
      const startX = -totalWidth / 2;

      const animPromises: Promise<unknown>[] = [];
      uniquePath.forEach((id, i) => {
        const n = cy.getElementById(id);
        animPromises.push(
          n
            .animation({ position: { x: startX + i * spacing, y: 0 }, duration: 600, easing: "ease-in-out-cubic" } as any)
            .play()
            .promise()
        );
      });

      Promise.all(animPromises).then(() => {
        cy.animate({ fit: { eles: connected.nodes(), padding: 80 }, duration: 400 });
        setTimeout(() => {
          if (graphWrapperRef.current) {
            startFlowAnimation(graphWrapperRef.current, connected.edges(), flowAnimIdRef, flowDotsRef);
          }
        }, 200);
      });

      setSelectedNodeId(nodeId);
      setIsIsolated(true);
      setStats((s) => ({ ...s, inPath: connected.nodes().length }));
      setNodeDetail(buildNodeDetail(nodeId));
      setPanelTab("detail");
    },
    [buildNodeDetail]
  );

  const restoreFullView = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;

    cy.elements().removeClass("highlighted selected-node dimmed search-match flow-animated isolated-node");
    stopFlowAnimation(flowAnimIdRef, flowDotsRef);

    cy.layout({
      name: "dagre",
      rankDir: "LR",
      nodeSep: 30,
      rankSep: 80,
      edgeSep: 15,
      animate: true,
      animationDuration: 600,
      fit: true,
      padding: 40,
    } as cytoscape.LayoutOptions).run();

    setSelectedNodeId(null);
    setIsIsolated(false);
    setNodeDetail(null);
    setStats((s) => ({ ...s, inPath: 0 }));
    setHighlightedType(null);
  }, []);

  // Label mode toggle
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;

    cy.nodes().forEach((n) => {
      const newLabel = labelMode === "exec" ? n.data("execLabel") : n.data("techLabel");
      n.data("label", newLabel);
    });

    if (selectedNodeId) {
      setNodeDetail(buildNodeDetail(selectedNodeId));
    }
  }, [labelMode, selectedNodeId, buildNodeDetail]);

  // Search
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;

    cy.nodes().removeClass("search-match");
    if (!searchQuery.trim()) return;

    const q = searchQuery.toLowerCase().trim();
    cy.nodes().forEach((n) => {
      const techLabel = (n.data("techLabel") || "").toLowerCase();
      const execLabel = (n.data("execLabel") || "").toLowerCase();
      const desc = (n.data("desc") || "").toLowerCase();
      const layer = (n.data("layer") || "").toLowerCase();
      if (techLabel.includes(q) || execLabel.includes(q) || desc.includes(q) || layer.includes(q)) {
        n.addClass("search-match");
      }
    });
  }, [searchQuery]);

  const handleLegendClick = useCallback(
    (typeKey: string) => {
      if (highlightedType === typeKey) {
        restoreFullView();
        return;
      }
      restoreFullView();
      setTimeout(() => {
        const cy = cyRef.current;
        if (!cy) return;
        cy.elements().addClass("dimmed");
        const typeNodes = cy.nodes(`[nodeType="${typeKey}"]`);
        typeNodes.removeClass("dimmed").addClass("highlighted");
        typeNodes.connectedEdges().removeClass("dimmed");
        setStats((s) => ({ ...s, inPath: typeNodes.length }));
        setHighlightedType(typeKey);
      }, 700);
    },
    [highlightedType, restoreFullView]
  );

  return (
    <div className="flex flex-col h-[calc(100vh-48px)]">
      {/* Toolbar */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-border bg-card/50 flex-shrink-0">
        <div className="flex items-center gap-4">
          <div>
            <h1 className="font-display text-base font-semibold tracking-tight">Flow Explorer</h1>
            <p className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium">Pipeline Dependency Map</p>
          </div>
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground pl-4 border-l border-border">
            <span className="w-1.5 h-1.5 rounded-full bg-[var(--cl-success,#3D8C5C)] inline-block" />
            <span className="font-mono text-[10px]">{stats.nodes} Nodes</span>
            <span className="text-border mx-1">|</span>
            <span className="font-mono text-[10px]">{stats.edges} Edges</span>
            {stats.inPath > 0 && (
              <>
                <span className="text-border mx-1">|</span>
                <span className="font-mono text-[10px] text-primary">{stats.inPath} In Path</span>
              </>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2">
          {/* Tech / Exec Toggle */}
          <div className="flex items-center gap-2 px-3 border-l border-r border-border h-8">
            <span
              className={`text-[10px] uppercase tracking-wider cursor-pointer font-medium transition-colors ${
                labelMode === "tech" ? "text-primary font-semibold" : "text-muted-foreground"
              }`}
              onClick={() => setLabelMode("tech")}
            >
              Technical
            </span>
            <div
              className={`relative w-10 h-5 rounded-full cursor-pointer transition-all duration-[var(--duration-smooth)] ${
                labelMode === "exec"
                  ? "bg-primary/15 border border-primary/30"
                  : "bg-muted border border-border"
              }`}
              onClick={() => setLabelMode((m) => (m === "tech" ? "exec" : "tech"))}
            >
              <div
                className={`absolute top-0.5 w-3.5 h-3.5 rounded-full transition-all duration-[var(--duration-smooth)] ${
                  labelMode === "exec"
                    ? "left-[22px] bg-primary shadow-[0_0_6px_rgba(174,86,48,0.25)]"
                    : "left-0.5 bg-muted-foreground"
                }`}
              />
            </div>
            <span
              className={`text-[10px] uppercase tracking-wider cursor-pointer font-medium transition-colors ${
                labelMode === "exec" ? "text-primary font-semibold" : "text-muted-foreground"
              }`}
              onClick={() => setLabelMode("exec")}
            >
              Executive
            </span>
          </div>

          <button
            className="inline-flex items-center gap-1.5 px-3 h-8 rounded-[var(--radius-md)] border border-border bg-card text-muted-foreground text-xs font-medium cursor-pointer transition-all duration-[var(--duration-normal)] hover:bg-muted hover:text-foreground hover:border-border active:scale-95 shadow-[var(--shadow-sm)]"
            onClick={() => cyRef.current?.animate({ fit: { eles: cyRef.current.elements(), padding: 40 }, duration: 400 })}
          >
            <Maximize className="w-3.5 h-3.5" />
            Fit
          </button>
          <button
            className="inline-flex items-center gap-1.5 px-3 h-8 rounded-[var(--radius-md)] border border-border bg-card text-muted-foreground text-xs font-medium cursor-pointer transition-all duration-[var(--duration-normal)] hover:bg-muted hover:text-foreground hover:border-border active:scale-95 shadow-[var(--shadow-sm)]"
            onClick={() => {
              restoreFullView();
              setSearchQuery("");
            }}
          >
            <RotateCcw className="w-3.5 h-3.5" />
            Clear
          </button>
        </div>
      </div>

      {/* Main Area */}
      <div className="flex flex-1 min-h-0">
        {/* Graph */}
        <div className="flex-1 relative bg-background" ref={graphWrapperRef}>
          {/* Legend */}
          <div className="absolute top-3 left-3 flex gap-1 flex-wrap z-[5] pointer-events-none">
            {Object.entries(NODE_TYPES).map(([key, val]) => (
              <div
                key={key}
                className={`flex items-center gap-1.5 px-2 py-1 rounded-[var(--radius-sm)] text-[10px] font-medium pointer-events-auto cursor-pointer transition-all duration-[var(--duration-normal)] border backdrop-blur-sm ${
                  highlightedType === key
                    ? "border-primary text-primary bg-primary/5"
                    : "border-border/30 text-muted-foreground bg-card/80 hover:bg-card hover:text-foreground hover:border-border"
                }`}
                style={{ boxShadow: "var(--shadow-sm)" }}
                onClick={() => handleLegendClick(key)}
              >
                <span className="w-[7px] h-[7px] rounded-full flex-shrink-0" style={{ background: val.color }} />
                {val.label}
              </div>
            ))}
          </div>

          {/* Mode Badge */}
          <div
            className={`absolute top-3 right-3 px-3 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider z-[5] pointer-events-none backdrop-blur-sm border ${
              labelMode === "exec"
                ? "bg-primary/8 text-primary border-primary/15"
                : "bg-[var(--cl-info,#3D7BB8)]/8 text-[var(--cl-info,#3D7BB8)] border-[var(--cl-info,#3D7BB8)]/15"
            }`}
            style={{ boxShadow: "var(--shadow-sm)" }}
          >
            {labelMode === "exec" ? "Executive View" : "Technical View"}
          </div>

          {/* Cytoscape container */}
          <div ref={containerRef} className="w-full h-full" />

          {/* Hover tooltip */}
          {tooltip && (
            <div
              ref={tooltipRef}
              className="fixed z-[100] pointer-events-none"
              style={{
                left: tooltip.x,
                top: tooltip.y,
                transform: "translate(-50%, -100%)",
              }}
            >
              <div className="bg-card/95 backdrop-blur-md border border-border rounded-[var(--radius-md)] shadow-[var(--shadow-lg)] px-3 py-2.5 max-w-[280px]">
                <div className="text-[10px] font-semibold uppercase tracking-wider text-primary mb-1">{tooltip.type}</div>
                <div className="text-xs font-semibold text-foreground leading-tight">{tooltip.label}</div>
                {tooltip.desc && (
                  <div className="text-[11px] text-muted-foreground mt-1.5 leading-snug">{tooltip.desc}</div>
                )}
              </div>
              <div className="w-2 h-2 bg-card/95 border-r border-b border-border rotate-45 mx-auto -mt-1" />
            </div>
          )}

          {/* Hint */}
          {!isIsolated && (
            <div className="absolute bottom-4 left-1/2 -translate-x-1/2 px-4 py-1.5 rounded-full bg-card/90 backdrop-blur-sm text-xs text-muted-foreground z-[5] pointer-events-none border border-border/30 shadow-[var(--shadow-sm)] transition-opacity duration-500">
              Click any node to isolate and zoom into its data path
            </div>
          )}

          {/* Back button */}
          {isIsolated && (
            <button
              className="absolute bottom-4 left-1/2 -translate-x-1/2 px-5 py-2 rounded-full bg-primary text-primary-foreground text-xs font-semibold z-[6] cursor-pointer border-none shadow-[0_2px_12px_rgba(174,86,48,0.3),0_0_0_0.5px_rgba(174,86,48,0.2)] transition-all duration-[var(--duration-normal)] hover:shadow-[0_4px_16px_rgba(174,86,48,0.4)] hover:-translate-x-1/2 hover:-translate-y-px active:scale-95"
              onClick={restoreFullView}
            >
              Back to Full View
            </button>
          )}
        </div>

        {/* Right Panel */}
        <div className="w-[340px] border-l border-border bg-card flex flex-col flex-shrink-0">
          {/* Search */}
          <div className="px-4 py-2 border-b border-border flex-shrink-0">
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground" />
              <input
                type="text"
                className="w-full pl-8 pr-3 py-1.5 rounded-[var(--radius-md)] border border-border bg-background text-foreground text-xs outline-none transition-all duration-[var(--duration-normal)] focus:border-[var(--cl-border-focus,rgba(0,0,0,0.2))] focus:shadow-[0_0_0_3px_var(--cl-accent-soft,rgba(174,86,48,0.1))] placeholder:text-muted-foreground"
                placeholder="Search nodes..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </div>
          </div>

          {/* Tabs */}
          <div className="flex border-b border-border flex-shrink-0">
            {(["detail", "matrix"] as const).map((tab) => (
              <button
                key={tab}
                className={`flex-1 px-3 py-2.5 text-center text-xs font-medium cursor-pointer transition-all duration-[var(--duration-normal)] border-b-2 ${
                  panelTab === tab
                    ? "text-primary border-primary"
                    : "text-muted-foreground border-transparent hover:text-foreground"
                }`}
                onClick={() => setPanelTab(tab)}
              >
                {tab === "detail" ? "Details" : "Audit Matrix"}
              </button>
            ))}
          </div>

          {/* Panel Content */}
          <div className="flex-1 overflow-y-auto p-4">
            {panelTab === "detail" ? (
              <DetailPanel detail={nodeDetail} labelMode={labelMode} onSelectNode={selectNode} />
            ) : (
              <AuditMatrix cy={cyRef.current} labelMode={labelMode} onSelectNode={(id) => { selectNode(id); setPanelTab("detail"); }} />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
