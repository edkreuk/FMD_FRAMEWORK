"""FMD Framework - Comprehensive Miro Architecture Board"""
import urllib.request
import urllib.error
import json
import time
import sys
import os

# Fix Windows cp1252 encoding issues
os.environ["PYTHONIOENCODING"] = "utf-8"
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "miro_board.log")
log_fh = open(LOG_FILE, "w", encoding="utf-8")

def log(msg):
    log_fh.write(msg + "\n")
    log_fh.flush()
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'replace').decode())

TOKEN = "eyJtaXJvLm9yaWdpbiI6ImV1MDEifQ_jbnCM-R9eAU0j9EFV4pY7IwO3QI"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api(method, path, data=None, retries=2):
    url = f"https://api.miro.com/v2{path}"
    body = json.dumps(data).encode() if data else None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=body, headers=HEADERS, method=method)
        try:
            resp = urllib.request.urlopen(req)
            result = resp.read().decode()
            return resp.status, json.loads(result) if result else {}
        except urllib.error.HTTPError as e:
            err = e.read().decode()[:300]
            if e.code == 429 and attempt < retries:
                log(f"    Rate limited, waiting 2s...")
                time.sleep(2)
                continue
            return e.code, err
    return 0, "Max retries"

def shape(board_id, content, x, y, w, h, fill="#ffffff", border="#000000", font_size="14", shape_type="rectangle"):
    data = {
        "data": {"shape": shape_type, "content": content},
        "style": {"fillColor": fill, "borderColor": border, "fontSize": font_size, "textAlign": "center", "textAlignVertical": "middle"},
        "position": {"x": x, "y": y, "origin": "center"},
        "geometry": {"width": w, "height": h}
    }
    code, resp = api("POST", f"/boards/{board_id}/shapes", data)
    item_id = resp.get("id", "?") if isinstance(resp, dict) else "?"
    if code in [200, 201]:
        log(f"    Shape: {content[:40]:40} id={item_id}")
    else:
        log(f"    Shape FAIL ({code}): {resp}")
    time.sleep(0.3)
    return item_id

def sticky(board_id, content, x, y, color="light_yellow", w=200):
    data = {
        "data": {"content": content, "shape": "square"},
        "style": {"fillColor": color},
        "position": {"x": x, "y": y, "origin": "center"},
        "geometry": {"width": w}
    }
    code, resp = api("POST", f"/boards/{board_id}/sticky_notes", data)
    item_id = resp.get("id", "?") if isinstance(resp, dict) else "?"
    if code in [200, 201]:
        log(f"    Sticky: {content[:40]:40} id={item_id}")
    else:
        log(f"    Sticky FAIL ({code}): {resp}")
    time.sleep(0.3)
    return item_id

def connector(board_id, start_id, end_id, caption="", style="straight"):
    data = {
        "startItem": {"id": str(start_id), "snapTo": "auto"},
        "endItem": {"id": str(end_id), "snapTo": "auto"},
        "shape": style,
        "style": {"strokeColor": "#333333", "strokeWidth": "2"}
    }
    if caption:
        data["captions"] = [{"content": caption, "textAlignVertical": "middle"}]
    code, resp = api("POST", f"/boards/{board_id}/connectors", data)
    if code in [200, 201]:
        log(f"    Connector: {start_id} -> {end_id} {'(' + caption + ')' if caption else ''}")
    else:
        log(f"    Connector FAIL ({code}): {resp}")
    time.sleep(0.3)

def text(board_id, content, x, y, font_size="24", w=400):
    data = {
        "data": {"content": content},
        "style": {"fontSize": font_size, "textAlign": "left"},
        "position": {"x": x, "y": y, "origin": "center"},
        "geometry": {"width": w}
    }
    code, resp = api("POST", f"/boards/{board_id}/texts", data)
    if code in [200, 201]:
        log(f"    Text: {content[:50]}")
    else:
        log(f"    Text FAIL ({code}): {resp}")
    time.sleep(0.3)

def frame(board_id, title, x, y, w, h):
    data = {
        "data": {"title": title, "format": "custom", "type": "freeform"},
        "position": {"x": x, "y": y, "origin": "center"},
        "geometry": {"width": w, "height": h}
    }
    code, resp = api("POST", f"/boards/{board_id}/frames", data)
    if code in [200, 201]:
        log(f"    Frame: {title}")
    else:
        log(f"    Frame FAIL ({code}): {resp}")
    time.sleep(0.3)


# ============================================================
# CREATE BOARD
# ============================================================
log("=== Creating FMD Framework Architecture Board ===")
code, board = api("POST", "/boards", {
    "name": "FMD Framework — Architecture & Game Plan",
    "description": "Fabric Metadata-Driven Framework: Architecture, Pipelines, Workspaces, Data Sources, and 6-Week Game Plan"
})
if code not in [200, 201]:
    log(f"FAILED to create board: {code} {board}")
    exit(1)

BOARD = board["id"]
log(f"Board created: {board['name']} (id: {BOARD})")
log(f"URL: {board.get('viewLink', 'N/A')}")
log("")

# ============================================================
# SECTION 1: MEDALLION ARCHITECTURE FLOW
# ============================================================
log("\n=== Section 1: Medallion Architecture Flow ===")
frame(BOARD, "1. Medallion Architecture — Data Flow", 0, 0, 2400, 700)

# Title
text(BOARD, "<b>FMD Medallion Architecture</b>", 0, -280, "36", 800)
text(BOARD, "Metadata-driven ingestion from source to analytics-ready gold layer", 0, -230, "14", 800)

# Source systems box
src = shape(BOARD, "<b>Source Systems</b><br>SQL Server<br>ADLS<br>FTP/SFTP<br>Oracle<br>OneLake<br>ADF", -900, 50, 250, 300, "#e8e8e8", "#666666", "12")

# Landing Zone
lz = shape(BOARD, "<b>Landing Zone</b><br>LH_DATA_LANDINGZONE<br><br>Raw files<br>as-is from source", -500, 50, 250, 300, "#fff3cd", "#f0ad4e", "12")

# Bronze
br = shape(BOARD, "<b>Bronze Layer</b><br>LH_BRONZE_LAYER<br><br>Exact copy<br>Append-only<br>Full history", -100, 50, 250, 300, "#cd7f32", "#8B4513", "12")

# Silver
sv = shape(BOARD, "<b>Silver Layer</b><br>LH_SILVER_LAYER<br><br>Cleaned & validated<br>SCD Type 2<br>Deduplication", 300, 50, 250, 300, "#C0C0C0", "#808080", "12")

# Gold
gd = shape(BOARD, "<b>Gold Layer</b><br>LH_GOLD_LAYER<br><br>Star schema<br>MLVs<br>Business-ready", 700, 50, 250, 300, "#FFD700", "#DAA520", "12")

# Power BI
pbi = shape(BOARD, "<b>Power BI</b><br>Semantic Models<br>Reports<br>Dashboards", 1050, 50, 200, 200, "#F2C811", "#333333", "12")

# Connectors
connector(BOARD, src, lz, "Extract")
connector(BOARD, lz, br, "Load")
connector(BOARD, br, sv, "Transform")
connector(BOARD, sv, gd, "Curate")
connector(BOARD, gd, pbi, "Visualize")

# Metadata DB below
meta = shape(BOARD, "<b>SQL_FMD_FRAMEWORK</b><br>Metadata Database<br><br>Drives all pipeline behavior<br>Connections, Entities, Rules", 0, 350, 400, 150, "#d4edda", "#28a745", "12")
connector(BOARD, meta, lz, "Config")
connector(BOARD, meta, br, "Config")
connector(BOARD, meta, sv, "Config")

# DQ Engine
dq = shape(BOARD, "<b>DQ Engine</b><br>NB_FMD_DQ_CLEANSING<br>Quality checks<br>Outlier detection", 450, 350, 250, 120, "#f8d7da", "#dc3545", "11")
connector(BOARD, dq, sv, "Validate")


# ============================================================
# SECTION 2: WORKSPACE LAYOUT
# ============================================================
log("\n=== Section 2: Workspace Layout ===")
Y2 = 1000
frame(BOARD, "2. Fabric Workspace Organization", 0, Y2, 2400, 900)

text(BOARD, "<b>Workspace Architecture</b>", 0, Y2 - 380, "36", 800)
text(BOARD, "6 workspaces across Dev/Prod environments + Config", 0, Y2 - 330, "14", 800)

# Integration Code
ws_code = shape(BOARD, "<b>INTEGRATION CODE (D/P)</b><br>2bdaef03-9e79-...<br><br>• 7 Notebooks<br>• 26 Data Pipelines<br>• 2 Variable Libraries<br>• Environments", -700, Y2, 350, 350, "#cce5ff", "#004085", "11")

# Integration Data
ws_data = shape(BOARD, "<b>INTEGRATION DATA (D/P)</b><br>40e27fdc-775a-...<br><br>• LH_DATA_LANDINGZONE<br>• LH_BRONZE_LAYER<br>• LH_SILVER_LAYER", -200, Y2, 350, 350, "#d4edda", "#155724", "11")

# Integration Config
ws_config = shape(BOARD, "<b>INTEGRATION CONFIG</b><br>7774469b-0c95-...<br><br>• SQL_FMD_FRAMEWORK<br>• Metadata tables<br>• Stored procedures", 300, Y2, 350, 350, "#fff3cd", "#856404", "11")

# Business Domain
ws_biz = shape(BOARD, "<b>BUSINESS DOMAIN</b><br>DATA / CODE / REPORTING<br><br>• LH_GOLD_LAYER<br>• NB_LOAD_GOLD<br>• NB_CREATE_SHORTCUTS<br>• Semantic Models<br>• Power BI Reports", 800, Y2, 350, 350, "#f8d7da", "#721c24", "11")

# Connectors between workspaces
connector(BOARD, ws_code, ws_data, "Pipelines execute")
connector(BOARD, ws_config, ws_code, "Config drives")
connector(BOARD, ws_data, ws_biz, "Silver to Gold")

# Service Principal
sp = shape(BOARD, "<b>Service Principal</b><br>Fabric-PowerBI-API<br>91741faf-f475-...<br>Contributor on all workspaces", 0, Y2 + 300, 500, 100, "#e2e3e5", "#383d41", "11")


# ============================================================
# SECTION 3: PIPELINE MAP
# ============================================================
log("\n=== Section 3: Pipeline Map ===")
Y3 = 2400
frame(BOARD, "3. Pipeline Architecture — 26 Generic Pipelines", 0, Y3, 2800, 1200)

text(BOARD, "<b>FMD Pipeline Map</b>", 0, Y3 - 530, "36", 800)
text(BOARD, "All pipelines are generic — behavior driven entirely by metadata database", 0, Y3 - 480, "14", 800)

# COMMAND Pipelines (trigger extraction)
cmd_header = shape(BOARD, "<b>COMMAND Pipelines</b><br>(Trigger extraction)", -1000, Y3 - 300, 300, 80, "#6c757d", "#343a40", "12")

cmds = [
    ("PL_FMD_LDZ_COMMAND_ADLS", "ADLS"),
    ("PL_FMD_LDZ_COMMAND_ASQL", "Azure SQL"),
    ("PL_FMD_LDZ_COMMAND_FTP", "FTP"),
    ("PL_FMD_LDZ_COMMAND_SFTP", "SFTP"),
    ("PL_FMD_LDZ_COMMAND_ONELAKE", "OneLake"),
    ("PL_FMD_LDZ_COMMAND_ORACLE", "Oracle"),
    ("PL_FMD_LDZ_COMMAND_ADF", "ADF"),
    ("PL_FMD_LDZ_COMMAND_NOTEBOOK", "Notebook"),
]

cmd_ids = []
for i, (name, label) in enumerate(cmds):
    cid = shape(BOARD, f"<b>{label}</b><br>{name}", -1000, Y3 - 180 + i * 80, 280, 60, "#e9ecef", "#495057", "10")
    cmd_ids.append(cid)

# COPY_FROM Pipelines (execute copy)
copy_header = shape(BOARD, "<b>COPY_FROM Pipelines</b><br>(Execute data copy)", -400, Y3 - 300, 300, 80, "#17a2b8", "#117a8b", "12")

copies = [
    ("PL_FMD_LDZ_COPY_FROM_ADLS_01", "ADLS"),
    ("PL_FMD_LDZ_COPY_FROM_ASQL_01", "Azure SQL"),
    ("PL_FMD_LDZ_COPY_FROM_FTP_01", "FTP"),
    ("PL_FMD_LDZ_COPY_FROM_SFTP_01", "SFTP"),
    ("PL_FMD_LDZ_COPY_FROM_ONELAKE_TABLES_01", "OneLake Tables"),
    ("PL_FMD_LDZ_COPY_FROM_ONELAKE_FILES_01", "OneLake Files"),
    ("PL_FMD_LDZ_COPY_FROM_ORACLE_01", "Oracle"),
    ("PL_FMD_LDZ_COPY_FROM_ADF", "ADF"),
    ("PL_FMD_LDZ_COPY_FROM_CUSTOM_NB", "Custom NB"),
]

copy_ids = []
for i, (name, label) in enumerate(copies):
    cid = shape(BOARD, f"<b>{label}</b><br>{name}", -400, Y3 - 180 + i * 70, 300, 55, "#d1ecf1", "#0c5460", "10")
    copy_ids.append(cid)

# Connect COMMAND → COPY_FROM where applicable
for i in range(min(len(cmd_ids), 7)):
    connector(BOARD, cmd_ids[i], copy_ids[i])

# LOAD Pipelines
load_header = shape(BOARD, "<b>LOAD Pipelines</b><br>(Orchestrate medallion layers)", 250, Y3 - 300, 300, 80, "#28a745", "#1e7e34", "12")

load_lz = shape(BOARD, "<b>PL_FMD_LOAD_LANDINGZONE</b><br>Processes raw files from LZ", 250, Y3 - 150, 350, 70, "#d4edda", "#155724", "10")
load_br = shape(BOARD, "<b>PL_FMD_LOAD_BRONZE</b><br>Landing Zone → Bronze", 250, Y3 - 50, 350, 70, "#d4edda", "#155724", "10")
load_sv = shape(BOARD, "<b>PL_FMD_LOAD_SILVER</b><br>Bronze to Silver", 250, Y3 + 50, 350, 70, "#d4edda", "#155724", "10")
load_all = shape(BOARD, "<b>PL_FMD_LOAD_ALL</b><br>Full pipeline orchestration", 250, Y3 + 170, 350, 70, "#28a745", "#1e7e34", "10")

connector(BOARD, load_lz, load_br)
connector(BOARD, load_br, load_sv)
connector(BOARD, load_all, load_lz, "Triggers")

# Connect COPY to LOAD
connector(BOARD, copy_ids[0], load_lz, "→ LZ")

# TOOLING
tool = shape(BOARD, "<b>PL_TOOLING_POST_ASQL_TO_FMD</b><br>Azure SQL → FMD integration", 250, Y3 + 300, 350, 70, "#fff3cd", "#856404", "10")

# Notebooks
nb_header = shape(BOARD, "<b>Processing Notebooks</b><br>(Spark execution engine)", 800, Y3 - 300, 300, 80, "#e83e8c", "#a71d5d", "12")

nbs = [
    ("NB_FMD_PROCESSING_PARALLEL_MAIN", "Parallel executor (max 50)"),
    ("NB_FMD_PROCESSING_LANDINGZONE_MAIN", "LZ file processor"),
    ("NB_FMD_LOAD_LANDING_BRONZE", "LZ → Bronze loader"),
    ("NB_FMD_LOAD_BRONZE_SILVER", "Bronze to Silver transformer"),
    ("NB_FMD_DQ_CLEANSING", "Data quality & cleansing"),
    ("NB_FMD_CUSTOM_NOTEBOOK_TEMPLATE", "Custom logic template"),
    ("NB_UTILITIES_SETUP_FMD", "Shared utility functions"),
]

nb_ids = []
for i, (name, desc) in enumerate(nbs):
    nid = shape(BOARD, f"<b>{name}</b><br>{desc}", 800, Y3 - 180 + i * 85, 380, 65, "#fce4ec", "#880e4f", "10")
    nb_ids.append(nid)

# Connect LOAD → Notebooks
connector(BOARD, load_lz, nb_ids[1], "Triggers")
connector(BOARD, load_br, nb_ids[2], "Triggers")
connector(BOARD, load_sv, nb_ids[3], "Triggers")


# ============================================================
# SECTION 4: GAME PLAN TIMELINE
# ============================================================
log("\n=== Section 4: Game Plan Timeline ===")
Y4 = 4200
frame(BOARD, "4. Six-Week Game Plan — Feb 18 to Mar 31", 0, Y4, 2800, 800)

text(BOARD, "<b>Execution Timeline</b>", 0, Y4 - 340, "36", 800)
text(BOARD, "Critical path: MT-15 → MT-16 → MT-17 → MT-18 → MT-19 → MT-20 → MT-55", 0, Y4 - 290, "14", 800)

weeks = [
    ("Week 1\n2/18 - 2/21", "MT-15: SQL Connections\nMT-16: Register Sources", "#dc3545", -1050),
    ("Week 2\n2/21 - 2/25", "MT-17: LZ + Bronze Pipelines\nMT-50: DQ Rules (parallel)", "#fd7e14", -630),
    ("Week 3\n2/25 - 3/6", "MT-18: Silver Transforms\nMT-19: Gold MLVs\nMT-53: Taskflow", "#ffc107", -210),
    ("Week 4\n3/6 - 3/13", "MT-51: Business Domains\nMT-57: Data Backfill\nMT-54: Security", "#28a745", 210),
    ("Week 5\n3/13 - 3/17", "MT-20: E2E Testing\nMT-52: Semantic Models\nMT-52: Power BI", "#17a2b8", 630),
    ("Week 6\n3/17 - 3/31", "MT-55: Prod Cutover\nMT-56: Runbooks\nMT-21: Console UI", "#6f42c1", 1050),
]

week_ids = []
for label, details, color, x_off in weeks:
    wid = shape(BOARD, f"<b>{label}</b>", x_off, Y4 - 150, 350, 80, color, color, "14")
    did = shape(BOARD, details, x_off, Y4 + 20, 350, 200, "#f8f9fa", color, "11")
    week_ids.append((wid, did))

# Connect weeks sequentially
for i in range(len(week_ids) - 1):
    connector(BOARD, week_ids[i][0], week_ids[i+1][0])

# Status indicators
shape(BOARD, "<b>✓ DONE</b>\nMT-13: Research\nMT-14: Deploy FMD", -1050, Y4 + 280, 350, 100, "#d4edda", "#28a745", "11")
shape(BOARD, "<b>→ NOW</b>\nMT-15: SQL Connections\n(Gateway config)", -630, Y4 + 280, 350, 100, "#cce5ff", "#004085", "11")
shape(BOARD, "<b>GO-LIVE TARGET</b>\nMarch 31, 2026", 1050, Y4 + 280, 350, 100, "#f8d7da", "#721c24", "14")


# ============================================================
# SECTION 5: DATA SOURCE CONNECTIONS
# ============================================================
log("\n=== Section 5: Data Source Connections ===")
Y5 = 5600
frame(BOARD, "5. On-Prem Data Sources → Fabric Gateway → FMD", 0, Y5, 2400, 800)

text(BOARD, "<b>Source System Connections</b>", 0, Y5 - 340, "36", 800)
text(BOARD, "4 on-prem SQL Server instances via Fabric Gateway (TrustServerCertificate=True)", 0, Y5 - 290, "14", 800)

# Gateway
gw = shape(BOARD, "<b>Fabric Gateway</b><br>On-Prem Data Gateway<br>TrustServerCertificate=True", 0, Y5, 300, 150, "#6610f2", "#6610f2", "12")

# Source databases
sources = [
    ("SQL2019Dev", "IPC_PowerData", "Analytics/Reporting\nPrimary data warehouse", "#007bff", -800, Y5 - 100),
    ("M3Dev-DB1", "M3FDBTST", "M3 ERP System\nCustomers, Items, Orders", "#28a745", -800, Y5 + 100),
    ("SQL2016Dev", "M3TST-ETL", "ETL Staging\nTransformation data", "#fd7e14", -800, Y5 + 300),
    ("SQL2012Test", "MES", "MES/DiverData\nManufacturing, Production", "#dc3545", -800, Y5 + 500),
]

# Fabric connections
connections = [
    ("CON_FMD_FABRIC_SQL", "372237f9-...", "Metadata queries"),
    ("CON_FMD_FABRIC_PIPELINES", "6d8146c6-...", "Pipeline orchestration"),
    ("CON_FMD_FABRIC_NOTEBOOKS", "5929775e-...", "Notebook execution"),
    ("CON_FMD_ADF_PIPELINES", "02e107b8-...", "ADF integration"),
]

src_ids = []
for server, db, desc, color, x, y in sources:
    sid = shape(BOARD, f"<b>{server}</b><br>{db}<br><br>{desc}", x, y, 300, 120, "#f8f9fa", color, "11")
    src_ids.append(sid)
    connector(BOARD, sid, gw)

# FMD destination
fmd_dest = shape(BOARD, "<b>FMD Framework</b><br>Landing Zone<br>-&gt; Bronze -&gt; Silver -&gt; Gold", 800, Y5, 350, 150, "#FFD700", "#DAA520", "12")
connector(BOARD, gw, fmd_dest, "Fabric Connections")

# Connection details
for i, (name, cid, desc) in enumerate(connections):
    shape(BOARD, f"<b>{name}</b><br>{cid}<br>{desc}", 800, Y5 + 200 + i * 80, 350, 60, "#e9ecef", "#495057", "10")

# Jira ticket mapping
jira_map = shape(BOARD, "<b>Jira Tracking</b><br>MT-26: SQL2019Dev/IPC_PowerData<br>MT-27: M3Dev-DB1/M3FDBTST<br>MT-28: SQL2016Dev/M3TST-ETL<br>MT-29: SQL2012Test/MES", 0, Y5 + 350, 400, 150, "#fff3cd", "#856404", "10")


log("\n" + "=" * 60)
log(f"BOARD URL: {board.get('viewLink', 'Check Miro')}")
log("=" * 60)
log("\nAll 5 sections created successfully!")
log_fh.close()
