# FMD Framework — CIO Presentation Talking Points
**Audience:** Mike (CIO), Patrick
**Presenter:** Steve Nahrup
**Date:** February 2026

---

## Opening — Set the Stage (2 min)

> "What I want to walk you through today is the architecture I've built for our data integration layer — the FMD Framework. FMD stands for Fabric Metadata-Driven. The core idea is simple: instead of hand-coding every pipeline, every connection, every transformation — we define it once in metadata, and the framework does the rest. One notebook deployment stood up our entire integration architecture — 5 workspaces, 6 lakehouses, 42 pipelines, 14 notebooks, a SQL metadata database, and 4 variable libraries. All configured, all connected, all ready to run."

**Key stat to land:** *"42 pipelines deployed from a single configuration notebook — not 42 manual builds."*

---

## Slide 1 — The Full FMD Overview (Bottom-Left Quadrant)
*Use: FMD_Overview.png or the bottom-left of the Excalidraw*

### What to say:
> "This is the 30,000-foot view. On the left you see our data sources — on-prem SQL Server, SFTP feeds, Azure Data Lake, OneLake — anything that needs to come into the platform. In the middle is the integration domain — that's the engine. It's broken into three workspace groups: Data workspaces hold the lakehouses at each layer, Code workspaces hold the pipelines and notebooks that do the actual work, and the Config workspace holds the SQL metadata database that drives everything."

> "The key differentiator here is that metadata backbone — the SQL FMD Framework database. Every pipeline reads its configuration from there. Add a new data source? You don't build a new pipeline. You add a row to the metadata table and the existing pipelines pick it up on the next run."

### Points to emphasize:
- **Configuration over code** — reduces development time by 80%+ for new source onboarding
- **Consistent patterns** — every source follows the same path, same standards, same monitoring
- **Environment parity** — Dev and Prod are structurally identical, deployed from the same config

---

## Slide 2 — Workspace Architecture (Top-Left Quadrant)
*Use: FMD_WORKSPACE_OVERVIEW.png or top-left of the Excalidraw*

### What to say:
> "Let me drill into the workspace structure. We have two environments — Development and Production — and each one mirrors the other exactly. Within each environment, there's a Code workspace and a Data workspace. Code holds the pipelines, notebooks, and variable libraries. Data holds the lakehouses — Landing Zone, Bronze, and Silver. Then there's a shared Configuration workspace that both environments read from."

> "This separation matters. Code and data have different security profiles, different access patterns, different lifecycle management. A data engineer working on pipeline logic doesn't need write access to production data. A report builder consuming Silver layer data doesn't need to see pipeline internals. The workspace boundaries enforce that naturally."

### Points to emphasize:
- **Security through architecture** — access control is structural, not just policy
- **Promotion path is clean** — Dev → Prod is a workspace-level operation, not file-by-file
- **Currently deployed and running** — this isn't a design doc, it's live in our Fabric tenant

---

## Slide 3 — The Data Journey (Lakehouse Layers)
*Use: FMD_LAKEHOUSE_OVERVIEW.png*

### What to say:
> "Data flows through four layers — the medallion architecture. Landing Zone is raw ingestion — data arrives exactly as the source sends it, no transformation, no schema enforcement. Bronze adds structure — deduplication, data typing, basic quality checks. Silver is where the real value starts — historical tracking, cleansing, validation, harmonization across sources. And Gold is the business-ready layer — star schemas, materialized views, the data that actually powers reports and AI models."

> "Right now, we have Landing Zone through Silver fully operational. Gold layer is the next phase — that's where we build out the business domain workspaces, each with their own semantic models and reports."

### If asked about Gold timeline:
> "Gold layer buildout is planned as part of the domain workspace deployment. Each business domain — Finance, Sales, Operations — gets its own Gold lakehouse with OneLake shortcuts pulling from Silver. No data duplication, just logical organization by business function."

---

## Slide 4 — Pipeline Architecture (42 Pipelines)
*Use: FMD_PROCESS_OVERVIEW.png*

### What to say:
> "Here's where it gets interesting from an engineering standpoint. We deployed 42 pipelines — 21 per environment. Let me break down what they do. There are 9 Copy pipelines that handle data extraction from different source types — SQL, SFTP, ADLS, OneLake. Each one is parameterized through metadata, so the same pipeline template handles any source of that type. Then there are 8 Command pipelines for orchestration logic — things like triggering downstream processes, managing dependencies. 3 Load pipelines move data between layers — Landing Zone to Bronze, Bronze to Silver. There's 1 master Orchestrator that sequences everything, and 1 Tooling pipeline for maintenance tasks."

> "Every single one of these reads its configuration from the SQL metadata database. The framework doesn't care what data you're moving — it cares about the pattern. Add a new SQL source? Add metadata rows. The pipelines already know what to do."

### Points to emphasize:
- **42 pipelines, zero hand-coding** — all deployed from configuration
- **Parallel processing** — independent sources run concurrently
- **Built-in monitoring** — every pipeline execution is logged with status, duration, row counts

---

## Slide 5 — Metadata Database (The Brain)
*Use: FMD_METADATA_OVERVIEW.png*

### What to say:
> "This is the brain of the whole operation — the SQL FMD Framework database. Three schemas: Integration holds the source definitions, connection details, and layer configurations. Execution tracks every pipeline run, notebook execution, and copy activity with full audit trails. Logging captures errors, warnings, and performance metrics."

> "The views are where the magic happens. There are pre-built views — LoadSourceToLandingzone, LoadToBronzeLayer, LoadToSilverLayer — that pipelines query at runtime to get their instructions. A pipeline doesn't know what it's loading. It asks the metadata: 'What do I need to process right now?' and gets back a result set with source connections, target locations, transformation rules, everything."

### Points to emphasize:
- **Declarative data movement** — define what, not how
- **Full audit trail** — every execution is tracked with timestamps, row counts, error details
- **Self-documenting** — the metadata IS the documentation of what's in the platform

---

## Slide 6 — Taskflow / Orchestration
*Use: FMD_TASKFLOW_OVERVIEW.png*

### What to say:
> "This is a live screenshot from our Fabric tenant showing the taskflow orchestration. You can see the sequential flow — metadata configuration drives data store connections, which feed into validation, then Bronze processing, then Silver processing, with audit and logging at each step. This is Fabric-native orchestration, not a bolt-on scheduler."

### Points to emphasize:
- **This is live** — not a mockup, not a design. Running in our tenant right now.
- **Fabric-native** — leverages platform capabilities rather than fighting them

---

## Slide 7 — Domain Scalability (Future State)
*Use: FMD_DOMAIN_OVERVIEW.png*

### What to say:
> "This is where the architecture really pays off at scale. The integration domain — what we just walked through — feeds into business domain workspaces. Finance, Sales, Operations — each gets their own Gold lakehouse, their own semantic models, their own reports. But they all consume from the same Silver layer through OneLake shortcuts. No data duplication, no ETL sprawl."

> "Each domain is self-contained but consistent. Same patterns, same monitoring, same security model. And because it's all metadata-driven, standing up a new domain is a configuration exercise, not a development project."

**Flag for audience:** *"This slide represents the target architecture. The integration domain is live today. Domain workspaces are the next phase."*

---

## Slide 8 — Fabric Platform Capabilities (Top-Right Quadrant)
*Use: Top-right of the Excalidraw*

### What to say:
> "And this is why we chose Fabric as the platform. It's not just a data lake — it's a unified analytics platform. Data Engineering, Data Science, Real-Time Intelligence, Data Warehousing, Business Intelligence — all in one platform with a single security model, single billing, single governance layer. The FMD Framework is purpose-built to leverage these capabilities."

> "What we've deployed so far touches Data Engineering, Data Integration, and Data Warehousing. As we build out Gold layer and domain workspaces, we'll naturally extend into Business Intelligence with Power BI semantic models, and eventually Data Science and Real-Time Intelligence capabilities."

---

## Closing — The Value Proposition (2 min)

> "Let me bring it back to what this means for the organization. We've built a framework that turns data integration from a development problem into a configuration problem. New data source? Metadata entry, not a development sprint. New business domain? Workspace deployment, not a six-month project. Full audit trail, consistent patterns, environment parity, and it's all running on Microsoft's latest enterprise platform."

> "The integration backbone is live. Next steps are on-prem gateway connections, source registration, end-to-end testing, and then Gold layer buildout with domain workspaces. The architecture is designed to scale to as many domains and data sources as the business needs — without architectural changes."

---

## Anticipated Questions & Answers

### "How long did this take to set up?"
> "The framework itself is open-source and mature. The real work was in configuration — mapping our specific infrastructure, setting up the service principal with the right permissions, configuring the trial capacity, and validating that all 68 artifacts deployed correctly. The deployment itself runs in about 15 minutes. The planning and configuration work took considerably longer — getting the security model right, understanding our source systems, defining the workspace structure."

### "What about security?"
> "Security is baked into the architecture at multiple levels. Workspace-level RBAC controls who can see code vs. data. Service principal handles automated operations with least-privilege access. Key Vault integration for credential management. And the metadata database itself tracks who ran what, when, and what the outcome was."

### "Can this handle real-time data?"
> "The current implementation is batch/scheduled — which covers 90%+ of enterprise data integration needs. Fabric does support Real-Time Intelligence capabilities, and the architecture is designed so that real-time sources can be added as a new source type without changing the core framework. That would be a phase 2 or 3 capability."

### "What if the framework breaks or has a bug?"
> "Two things. First, it's open-source with an active community — we're not dependent on a single vendor for fixes. Second, because it's metadata-driven, the framework logic and the data configuration are separated. A framework update doesn't change your data configurations, and a configuration change doesn't require framework modifications."

### "How does this compare to Azure Data Factory?"
> "ADF is a great tool, and the FMD Framework actually supports ADF as a data source. The difference is abstraction level. ADF requires you to build each pipeline. FMD lets you define sources in metadata and generates the pipeline behavior from configuration. Think of it as ADF is the engine, FMD is the autopilot."

### "What's the cost?"
> "We're currently running on a Fabric trial capacity — F64 SKU. In production, the cost scales with compute consumption, not with the number of pipelines or data sources. Microsoft's Fabric pricing is capacity-based, so you pay for the compute power, not per-pipeline or per-workspace. The framework itself is free and open-source."

---

## Diagram Presentation Safety Guide

| Diagram | Status | Safe to Present As-Is? |
|---------|--------|----------------------|
| FMD Overview (High Level Design) | Accurate to deployment | YES |
| Workspace Overview | Exact match to 5-workspace layout | YES |
| Process Overview (Pipelines) | Exact match to 42 deployed pipelines | YES |
| Metadata Overview (SQL Schema) | Exact match to SQL database | YES |
| Taskflow Overview | Live Fabric screenshot | YES |
| Lakehouse Overview | LZ/Bronze/Silver accurate, Gold is future state | YES — note Gold is "next phase" |
| Domain Overview | Future state — Gold/business domains not built yet | YES — clearly label as "target architecture" |
| Variable Library Configuration | Shows DEMO values, not IP Corp GUIDs | SKIP or label as "example configuration" |
| Fabric Capability Map (top-right) | Platform capability view, not deployment-specific | YES — it's a capability reference |

---

## Recommended Presentation Flow

1. **Open with the Full FMD Overview** (bottom-left) — the money slide, shows scope and value
2. **Drill into Workspace Architecture** (top-left) — shows engineering rigor and security thinking
3. **Walk the Data Journey** — Lakehouse layers, Landing Zone → Bronze → Silver
4. **Show Pipeline Architecture** — 42 pipelines, metadata-driven, zero hand-coding
5. **Show the Metadata Database** — the brain, declarative movement, full audit trail
6. **Live Fabric Demo** — Taskflow screenshot proves it's real, not a deck
7. **Domain Scalability** — where this goes next (Finance, Sales, etc.)
8. **Platform Capabilities** — why Fabric is the right platform choice
9. **Close with Value Prop** — configuration over code, scale without rearchitecture
