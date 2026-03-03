# Fabric Gateway Connections — Complete Inventory

**Gateway ID**: `66428eaa-90a0-4d3b-ab7a-71406c41a1cb`
**Gateway Type**: OnPremisesGateway (PowerBIGateway)
**Pulled via**: Fabric REST API (`/v1/connections`)
**Date**: 2026-02-19

## All Connections

| # | Display Name | Connection GUID | Server | Database | Auth | Encryption |
|---|---|---|---|---|---|---|
| 1 | SQLLogShipPRD_HP3000 | `918c04e1-be85-48f0-a836-5967164ff34d` | SQLLogShipPrd.interplastic.local | HP3000 | Basic | Any |
| 2 | SQLLogShipPRd_M3FDBPRD | `04c16d88-0a9b-44b0-8a97-a81898fa2fee` | sqllogshipprd | m3fdbprd | Basic | Any |
| 3 | M3-DB1_PRD_MES | `eace5b34-df2e-4057-a01f-5770ab3f9003` | m3-db1 | mes | OAuth2 | Encrypted |
| 4 | Data Warehouse | `9243f2a4-bf6c-4f63-935a-04c9491f0c1a` | rptlive.interplastic.com | datawarehouse | Basic | Any |
| 5 | Diver | `af673179-a72b-4c3f-8f9c-af2cb394494f` | SQL2012Test | DiverData | Basic | Any |
| 6 | SalesForcePRD-SQL2019PRD | `e4fa94cd-c674-45b3-bc47-2ae85f035881` | SQL2019Live | SalesforcePRD | Basic | Any |
| 7 | ETQ | `c5cd66d8-3503-44de-9934-e04715697906` | M3-DB3 | ETQStagingPRD | Basic | Any |
| 8 | M3 Cloud | `1187a5d7-5d6e-4a23-8c54-2a0734350629` | sql2016live | DI_PRD_Staging | Basic | NotEncrypted |

## FMD Registration Mapping

When registering these in SQL_INTEGRATION_FRAMEWORK, use this naming convention:

| Display Name | FMD Connection Name | FMD Data Source Name | Notes |
|---|---|---|---|
| M3 Cloud | CON_FMD_SQL2016LIVE_M3CLOUD | DI_PRD_Staging | **Registered** - M3 Cloud staging data |
| SQLLogShipPRD_HP3000 | CON_FMD_LOGSHIPPRD_HP3000 | HP3000 | HP3000 ERP database |
| SQLLogShipPRd_M3FDBPRD | CON_FMD_LOGSHIPPRD_M3FDBPRD | m3fdbprd | M3 FDB production |
| M3-DB1_PRD_MES | CON_FMD_M3DB1_MES | mes | MES production (OAuth2) |
| Data Warehouse | CON_FMD_RPTLIVE_DATAWAREHOUSE | datawarehouse | Existing data warehouse |
| Diver | CON_FMD_SQL2012TEST_DIVER | DiverData | Diver analytics data |
| SalesForcePRD-SQL2019PRD | CON_FMD_SQL2019LIVE_SALESFORCE | SalesforcePRD | Salesforce staging |
| ETQ | CON_FMD_M3DB3_ETQ | ETQStagingPRD | ETQ quality staging |

## Privacy & Connectivity

- All connections use **OnPremisesGateway** connectivity
- All connections have **Organizational** privacy level
- All use **Basic** auth except M3-DB1_PRD_MES which uses **OAuth2**
- All use **Any** encryption except:
  - M3-DB1_PRD_MES: **Encrypted**
  - M3 Cloud: **NotEncrypted**

## Currently Registered in SQL_INTEGRATION_FRAMEWORK

As of 2026-02-19:
- [x] M3 Cloud → CON_FMD_SQL2016LIVE_M3CLOUD (ConnectionId=4)
- [ ] SQLLogShipPRD_HP3000
- [ ] SQLLogShipPRd_M3FDBPRD
- [ ] M3-DB1_PRD_MES
- [ ] Data Warehouse
- [ ] Diver
- [ ] SalesForcePRD-SQL2019PRD
- [ ] ETQ
