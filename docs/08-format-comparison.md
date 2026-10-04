# Module 8: Table format and engine comparison (reference, lesson to come)

The full lesson comes after modules 2 to 7, once you know Iceberg well enough to
compare. Kept here now: the release facts gathered on 2026-10-03.

## Release snapshot (as of 2026-10-03)

| Project | Latest stable | Released | Licence | Governance |
|---|---|---|---|---|
| Apache Iceberg | 1.11.0 | 2026-05-19 | Apache 2.0 | ASF top-level |
| Delta Lake | 4.4.0 | 2026-08-20 | Apache 2.0 | Linux Foundation (Databricks-led) |
| Apache Hudi | 1.2.0 (table format v9) | 2026-05-23 | Apache 2.0 | ASF top-level |
| Apache Paimon | 2.0.0 | 2026-08-07 | Apache 2.0 | ASF top-level |
| DuckLake | 1.0 (spec) | 2026-04-13 | MIT | DuckDB Labs / DuckDB Foundation |
| Apache XTable | 0.4.0-incubating | 2026-08-24 | Apache 2.0 | ASF incubator |

Notes that matter for choosing:

- **Delta 4.4.0** adds Spark 4.2 support and Unity Catalog Delta Table API integration.
  The 3.3.x line still gets patches (3.3.3, 2026-08-12).
- **Hudi 1.1.1** is the last patch of 1.1.x.
- **Paimon 2.0.0** is a major release (with PyPaimon 2.0.0), positioned for
  streaming + batch + AI. Its release notes advise testing your engine/catalog pair
  before upgrading.
- **DuckLake 1.0** is the first spec with backward-compatibility guarantees. The
  reference implementation is the DuckDB `ducklake` extension.
- **XTable** is not a table format. It translates metadata so one copy of the data
  can be read as Iceberg, Delta or Hudi (0.4.0 went from 3 to 5 formats and added a
  REST service). Still incubating.

## Engines that read Iceberg

Table formats are only half the comparison. The other half is which **engine**
reads and writes them. chDB is an engine, not a table format: it is ClickHouse
packaged as an in-process Python library, the same deployment model as DuckDB.
It gets its own module (9).

| | DuckDB | chDB | Trino / Starburst | Spark |
|---|---|---|---|---|
| What it is | In-process analytic database | In-process ClickHouse | Distributed MPP SQL cluster | Distributed processing engine |
| Runs as | Python/CLI library | Python library | Server cluster | Cluster or local |
| Licence | MIT | Apache 2.0 | Apache 2.0 (Trino); commercial (Starburst) | Apache 2.0 |
| Iceberg support ships as | `iceberg` extension, downloaded on first use | Built in (`icebergLocal`, `icebergS3` table functions) | Built-in Iceberg connector | Iceberg runtime JAR |
| Read Iceberg | Yes | Yes (verified) | Yes | Yes |
| Time travel | Yes | Yes, `SETTINGS iceberg_snapshot_id = ...` (verified) | `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` | `VERSION AS OF` / `TIMESTAMP AS OF` |
| Write Iceberg | Yes, in recent versions | Experimental, off by default (`allow_experimental_insert_into_iceberg`) | Yes, including `MERGE`, `UPDATE`, `DELETE` | Yes, the reference implementation |
| Maintenance (compaction, expire snapshots) | No | No | Yes (`ALTER TABLE ... EXECUTE`) | Yes (stored procedures) |
| Native storage of its own | DuckDB database file | MergeTree tables | None (connectors only) | None |
| Best fit | Local dev, tests, single-node analytics | Local analytics with ClickHouse SQL and functions; prototyping for a ClickHouse server | Shared, concurrent SQL over the lake | Heavy ETL, streaming, maintenance jobs |

"Verified" means checked in this repo on 2026-10-04 with chDB 4.4.0 (bundling
ClickHouse 26.9.2) against the module 2 table: `icebergLocal` returned the right row
count and the right value for order 400000 at both the current and the first snapshot.
The same check exposed the module 1 lesson again: reading the table's Parquet files
directly with ClickHouse's `file()` function counted 2,183,560 rows instead of
2,005,000, because it also picked up files that only older snapshots reference.

## Outline of the lesson

| Question | What we will compare |
|---|---|
| Where does metadata live? | Files on object storage (Iceberg, Delta, Hudi, Paimon) vs a SQL database (DuckLake) |
| What is it built for? | Analytic batch + broad engine support (Iceberg), Spark/Databricks (Delta), upsert-heavy CDC (Hudi), streaming LSM tables with Flink (Paimon), simplicity (DuckLake) |
| How do updates work? | Copy-on-write, merge-on-read, deletion vectors, LSM merges |
| Who can read and write it? | Engine and catalog support matrix (see below) |
| Interop | Delta UniForm, XTable, Iceberg REST as the common catalog API |

Lab idea: build the module 1 `orders` table in DuckLake and in Iceberg, then compare
what lands on disk.
