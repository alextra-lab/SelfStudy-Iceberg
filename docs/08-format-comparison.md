# Module 8: Open table format comparison (reference, lesson to come)

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

## Outline of the lesson

| Question | What we will compare |
|---|---|
| Where does metadata live? | Files on object storage (Iceberg, Delta, Hudi, Paimon) vs a SQL database (DuckLake) |
| What is it built for? | Analytic batch + broad engine support (Iceberg), Spark/Databricks (Delta), upsert-heavy CDC (Hudi), streaming LSM tables with Flink (Paimon), simplicity (DuckLake) |
| How do updates work? | Copy-on-write, merge-on-read, deletion vectors, LSM merges |
| Who can read and write it? | Engine and catalog support matrix |
| Interop | Delta UniForm, XTable, Iceberg REST as the common catalog API |

Lab idea: build the module 1 `orders` table in DuckLake and in Iceberg, then compare
what lands on disk.
