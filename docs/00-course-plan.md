# Course plan

Eight modules, each with a lesson in `docs/` and a runnable lab in `labs/`.
Everything runs on Python >= 3.13 with `uv`. No Hadoop, no HDFS, no Hive Metastore:
storage is a local folder first, then MinIO (S3-compatible) in Docker; catalogs are
SQLite/Postgres-backed or REST.

| # | Module | You will be able to | Stack |
|---|--------|---------------------|-------|
| 1 | Modern data lake basics | Explain object storage + open file formats + table formats + catalogs + engines, and why plain Parquet is not a table | DuckDB, Parquet |
| 2 | Iceberg basics | Read an Iceberg table's metadata tree (metadata.json, manifest list, manifests, data files), take snapshots, time travel, evolve schema | PyIceberg (SQLite catalog), DuckDB `iceberg` extension |
| 3 | Catalog selection | Choose between REST catalogs (Polaris, Lakekeeper, Nessie, Gravitino, Unity), cloud catalogs (Glue, S3 Tables) and JDBC/SQL; know why Hive Metastore is legacy | Docker: MinIO + a REST catalog + Trino |
| 4 | Best-practice table creation | Pick partition transforms, sort order, target file size, format version (v2 vs v3) and table properties for a workload | Trino/Starburst, PyIceberg |
| 5 | Best-practice queries | Write queries that prune partitions and files, use metadata tables, avoid small-file and delete-file traps | Trino/Starburst, DuckDB |
| 6 | Maintenance | Run compaction, expire snapshots, remove orphan files, rewrite manifests; schedule them | Trino `ALTER TABLE ... EXECUTE`, PyIceberg |
| 7 | Development and CI | Test pipelines locally, use branches/tags and write-audit-publish, run Iceberg tests in GitHub Actions | pytest, GitHub Actions, Nessie or Iceberg branches |
| 8 | Format comparison | Place Iceberg against Delta Lake, Hudi, Paimon, DuckLake, and know what XTable does | Reading + small DuckLake lab |

## How each module works

1. Read `docs/NN-*.md` (concepts, mapped to Exadata and Elasticsearch where useful).
2. Run `uv run python labs/NN_*/lab.py` and read the output against the lesson.
3. Do the exercises at the end of the lesson.
4. `uv run pytest` keeps every lab runnable, which is also the seed of module 7.

## Setup (once)

```bash
# install uv: https://docs.astral.sh/uv/
uv python install 3.13
uv sync                      # creates .venv with duckdb, pyarrow, pyiceberg, pytest
uv run python labs/01_data_lake/lab.py
uv run pytest
```

Docker is needed from module 3 onward (MinIO, a REST catalog, Trino).
Starburst Galaxy's free tier is an option for module 4+ if you would rather not run Trino locally.
