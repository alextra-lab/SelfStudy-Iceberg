# SelfStudy-Iceberg

Hands-on self-study of **Apache Iceberg**, queried through **Starburst (Trino)** and **DuckDB**.

The goal is to learn Iceberg by doing it: create tables, evolve them, time travel through snapshots, and read the same data from more than one engine.

## Topics

- Iceberg fundamentals: metadata files, manifests, snapshots, catalogs
- Creating and loading tables with Starburst / Trino
- Reading Iceberg tables from DuckDB (`iceberg` extension)
- Schema and partition evolution
- Time travel and snapshot management
- Table maintenance: compaction, expiring snapshots, orphan file cleanup

## Layout

Folders will be added as the work progresses, for example:

```
docs/        notes and write-ups
labs/        step-by-step exercises
sql/         Trino and DuckDB queries
data/        small sample datasets (large files stay out of git)
```

## Status

Just getting started.
