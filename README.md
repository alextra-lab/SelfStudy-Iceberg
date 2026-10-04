# SelfStudy-Iceberg

Hands-on self-study of **Apache Iceberg**, queried through **Starburst (Trino)** and **DuckDB**.
No Hadoop: object storage (local folder, then MinIO), SQL or REST catalogs, Python >= 3.13.

## Course

See [docs/00-course-plan.md](docs/00-course-plan.md) for the full plan.

| # | Module | Lesson | Lab |
|---|--------|--------|-----|
| 1 | Modern data lake basics | [docs/01-modern-data-lake.md](docs/01-modern-data-lake.md) | `labs/01_data_lake/lab.py` |
| 2 | Iceberg basics | [docs/02-iceberg-basics.md](docs/02-iceberg-basics.md) | `labs/02_iceberg_basics/lab.py` |
| 3 | Catalog selection | coming | |
| 4 | Best-practice table creation | coming | |
| 5 | Best-practice queries | coming | |
| 6 | Maintenance | coming | |
| 7 | Development and CI | coming | |
| 8 | Format comparison (Delta, Hudi, Paimon, DuckLake, XTable) | [reference](docs/08-format-comparison.md) | |

## Quick start

```bash
uv python install 3.13
uv sync
uv run marimo edit labs/01_data_lake/lab.py   # labs are marimo notebooks
uv run pytest
```

## Layout

```
docs/     lessons
labs/     marimo notebooks (plain .py), one folder per module
tests/    smoke tests that keep every lab runnable
lake/     local data written by the labs (git-ignored)
```
