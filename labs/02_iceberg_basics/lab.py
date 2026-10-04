import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import shutil
    import sqlite3
    from pathlib import Path

    import duckdb
    import marimo as mo
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform, MonthTransform
    from pyiceberg.types import DoubleType, IntegerType, LongType, NestedField, StringType, TimestampType

    return (
        DayTransform,
        DoubleType,
        IntegerType,
        LongType,
        MonthTransform,
        NestedField,
        PartitionField,
        PartitionSpec,
        Path,
        Schema,
        SqlCatalog,
        StringType,
        TimestampType,
        duckdb,
        mo,
        shutil,
        sqlite3,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Module 2 lab: Iceberg basics

    Read alongside `docs/02-iceberg-basics.md`.

    - **Interactive:** `uv run marimo edit labs/02_iceberg_basics/lab.py`
    - **Headless:** `uv run python labs/02_iceberg_basics/lab.py`

    We build the module 1 `orders` table again, this time as an Iceberg table, and
    replay module 1's failures to see how Iceberg fixes each one. Everything is
    written under `./lake/module02/` (git-ignored) and rebuilt on every run.

    Tools: **PyIceberg** writes and reads the table, and **DuckDB** runs SQL over
    what PyIceberg returns.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 0: the catalog is one SQLite file

    A catalog maps a table name to its current metadata file. For learning, PyIceberg's
    `SqlCatalog` keeps that map in a SQLite file. In production this is a REST catalog
    (module 3), but the job is identical.
    """)
    return


@app.cell
def _(Path, SqlCatalog, mo, shutil):
    WAREHOUSE = Path(mo.notebook_dir()).resolve().parents[1] / "lake" / "module02"
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    WAREHOUSE.mkdir(parents=True)

    catalog = SqlCatalog(
        "lab",
        uri=f"sqlite:///{WAREHOUSE}/catalog.db",  # where the name -> metadata pointer lives
        warehouse=f"file://{WAREHOUSE}",  # where data and metadata files go
    )
    catalog.create_namespace("sales")
    mo.md(f"Catalog created at `{WAREHOUSE}/catalog.db`, namespace `sales`.")
    return WAREHOUSE, catalog


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 1: create the table with hidden partitioning

    Same columns as module 1. The partition spec says **partition by `month(order_ts)`**.
    There is no `month` column: Iceberg derives the partition value from `order_ts`
    and records the rule in the table metadata. That is **hidden partitioning**.
    """)
    return


@app.cell
def _(
    DoubleType,
    IntegerType,
    LongType,
    MonthTransform,
    NestedField,
    PartitionField,
    PartitionSpec,
    Schema,
    StringType,
    TimestampType,
    WAREHOUSE,
    catalog,
    mo,
):
    orders_schema = Schema(
        NestedField(field_id=1, name="order_id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="order_ts", field_type=TimestampType(), required=False),
        NestedField(field_id=3, name="customer_id", field_type=IntegerType(), required=False),
        NestedField(field_id=4, name="region", field_type=StringType(), required=False),
        NestedField(field_id=5, name="amount", field_type=DoubleType(), required=False),
        NestedField(field_id=6, name="status", field_type=StringType(), required=False),
    )
    orders_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=MonthTransform(), name="order_ts_month")
    )
    orders = catalog.create_table("sales.orders", schema=orders_schema, partition_spec=orders_spec)

    _files = sorted(str(p.relative_to(WAREHOUSE)) for p in WAREHOUSE.rglob("*") if p.is_file())
    mo.md(
        "Files on disk after `CREATE TABLE` (no data yet, just one metadata file):\n\n"
        + "\n".join(f"- `{_f}`" for _f in _files)
    )
    return (orders,)


@app.cell
def _(mo):
    mo.md(r"""
    The catalog's whole job, as SQL. One row: table name and the metadata file it points to.
    """)
    return


@app.cell
def _(WAREHOUSE, mo, orders, sqlite3):
    _ = orders
    _db = sqlite3.connect(WAREHOUSE / "catalog.db")
    _rows = _db.execute("""
        SELECT table_namespace, table_name, metadata_location
        FROM iceberg_tables
    """).fetchall()
    _db.close()
    mo.md(
        "| table_namespace | table_name | metadata_location |\n|---|---|---|\n"
        + "\n".join(f"| {_r[0]} | {_r[1]} | `{_r[2].rsplit('/', 1)[1]}` |" for _r in _rows)
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 2: load the same 2M orders as module 1, in one commit

    The generator query is identical to module 1. DuckDB builds the rows, PyIceberg
    appends them. One `append` = one commit = one new snapshot.
    """)
    return


@app.cell
def _(WAREHOUSE, duckdb, mo, orders):
    con = duckdb.connect()
    orders_arrow = con.sql("""
        SELECT
            i                                                               AS order_id,
            TIMESTAMP '2025-01-01' + to_seconds((i * 15) % 31_536_000)      AS order_ts,
            (hash(i) % 50_000)::INT                                         AS customer_id,
            ['EMEA','NA','LATAM','APAC','ANZ'][(hash(i * 7) % 5)::INT + 1]  AS region,
            round(((hash(i * 13) % 100_000) / 100.0), 2)                    AS amount,
            ['NEW','PAID','SHIPPED','CANCELLED'][(hash(i * 3) % 4)::INT + 1] AS status
        FROM range(2_000_000) t(i)
    """).to_arrow_table()

    orders.append(orders_arrow)
    first_snapshot_id = orders.current_snapshot().snapshot_id

    _files = sorted(str(p.relative_to(WAREHOUSE / "sales" / "orders")) for p in (WAREHOUSE / "sales" / "orders").rglob("*") if p.is_file())
    _meta = [_f for _f in _files if _f.startswith("metadata/")]
    _data = [_f for _f in _files if _f.startswith("data/")]
    mo.md(
        f"Snapshot `{first_snapshot_id}` committed.\n\n"
        f"**metadata/** ({len(_meta)} files):\n\n" + "\n".join(f"- `{_f}`" for _f in _meta)
        + f"\n\n**data/** ({len(_data)} files), e.g.:\n\n" + "\n".join(f"- `{_f}`" for _f in _data[:3])
        + "\n\nIn `metadata/`: `00000-*.metadata.json` is from `CREATE TABLE`, `00001-*.metadata.json` "
        "is from this commit, `snap-*.avro` is the **manifest list**, and `*-m0.avro` is the **manifest**."
    )
    return con, first_snapshot_id


@app.cell
def _(WAREHOUSE, mo, orders, sqlite3):
    _ = orders.current_snapshot()
    _db = sqlite3.connect(WAREHOUSE / "catalog.db")
    _rows = _db.execute("""
        SELECT table_name, metadata_location, previous_metadata_location
        FROM iceberg_tables
    """).fetchall()
    _db.close()
    mo.md(
        "The commit swapped the catalog pointer from `00000` to `00001`:\n\n"
        "| table_name | metadata_location | previous_metadata_location |\n|---|---|---|\n"
        + "\n".join(
            f"| {_r[0]} | `{_r[1].rsplit('/', 1)[1][:5]}...metadata.json` | `{_r[2].rsplit('/', 1)[1][:5]}...metadata.json` |"
            for _r in _rows
        )
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 3: walk the metadata tree, top down

    ```
    catalog  ──►  metadata.json  ──►  manifest list  ──►  manifests  ──►  data files
                  (table: schema,     (one per          (one row per     (Parquet)
                   specs, snapshots)   snapshot)         data file, with
                                                         min/max stats)
    ```

    **3a. The metadata file** is plain JSON. DuckDB reads it with `read_json`.
    """)
    return


@app.cell
def _(con, first_snapshot_id, mo, orders):
    _ = first_snapshot_id
    metadata_file = orders.metadata_location.removeprefix("file://")
    _df = mo.sql(
        f"""
        SELECT "format-version",
               "current-snapshot-id",
               len(snapshots)          AS snapshots,
               "partition-specs"[1]    AS partition_spec,
               schemas[1].fields       AS columns
        FROM read_json('{metadata_file}')
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    **3b. The manifest list** for the current snapshot: which manifests make up the
    table, and a min/max summary of the partition values in each. PyIceberg exposes
    it as a metadata table, the same idea as Trino's `"orders$manifests"`.
    """)
    return


@app.cell
def _(con, first_snapshot_id, mo, orders):
    _ = first_snapshot_id
    con.register("manifests", orders.inspect.manifests())
    _df = mo.sql(
        """
        SELECT regexp_extract(path, '[^/]+$')  AS manifest_file,
               added_data_files_count,
               partition_summaries
        FROM manifests
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    **3c. The manifest entries**: one row per data file, with its partition value,
    row count and per-column min/max. This is where module 1's Parquet footer stats
    moved to: the engine reads them **before** opening any data file.
    """)
    return


@app.cell
def _(con, first_snapshot_id, mo, orders):
    _ = first_snapshot_id
    con.register("data_files", orders.inspect.files())
    _df = mo.sql(
        """
        SELECT regexp_extract(file_path, 'data/(.*)$', 1)      AS file,
               partition.order_ts_month                       AS month_partition,
               record_count,
               readable_metrics.order_id.lower_bound          AS order_id_min,
               readable_metrics.order_id.upper_bound          AS order_id_max
        FROM data_files
        ORDER BY order_id_min
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    `month_partition` is months since 1970-01 (660 = 2025-01). Iceberg stores the
    transform's result, not a string.

    ## Step 4: hidden partitioning prunes on the real column

    Module 1 exercise 1 filtered the Hive-style table on `order_ts` and read
    **12 of 12** files, because `month` lived only in folder names. Same filter here:
    """)
    return


@app.cell
def _(first_snapshot_id, mo, orders):
    _ = first_snapshot_id
    march_scan = orders.scan(
        row_filter="order_ts >= '2025-03-01T00:00:00' AND order_ts < '2025-04-01T00:00:00'"
    )
    planned_files = len(list(march_scan.plan_files()))
    total_files = len(list(orders.scan().plan_files()))
    mo.md(f"Files planned: **{planned_files} of {total_files}**")
    return march_scan, planned_files


@app.cell
def _(mo):
    mo.md(r"""
    Iceberg knows the rule `month(order_ts)`, so it turns the `order_ts` range into
    partition values and prunes, like Oracle partition pruning on a range-partitioned
    table. Here is the query's result, run by DuckDB over the planned files only:
    """)
    return


@app.cell
def _(con, march_scan, mo):
    con.register("march_orders", march_scan.to_arrow())
    _df = mo.sql(
        """
        SELECT region, count(*) AS orders, round(sum(amount), 2) AS revenue
        FROM march_orders
        GROUP BY region
        ORDER BY region
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 5: read consistency, replaying module 1 step 5b

    In module 1, a reader that listed files while a writer rewrote March counted
    **2,178,560** rows in a 2,000,000-row table. Here a reader pins the current
    snapshot (Oracle: notes its SCN), then a writer rewrites March to cancel order
    400000 (it is in March). `overwrite` deletes the March files and adds new ones in
    **one commit**.
    """)
    return


@app.cell
def _(WAREHOUSE, con, first_snapshot_id, mo, orders):
    reader_snapshot_id = orders.current_snapshot().snapshot_id  # the reader pins its "SCN"
    assert reader_snapshot_id == first_snapshot_id

    _march_filter = "order_ts >= '2025-03-01T00:00:00' AND order_ts < '2025-04-01T00:00:00'"
    con.register("march_before", orders.scan(row_filter=_march_filter).to_arrow())
    _march_after = con.sql("""
        SELECT * REPLACE (CASE WHEN order_id = 400000 THEN 'CANCELLED' ELSE status END AS status)
        FROM march_before
    """).to_arrow_table()
    orders.overwrite(_march_after, overwrite_filter=_march_filter)  # the writer commits

    rows_at_reader_snapshot = orders.scan(snapshot_id=reader_snapshot_id).to_arrow().num_rows
    rows_now = orders.scan().to_arrow().num_rows
    _json_files = sorted(p.name[:5] for p in (WAREHOUSE / "sales" / "orders" / "metadata").glob("*.metadata.json"))
    mo.md(f"""
    | Reader | Rows |
    |---|---|
    | Pinned snapshot `{reader_snapshot_id}` (started before the write) | **{rows_at_reader_snapshot:,}** |
    | Current snapshot (after the write) | **{rows_now:,}** |

    Metadata files now: {", ".join(f"`{_j}`" for _j in _json_files)}. The write added exactly
    one (`00002`), so the table went from the old state to the new one in a single pointer swap.
    There was no moment in between for a reader to see.
    """)
    return reader_snapshot_id, rows_at_reader_snapshot, rows_now


@app.cell
def _(mo):
    mo.md(r"""
    The snapshot history. PyIceberg records the overwrite as two snapshots, a
    `delete` then an `append`, but both were written in that one metadata file, so
    the `delete` snapshot was never the current state for any reader.
    """)
    return


@app.cell
def _(con, mo, orders, rows_now):
    _ = rows_now
    con.register("snapshots", orders.inspect.snapshots())
    _df = mo.sql(
        """
        SELECT committed_at, snapshot_id, parent_id, operation,
               map_extract(summary, 'total-records')[1] AS total_records
        FROM snapshots
        ORDER BY committed_at, snapshot_id
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 6: time travel (Oracle flashback query)

    `scan(snapshot_id=...)` is `SELECT ... AS OF SCN`. Order 400000 before and after
    the write:
    """)
    return


@app.cell
def _(mo, orders, reader_snapshot_id, rows_now):
    _ = rows_now
    status_before = orders.scan(row_filter="order_id = 400000", snapshot_id=reader_snapshot_id).to_arrow()["status"][0].as_py()
    status_now = orders.scan(row_filter="order_id = 400000").to_arrow()["status"][0].as_py()
    mo.md(f"""
    | Query | status of order 400000 |
    |---|---|
    | `orders.scan(row_filter="order_id = 400000", snapshot_id={reader_snapshot_id})` | {status_before} |
    | `orders.scan(row_filter="order_id = 400000")` | {status_now} |
    """)
    return status_before, status_now


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 7: schema evolution, replaying module 1 step 5c

    In module 1, adding a `channel` column to new files broke readers. In Iceberg the
    schema lives in the metadata, and every column has an ID. Adding a column is a
    metadata-only commit: no data file is rewritten, and old rows read it as NULL.
    """)
    return


@app.cell
def _(StringType, WAREHOUSE, mo, orders, status_now):
    _ = status_now
    _data_dir = WAREHOUSE / "sales" / "orders" / "data"
    data_files_before = sorted(p.name for p in _data_dir.rglob("*.parquet"))

    with orders.update_schema() as _update:
        _update.add_column("channel", StringType())

    data_files_after = sorted(p.name for p in _data_dir.rglob("*.parquet"))
    mo.md(f"""
    Data files on disk before `add_column`: {len(data_files_before)}, after: {len(data_files_after)},
    identical: **{data_files_before == data_files_after}**.

    (13, not 12: the old March file from step 5 is still on disk because the first
    snapshot still references it. That is what makes time travel work, like undo.
    Module 6 covers expiring snapshots to clean these up.)
    """)
    return


@app.cell
def _(con, data_files_after, mo, orders):
    _ = data_files_after
    con.register("orders_now", orders.scan().to_arrow())
    _df = mo.sql(
        """
        SELECT channel, count(*) AS orders
        FROM orders_now
        GROUP BY channel
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ---
    # Exercises

    **Exercise 1. File skipping with manifest stats.** This is module 1 step 4b at file
    level. Step 4 used a partition filter. Here the filter is on columns that are not
    partitioned, and Iceberg prunes using the min/max in the manifests (step 3c).
    Run it, then change only the `row_filter` string to try your own predicates.
    """)
    return


@app.cell
def _(data_files_after, mo, orders):
    _ = data_files_after  # run after step 7
    _total = len(list(orders.scan().plan_files()))
    _results = {}
    for _row_filter in ["order_id < 100000", "amount < 1"]:  # edit me
        _results[_row_filter] = len(list(orders.scan(row_filter=_row_filter).plan_files()))
    mo.md(
        "| row_filter | Files planned |\n|---|---|\n"
        + "\n".join(f"| `{_k}` | {_v} of {_total} |" for _k, _v in _results.items())
    )
    exercise1_files = _results
    return (exercise1_files,)


@app.cell
def _(mo):
    mo.md(r"""
    Why can `order_id < 100000` skip files when `order_id` is not a partition column?
    Look at the `order_id_min` and `order_id_max` columns in step 3c.

    **Exercise 2. Partition evolution.** Module 1 said changing a Hive layout from
    month to day means rewriting the whole table. Here the cell switches the spec to
    `day(order_ts)` and appends one new day of orders. Which folders hold old and new
    files? Does the step 4 filter still prune?
    """)
    return


@app.cell
def _(DayTransform, WAREHOUSE, con, exercise1_files, mo, orders):
    _ = exercise1_files  # run after exercise 1
    with orders.update_spec() as _spec:
        _spec.add_field("order_ts", DayTransform(), "order_ts_day")
        _spec.remove_field("order_ts_month")

    _new_day = con.sql("""
        SELECT
            2_000_000 + i                                                   AS order_id,
            TIMESTAMP '2026-01-01' + to_seconds(i * 15)                     AS order_ts,
            (hash(i) % 50_000)::INT                                         AS customer_id,
            ['EMEA','NA','LATAM','APAC','ANZ'][(hash(i * 7) % 5)::INT + 1]  AS region,
            round(((hash(i * 13) % 100_000) / 100.0), 2)                    AS amount,
            'NEW'                                                           AS status,
            'web'                                                           AS channel
        FROM range(5_000) t(i)
    """).to_arrow_table()
    orders.append(_new_day)

    _dirs = sorted({p.parent.name for p in (WAREHOUSE / "sales" / "orders" / "data").rglob("*.parquet")})
    _march = len(list(orders.scan(row_filter="order_ts >= '2025-03-01T00:00:00' AND order_ts < '2025-04-01T00:00:00'").plan_files()))
    _jan_1 = len(list(orders.scan(row_filter="order_ts >= '2026-01-01T00:00:00' AND order_ts < '2026-01-02T00:00:00'").plan_files()))
    _total = len(list(orders.scan().plan_files()))
    spec_evolved = {"march": _march, "jan_1": _jan_1, "total": _total}
    mo.md(
        "Partition folders now:\n\n" + ", ".join(f"`{_d}`" for _d in _dirs)
        + f"\n\n| Filter | Files planned |\n|---|---|\n"
        f"| March 2025 (old month spec) | {_march} of {_total} |\n"
        f"| 2026-01-01 (new day spec) | {_jan_1} of {_total} |"
    )
    return (spec_evolved,)


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 3. Time travel.** Pick a snapshot from the step 5 history table and
    count the rows the table had at that point. What does the `delete` snapshot show,
    and why did no reader ever see it?
    """)
    return


@app.cell
def _(mo, orders, spec_evolved):
    _ = spec_evolved  # list snapshots after exercise 2
    snapshot_choice = mo.ui.dropdown(
        options={str(_s.snapshot_id): _s.snapshot_id for _s in orders.snapshots()},
        value=str(orders.snapshots()[0].snapshot_id),
        label="snapshot_id",
    )
    snapshot_choice
    return (snapshot_choice,)


@app.cell
def _(mo, orders, snapshot_choice):
    _snap = orders.snapshot_by_id(snapshot_choice.value)
    _rows = orders.scan(snapshot_id=snapshot_choice.value).to_arrow().num_rows
    mo.md(f"""
    | snapshot_id | operation | Rows at this snapshot |
    |---|---|---|
    | {snapshot_choice.value} | {_snap.summary.operation.value} | {_rows:,} |
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 4. Two writers at once.** Two writers both read metadata file `00002`
    and each tries to commit a change. Using the commit protocol in lesson section 4,
    what happens to each one? Compare with two Oracle sessions updating the same row.
    (No code. Answer in the project thread.)

    ---
    ## Optional: read the table from DuckDB's `iceberg` extension

    DuckDB can read an Iceberg table directly from its metadata file. The extension
    downloads on first use, so this needs internet access.
    """)
    return


@app.cell
def _(mo, orders, spec_evolved):
    _ = spec_evolved
    import duckdb as _duckdb

    _con = _duckdb.connect()
    _metadata_file = orders.metadata_location.removeprefix("file://")
    try:
        _con.execute("INSTALL iceberg; LOAD iceberg;")
        _result = _con.sql(f"""
            SELECT count(*) AS orders
            FROM iceberg_scan('{_metadata_file}')
        """).fetchall()
        _out = mo.md(f"`iceberg_scan` counted **{_result[0][0]:,}** rows.")
    except Exception as _e:
        _out = mo.md(f"Could not use the DuckDB iceberg extension here: `{str(_e).splitlines()[0][:150]}`")
    _out
    return


if __name__ == "__main__":
    app.run()
