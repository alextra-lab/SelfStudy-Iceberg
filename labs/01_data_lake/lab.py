import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import shutil
    from pathlib import Path

    import duckdb
    import marimo as mo

    return Path, duckdb, mo, shutil


@app.cell
def _(mo):
    mo.md(r"""
    # Module 1 lab: a data lake made of plain Parquet files, and where it hurts

    Read alongside `docs/01-modern-data-lake.md`.

    - **Interactive:** `uv run marimo edit labs/01_data_lake/lab.py`
    - **Headless (as a script):** `uv run python labs/01_data_lake/lab.py`

    marimo is reactive. Edit a cell and every cell that depends on it reruns, so
    what you see always matches the code on screen. Everything is written under
    `./lake/module01/` (git-ignored). Changing `ROWS` below rebuilds the whole lake.
    Steps 1 to 4 build a clean lake. Step 5 damages a **separate copy** on purpose,
    so the exercises at the bottom always run against clean data.
    """)
    return


@app.cell
def _(Path, mo, shutil):
    ROWS = 2_000_000
    LAKE = Path(mo.notebook_dir()).resolve().parents[1] / "lake" / "module01"

    shutil.rmtree(LAKE, ignore_errors=True)
    LAKE.mkdir(parents=True)


    def du(path: Path) -> int:
        if path.is_dir():
            return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
        return path.stat().st_size


    def mb(n: int) -> str:
        return f"{n / 1_048_576:,.1f} MB"

    return LAKE, ROWS, du, mb


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 1: generate synthetic orders (about one year, 5 regions)
    """)
    return


@app.cell
def _(LAKE, ROWS, duckdb):
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE orders AS
        SELECT
            i                                                               AS order_id,
            TIMESTAMP '2025-01-01' + to_seconds((i * 15) % 31_536_000)      AS order_ts,
            (hash(i) % 50_000)::INT                                         AS customer_id,
            ['EMEA','NA','LATAM','APAC','ANZ'][(hash(i * 7) % 5)::INT + 1]  AS region,
            round(((hash(i * 13) % 100_000) / 100.0), 2)                    AS amount,
            ['NEW','PAID','SHIPPED','CANCELLED'][(hash(i * 3) % 4)::INT + 1] AS status
        FROM range({ROWS}) t(i)
    """)
    orders_ready = LAKE  # downstream cells depend on this so they rerun after a rebuild
    return con, orders_ready


@app.cell
def _(con, mo, orders_ready):
    _df = mo.sql(
        f"""
        -- {orders_ready.name}
        SELECT * FROM orders LIMIT 5
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 2: same data as CSV vs Parquet (row store vs column store on disk)
    """)
    return


@app.cell
def _(LAKE, con, du, mb, mo, orders_ready):
    _ = orders_ready
    csv_path = LAKE / "orders.csv"
    pq = LAKE / "orders.parquet"
    con.execute(f"COPY orders TO '{csv_path}' (HEADER)")
    con.execute(f"COPY orders TO '{pq}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)")
    csv_bytes, pq_bytes = du(csv_path), du(pq)
    mo.md(f"""
    | File | Size |
    |---|---|
    | CSV | {mb(csv_bytes)} |
    | Parquet | {mb(pq_bytes)} (columnar + dictionary/RLE encoding + zstd) |

    Below is the Parquet footer: one row per (row group, column). Look at min/max
    for `order_ts`. Each row group covers a narrow time range because the data was
    written in time order.
    """)
    return csv_bytes, pq, pq_bytes


@app.cell
def _(con, mo, pq):
    _df = mo.sql(
        f"""
        SELECT row_group_id, num_values, stats_min, stats_max
        FROM parquet_metadata('{pq}')
        WHERE path_in_schema = 'order_ts'
        ORDER BY row_group_id
        LIMIT 6
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 3: Hive-style partitioned layout (one directory per month)
    """)
    return


@app.cell
def _(LAKE, con, du, mb, mo, orders_ready):
    _ = orders_ready
    by_month = LAKE / "orders_by_month"
    con.execute(f"""
        COPY (SELECT *, strftime(order_ts, '%Y-%m') AS month FROM orders)
        TO '{by_month}' (FORMAT parquet, PARTITION_BY (month), COMPRESSION zstd)
    """)
    _dirs = sorted(p.name for p in by_month.iterdir())
    partition_files = len(list(by_month.rglob("*.parquet")))
    mo.md(f"""
    {len(_dirs)} partition directories, e.g. `{_dirs[0]}`, `{_dirs[1]}`, ...
    {partition_files} data files, {mb(du(by_month))} in total.
    """)
    return by_month, partition_files


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 4: how the engine avoids reading data

    **4a. Partition pruning.** Filter on the partition column (the directory name).
    """)
    return


@app.cell
def _(by_month, con, mo):
    _plan = con.sql(f"""
        EXPLAIN ANALYZE
        SELECT region, sum(amount)
        FROM read_parquet('{by_month}/**/*.parquet', hive_partitioning = true)
        WHERE month = '2025-03'
        GROUP BY region
    """).fetchall()[0][1]
    pruning_lines = [
        _l.strip(" │")
        for _l in _plan.splitlines()
        if "Scanning Files" in _l or "Total Files Read" in _l
    ]
    mo.md("\n".join(f"- `{_l}`" for _l in pruning_lines))
    return (pruning_lines,)


@app.cell
def _(mo):
    mo.md(r"""
    **4b. Min/max (zone map) skipping inside one file.** A row group can be skipped
    when its [min, max] range cannot satisfy the predicate.
    """)
    return


@app.cell
def _(con, mo, pq):
    _stats = f"""
        SELECT row_group_id,
               max(TRY_CAST(stats_min AS DOUBLE)) FILTER (WHERE path_in_schema = 'order_id') AS id_min,
               max(TRY_CAST(stats_min AS DOUBLE)) FILTER (WHERE path_in_schema = 'amount')   AS amt_min
        FROM parquet_metadata('{pq}') GROUP BY row_group_id
    """
    _total = con.sql(f"SELECT count(*) FROM ({_stats})").fetchone()[0]
    row_groups_read = {
        _label: con.sql(f"SELECT count(*) FROM ({_stats}) WHERE {_could_match}").fetchone()[0]
        for _label, _could_match in [
            ("order_id < 100000", "id_min < 100000"),
            ("amount < 1", "amt_min < 1"),
        ]
    }
    mo.md(
        "| Predicate | Row groups that must be read |\n|---|---|\n"
        + "\n".join(f"| `{_k}` | {_v} of {_total} |" for _k, _v in row_groups_read.items())
        + "\n\n`order_id` is sorted, so each row group covers a narrow range and most are"
        " skipped. `amount` is random, so every row group spans 0 to 999 and none can be skipped."
    )
    return (row_groups_read,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 5: why plain files are not a table

    This step works on a copy, `orders_by_month_scratch/`, and damages it on purpose.
    """)
    return


@app.cell
def _(LAKE, by_month, con, du, mb, mo, shutil):
    scratch = LAKE / "orders_by_month_scratch"
    shutil.rmtree(scratch, ignore_errors=True)
    shutil.copytree(by_month, scratch)
    _glob = f"{scratch}/**/*.parquet"
    _count = f"SELECT count(*) FROM read_parquet('{_glob}', hive_partitioning = true)"

    # 5a. 'UPDATE' one row in March = rewrite the whole March file(s).
    _march = scratch / "month=2025-03"
    _old_files = sorted(_march.glob("*.parquet"))
    _tmp = LAKE / "march_rewrite.parquet"
    con.execute(f"""
        COPY (SELECT * EXCLUDE (month) REPLACE (
                CASE WHEN order_id = 42 THEN 'CANCELLED' ELSE status END AS status)
              FROM read_parquet('{_march}/*.parquet'))
        TO '{_tmp}' (FORMAT parquet)
    """)
    rewrite_bytes = du(_tmp)

    # 5b. No atomic commit: a reader that lists files mid-rewrite sees duplicates.
    rows_before = con.sql(_count).fetchone()[0]
    shutil.copy(_tmp, _march / "data_new.parquet")  # writer step 1: add new file
    rows_during = con.sql(_count).fetchone()[0]  # a concurrent reader runs here
    for _f in _old_files:  # writer step 2: remove old files
        _f.unlink()
    rows_after = con.sql(_count).fetchone()[0]

    # 5c. Schema drift: a new writer adds a column to new files only.
    con.execute(f"""
        COPY (SELECT *, 'web' AS channel FROM orders WHERE order_ts >= TIMESTAMP '2025-12-06')
        TO '{scratch}/month=2025-12/data_with_channel.parquet' (FORMAT parquet)
    """)
    try:
        con.sql(f"SELECT channel FROM read_parquet('{_glob}', hive_partitioning = true)").fetchall()
        drift_error = "(no error)"
    except Exception as _e:
        drift_error = f"{type(_e).__name__}: {str(_e).splitlines()[0][:100]}"
    drift_rows = con.sql(f"""
        SELECT channel, count(*) FROM read_parquet('{_glob}', hive_partitioning = true, union_by_name = true)
        GROUP BY ALL ORDER BY ALL
    """).fetchall()

    mo.md(f"""
    **5a.** Changing one value rewrote {mb(rewrite_bytes)}.

    **5b.** Rows before = {rows_before:,}, during = **{rows_during:,}**, after = {rows_after:,}.
    The reader in the middle got a wrong answer and nothing told it so.

    **5c.** The default read of the new column fails with `{drift_error}`.
    With `union_by_name = true` it works ({drift_rows}), but every reader must know
    to ask, and those `web` rows also duplicate rows already in December.

    **Takeaway:** the files are fine. What is missing is a **table**: an atomic list
    of which files belong to it, its schema, and its partitioning. That is Iceberg
    (module 2).
    """)
    return rows_after, rows_before, rows_during


@app.cell
def _(mo):
    mo.md(r"""
    ---
    # Exercises

    Edit the cells below freely. They read the clean lake from steps 2 and 3.

    **Exercise 1.** Filter on `order_ts` instead of `month`. How many files are scanned
    now, and why? (This is the problem Iceberg's hidden partitioning solves.)
    """)
    return


@app.cell
def _(by_month, con, mo):
    _plan = con.sql(f"""
        EXPLAIN ANALYZE
        SELECT region, sum(amount)
        FROM read_parquet('{by_month}/**/*.parquet', hive_partitioning = true)
        WHERE order_ts >= TIMESTAMP '2025-03-01' AND order_ts < TIMESTAMP '2025-04-01'
        GROUP BY region
    """).fetchall()[0][1]
    mo.plain_text(_plan)
    return


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 2.** Write the file sorted by a different column and look at row group
    skipping again. Try `amount`, then `region, order_ts`. What happens to
    `amount < 1`, and to `order_id < 100000`? What does that say about choosing a sort order?
    """)
    return


@app.cell
def _(LAKE, con, mo, orders_ready):
    _ = orders_ready
    sort_by = "amount"  # edit me
    sorted_pq = LAKE / "orders_sorted.parquet"
    con.execute(
        f"COPY (SELECT * FROM orders ORDER BY {sort_by}) TO '{sorted_pq}' "
        "(FORMAT parquet, ROW_GROUP_SIZE 122880)"
    )
    _df = mo.sql(
        f"""
        WITH s AS (
            SELECT row_group_id,
                   max(TRY_CAST(stats_min AS DOUBLE)) FILTER (WHERE path_in_schema = 'order_id') AS id_min,
                   max(TRY_CAST(stats_min AS DOUBLE)) FILTER (WHERE path_in_schema = 'amount')   AS amt_min
            FROM parquet_metadata('{sorted_pq}') GROUP BY row_group_id
        )
        SELECT count(*)                                AS row_groups,
               count(*) FILTER (WHERE id_min < 100000) AS read_for_order_id_lt_100000,
               count(*) FILTER (WHERE amt_min < 1)     AS read_for_amount_lt_1
        FROM s
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 3.** Drag the slider to change the Parquet row group size. How do file
    size, row group count and skipping change? Which Exadata trade-off does this remind you of?
    """)
    return


@app.cell
def _(mo):
    row_group_size = mo.ui.slider(
        steps=[10_000, 50_000, 122_880, 500_000, 1_000_000], value=122_880, label="ROW_GROUP_SIZE"
    )
    row_group_size
    return (row_group_size,)


@app.cell
def _(LAKE, con, du, mb, mo, orders_ready, row_group_size):
    _ = orders_ready
    _path = LAKE / "orders_rg.parquet"
    con.execute(
        f"COPY orders TO '{_path}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {row_group_size.value})"
    )
    _groups, _read = con.sql(f"""
        SELECT count(*), count(*) FILTER (WHERE TRY_CAST(stats_min AS DOUBLE) < 100000)
        FROM parquet_metadata('{_path}') WHERE path_in_schema = 'order_id'
    """).fetchone()
    mo.md(f"""
    | ROW_GROUP_SIZE | File size | Row groups | Read for `order_id < 100000` |
    |---|---|---|---|
    | {row_group_size.value:,} | {mb(du(_path))} | {_groups} | {_read} |
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 4.** Explain in two sentences, in Oracle terms, what the reader in
    step 5b was missing. (No code needed. Tell Claude in the project thread.)
    """)
    return


if __name__ == "__main__":
    app.run()
