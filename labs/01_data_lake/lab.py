"""Module 1 lab: a data lake made of plain Parquet files, and where it hurts.

Run:  uv run python labs/01_data_lake/lab.py
Everything is written under ./lake/ (git-ignored). Re-running starts clean.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

LAKE = Path(__file__).resolve().parents[2] / "lake" / "module01"
ROWS = 2_000_000


def banner(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def du(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) if path.is_dir() else path.stat().st_size


def mb(n: int) -> str:
    return f"{n / 1_048_576:,.1f} MB"


def step1_generate(con: duckdb.DuckDBPyConnection) -> None:
    banner("Step 1: generate 2M synthetic orders (one year, 5 regions)")
    con.execute(f"""
        CREATE TABLE orders AS
        SELECT
            i                                                     AS order_id,
            TIMESTAMP '2025-01-01' + to_seconds((i * 15) % 31_536_000)  AS order_ts,
            (hash(i) % 50_000)::INT                               AS customer_id,
            ['EMEA','NA','LATAM','APAC','ANZ'][(hash(i * 7) % 5)::INT + 1] AS region,
            round(((hash(i * 13) % 100_000) / 100.0), 2)          AS amount,
            ['NEW','PAID','SHIPPED','CANCELLED'][(hash(i * 3) % 4)::INT + 1] AS status
        FROM range({ROWS}) t(i)
    """)
    print(con.sql("SELECT * FROM orders LIMIT 5"))


def step2_formats(con: duckdb.DuckDBPyConnection) -> None:
    banner("Step 2: same data as CSV vs Parquet (row store vs column store on disk)")
    csv, pq = LAKE / "orders.csv", LAKE / "orders.parquet"
    con.execute(f"COPY orders TO '{csv}' (HEADER)")
    con.execute(f"COPY orders TO '{pq}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)")
    print(f"CSV     : {mb(du(csv))}")
    print(f"Parquet : {mb(du(pq))}  (columnar + dictionary/RLE encoding + zstd)")

    print("\nParquet footer: one row per (row group, column). Look at min/max for order_ts:")
    print(con.sql(f"""
        SELECT row_group_id, num_values, stats_min, stats_max
        FROM parquet_metadata('{pq}')
        WHERE path_in_schema = 'order_ts'
        ORDER BY row_group_id LIMIT 6
    """))


def step3_partition(con: duckdb.DuckDBPyConnection) -> None:
    banner("Step 3: Hive-style partitioned layout (directory per month)")
    out = LAKE / "orders_by_month"
    con.execute(f"""
        COPY (SELECT *, strftime(order_ts, '%Y-%m') AS month FROM orders)
        TO '{out}' (FORMAT parquet, PARTITION_BY (month), COMPRESSION zstd)
    """)
    dirs = sorted(p.name for p in out.iterdir())
    print(f"{len(dirs)} partition directories, e.g. {dirs[:3]} ...")
    files = sorted(out.rglob("*.parquet"))
    print(f"{len(files)} data files, total {mb(du(out))}")


def step4_pruning(con: duckdb.DuckDBPyConnection) -> None:
    banner("Step 4: how the engine avoids reading data")
    glob = f"{LAKE}/orders_by_month/**/*.parquet"

    print("4a. Partition pruning: filter on the partition column (directory names)")
    plan = con.sql(f"""
        EXPLAIN ANALYZE
        SELECT region, sum(amount) FROM read_parquet('{glob}', hive_partitioning = true)
        WHERE month = '2025-03' GROUP BY region
    """).fetchall()[0][1]
    for line in plan.splitlines():
        if "Filter" in line or "Scanning Files" in line or "Total Files" in line:
            print("   ", line.strip(" │"))

    print("\n4b. Min/max (zone map) skipping inside one file: filter on a non-partition column")
    pq = LAKE / "orders.parquet"
    stats = f"""
        SELECT row_group_id,
               max(TRY_CAST(stats_min AS DOUBLE)) FILTER (WHERE path_in_schema = 'order_id') AS id_min,
               max(TRY_CAST(stats_max AS DOUBLE)) FILTER (WHERE path_in_schema = 'order_id') AS id_max,
               max(TRY_CAST(stats_min AS DOUBLE)) FILTER (WHERE path_in_schema = 'amount')   AS amt_min,
               max(TRY_CAST(stats_max AS DOUBLE)) FILTER (WHERE path_in_schema = 'amount')   AS amt_max
        FROM parquet_metadata('{pq}') GROUP BY row_group_id
    """
    total = con.sql(f"SELECT count(*) FROM ({stats})").fetchone()[0]
    # A row group can be skipped when its [min, max] range cannot satisfy the predicate.
    for label, could_match in [("order_id < 100000", "id_min < 100000"),
                               ("amount < 1       ", "amt_min < 1")]:
        n = con.sql(f"SELECT count(*) FROM ({stats}) WHERE {could_match}").fetchone()[0]
        print(f"    WHERE {label} -> must read {n:>2} of {total} row groups")
    print("    order_id is sorted, so each row group covers a narrow range and most are skipped.")
    print("    amount is random, so every row group spans 0..999 and none can be skipped.")


def step5_pain(con: duckdb.DuckDBPyConnection) -> None:
    banner("Step 5: why plain files are not a table")
    out = LAKE / "orders_by_month"
    glob = f"{out}/**/*.parquet"
    q = f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning = true)"

    print("5a. 'UPDATE' one row in March = rewrite the whole March file(s).")
    march = out / "month=2025-03"
    old_files = sorted(march.glob("*.parquet"))
    tmp = LAKE / "march_rewrite.parquet"
    con.execute(f"""
        COPY (SELECT * EXCLUDE (month) REPLACE (
                CASE WHEN order_id = 42 THEN 'CANCELLED' ELSE status END AS status)
              FROM read_parquet('{march}/*.parquet'))
        TO '{tmp}' (FORMAT parquet)
    """)
    print(f"    rewrote {mb(du(tmp))} to change one value")

    print("\n5b. No atomic commit: a reader that lists files mid-rewrite sees duplicates.")
    before = con.sql(q).fetchone()[0]
    shutil.copy(tmp, march / "data_new.parquet")      # writer step 1: add new file
    during = con.sql(q).fetchone()[0]                  # a concurrent reader runs here
    for f in old_files:                                # writer step 2: remove old files
        f.unlink()
    after = con.sql(q).fetchone()[0]
    print(f"    rows before={before:,}  during={during:,}  after={after:,}")
    print("    The reader in the middle got a wrong answer and nothing told it so.")

    print("\n5c. Schema drift: a new writer adds a column to new files only.")
    con.execute(f"""
        COPY (SELECT *, 'web' AS channel FROM orders WHERE order_ts >= TIMESTAMP '2025-12-06')
        TO '{out}/month=2025-12/data_with_channel.parquet' (FORMAT parquet)
    """)
    try:
        con.sql(f"SELECT channel, count(*) FROM read_parquet('{glob}', hive_partitioning = true) GROUP BY ALL").fetchall()
    except duckdb.Error as e:
        print(f"    default read fails: {type(e).__name__}: {str(e).splitlines()[0][:90]}")
    rows = con.sql(f"""
        SELECT channel, count(*) FROM read_parquet('{glob}', hive_partitioning = true, union_by_name = true)
        GROUP BY ALL ORDER BY ALL
    """).fetchall()
    print(f"    union_by_name=true works, but every reader must know to ask: {rows}")
    print("    (and those 'web' rows are also duplicates of rows already in December)")

    print("\nTakeaway: the files are fine. What is missing is a TABLE: an atomic list of")
    print("which files are in it, its schema, and its partitioning. That is Iceberg (module 2).")


def main() -> None:
    shutil.rmtree(LAKE, ignore_errors=True)
    LAKE.mkdir(parents=True)
    con = duckdb.connect()
    step1_generate(con)
    step2_formats(con)
    step3_partition(con)
    step4_pruning(con)
    step5_pain(con)


if __name__ == "__main__":
    main()
