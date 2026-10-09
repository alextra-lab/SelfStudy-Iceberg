import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import logging

    import duckdb
    import marimo as mo
    import pyarrow.fs as pafs
    import requests
    import sqlalchemy
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table import StaticTable
    from pyiceberg.transforms import MonthTransform
    from pyiceberg.types import DoubleType, IntegerType, LongType, NestedField, StringType, TimestampType

    return (
        CommitFailedException,
        DoubleType,
        IntegerType,
        LongType,
        MonthTransform,
        NestedField,
        PartitionField,
        PartitionSpec,
        RestCatalog,
        Schema,
        StaticTable,
        StringType,
        TimestampType,
        duckdb,
        json,
        logging,
        mo,
        pafs,
        requests,
        sqlalchemy,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Module 3 lab: a real catalog, two engines

    Read alongside `docs/03-catalog-selection.md`.

    This lab needs three services in Docker. Start them once from the repo root:

    ```bash
    docker compose -f labs/03_catalogs/docker-compose.yml up -d
    uv run marimo edit labs/03_catalogs/lab.py
    ```

    | Service | Role | Oracle analogy |
    |---|---|---|
    | **RustFS** (S3 API, port 9000) | Object storage for data and metadata files | ASM disk groups |
    | **Apache Polaris** (port 8181) | Iceberg REST catalog: table name to current metadata file | The data dictionary plus GRANTs |
    | **Trino** (port 8080) | SQL engine | A second database server reading the same tables |

    PyIceberg writes the table, then Trino reads, updates and maintains it through the
    same catalog. No Hadoop and no Hive Metastore. To wipe everything:
    `docker compose -f labs/03_catalogs/docker-compose.yml down -v`.

    Each step re-runs safely. Step 2 drops and rebuilds the table, and every step
    after it runs again in order.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Settings

    Connection details for the three services. This is plumbing, not the lesson:
    every step below shows its own query or API call in full.
    """)
    return


@app.cell
def _():
    S3_ENDPOINT = "http://localhost:9000"
    POLARIS = "http://localhost:8181"
    TRINO = "trino://lab@localhost:8080/lab"  # Trino catalog "lab", see trino/catalog/lab.properties
    # PyIceberg's file access: our own S3 keys. The catalog does not hand out keys
    # in this lab (RustFS has no STS), so the empty X-Iceberg-Access-Delegation
    # header tells Polaris not to try.
    S3_PROPS = {
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": "labadmin",
        "s3.secret-access-key": "labpassword",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
        "header.X-Iceberg-Access-Delegation": "",
    }
    return POLARIS, S3_ENDPOINT, S3_PROPS, TRINO


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 0: are the services up?
    """)
    return


@app.cell
def _(POLARIS, S3_ENDPOINT, mo, requests):
    def _up(url):
        try:
            requests.get(url, timeout=3)
            return True
        except requests.ConnectionError:
            return False

    _status = {
        "RustFS": _up(S3_ENDPOINT),
        "Polaris": _up(f"{POLARIS}/api/catalog/v1/config"),
        "Trino": _up("http://localhost:8080/v1/info"),
    }
    mo.stop(
        not all(_status.values()),
        mo.md(
            f"Not running: {', '.join(k for k, v in _status.items() if not v)}. Start them with "
            "`docker compose -f labs/03_catalogs/docker-compose.yml up -d`, wait about 30 seconds, then re-run this cell."
        ),
    )
    services_up = True
    mo.md("RustFS, Polaris and Trino are all answering.")
    return (services_up,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 1: create the storage and the catalog

    Three one-time admin actions, like a DBA setting up a new database:

    1. Create the bucket `lab` (Oracle: create the disk group).
    2. Log in to Polaris and create a catalog named `lab` whose tables live under
       `s3://lab/warehouse` (Oracle: `CREATE TABLESPACE`). Polaris refuses table
       locations outside `allowedLocations`.
    3. Grant the catalog's admin role `CATALOG_MANAGE_CONTENT`, the full set of
       table privileges (Oracle: `GRANT ALL ...`). Without it, step 2's
       `DROP ... PURGE` is refused when you re-run the lab.

    The management API is plain JSON over HTTP. Here is every call:
    """)
    return


@app.cell
def _(POLARIS, S3_ENDPOINT, json, mo, pafs, requests, services_up):
    _ = services_up
    # 1. The bucket.
    _s3 = pafs.S3FileSystem(
        access_key="labadmin", secret_key="labpassword", endpoint_override=S3_ENDPOINT,
        scheme="http", region="us-east-1", allow_bucket_creation=True,
    )
    _s3.create_dir("lab")

    # 2. Log in (OAuth2 client credentials), then create the catalog.
    _token = requests.post(
        f"{POLARIS}/api/catalog/v1/oauth/tokens",
        data={"grant_type": "client_credentials", "client_id": "root", "client_secret": "secret", "scope": "PRINCIPAL_ROLE:ALL"},
    ).json()["access_token"]
    polaris_headers = {"Authorization": f"Bearer {_token}"}

    _catalog_body = {
        "catalog": {
            "name": "lab",
            "type": "INTERNAL",
            "properties": {
                "default-base-location": "s3://lab/warehouse",
                "polaris.config.drop-with-purge.enabled": "true",  # so step 2 can drop and rebuild
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": ["s3://lab/warehouse"],
                "endpoint": "http://localhost:9000",  # how clients on your machine reach RustFS
                "endpointInternal": "http://rustfs:9000",  # how Polaris, inside Docker, reaches it
                "pathStyleAccess": True,
                "stsUnavailable": True,  # RustFS has no STS, so no per-table temporary keys
                "region": "us-east-1",
            },
        }
    }
    _create = requests.post(f"{POLARIS}/api/management/v1/catalogs", json=_catalog_body, headers=polaris_headers)

    # 3. The grant.
    _grant = requests.put(
        f"{POLARIS}/api/management/v1/catalogs/lab/catalog-roles/catalog_admin/grants",
        json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
        headers=polaris_headers,
    )
    mo.md(f"""
    `POST /api/management/v1/catalogs` returned **{_create.status_code}**
    ({"created" if _create.status_code == 201 else "already exists" if _create.status_code == 409 else _create.text}).

    `PUT .../catalog-roles/catalog_admin/grants` (`CATALOG_MANAGE_CONTENT`) returned **{_grant.status_code}**.

    The catalog body that was sent:

    ```json
    {json.dumps(_catalog_body, indent=2)}
    ```
    """)
    return (polaris_headers,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 2: PyIceberg creates and loads the table through the REST catalog

    The table definition and the 2M-row generator are identical to module 2. The
    only change is the catalog: `RestCatalog` (HTTP to Polaris) instead of
    `SqlCatalog` (a SQLite file).
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
    RestCatalog,
    S3_PROPS,
    Schema,
    StringType,
    TimestampType,
    duckdb,
    mo,
    polaris_headers,
):
    _ = polaris_headers
    catalog = RestCatalog(
        "lab",
        uri="http://localhost:8181/api/catalog",
        warehouse="lab",  # the Polaris catalog created in step 1
        credential="root:secret",
        scope="PRINCIPAL_ROLE:ALL",
        **{"oauth2-server-uri": "http://localhost:8181/api/catalog/v1/oauth/tokens"},
        **S3_PROPS,
    )
    catalog.create_namespace_if_not_exists("sales")
    if catalog.table_exists("sales.orders"):
        catalog.purge_table("sales.orders")  # start clean on every run: DROP TABLE ... PURGE

    orders = catalog.create_table(
        "sales.orders",
        schema=Schema(
            NestedField(field_id=1, name="order_id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="order_ts", field_type=TimestampType(), required=False),
            NestedField(field_id=3, name="customer_id", field_type=IntegerType(), required=False),
            NestedField(field_id=4, name="region", field_type=StringType(), required=False),
            NestedField(field_id=5, name="amount", field_type=DoubleType(), required=False),
            NestedField(field_id=6, name="status", field_type=StringType(), required=False),
        ),
        partition_spec=PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=MonthTransform(), name="order_ts_month")
        ),
    )

    _rows = duckdb.sql("""
        SELECT
            i                                                               AS order_id,
            TIMESTAMP '2025-01-01' + to_seconds((i * 15) % 31_536_000)      AS order_ts,
            (hash(i) % 50_000)::INT                                         AS customer_id,
            ['EMEA','NA','LATAM','APAC','ANZ'][(hash(i * 7) % 5)::INT + 1]  AS region,
            round(((hash(i * 13) % 100_000) / 100.0), 2)                    AS amount,
            ['NEW','PAID','SHIPPED','CANCELLED'][(hash(i * 3) % 4)::INT + 1] AS status
        FROM range(2_000_000) t(i)
    """).to_arrow_table()
    orders.append(_rows)
    first_snapshot_id = orders.current_snapshot().snapshot_id

    mo.md(f"""
    Table location: `{orders.location()}`

    Current metadata file: `{orders.metadata_location.rsplit("/", 1)[1]}`

    First snapshot: `{first_snapshot_id}`
    """)
    return catalog, first_snapshot_id, orders


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 3: what the catalog actually stores

    In module 2 the whole catalog was one SQLite row: table name and metadata file.
    A REST catalog answers the same question over HTTP. This is the request every
    engine sends when you run `SELECT ... FROM sales.orders` (the REST spec's
    `loadTable`), and the parts of the answer that matter:
    """)
    return


@app.cell
def _(POLARIS, first_snapshot_id, mo, polaris_headers, requests):
    _ = first_snapshot_id
    _r = requests.get(f"{POLARIS}/api/catalog/v1/lab/namespaces/sales/tables/orders", headers=polaris_headers).json()
    mo.md(f"""
    `GET /api/catalog/v1/lab/namespaces/sales/tables/orders`

    | Field in the response | Value |
    |---|---|
    | `metadata-location` (the pointer) | `{_r["metadata-location"]}` |
    | `metadata.format-version` | {_r["metadata"]["format-version"]} |
    | `metadata.current-snapshot-id` | {_r["metadata"]["current-snapshot-id"]} |
    | number of `metadata.snapshots` | {len(_r["metadata"]["snapshots"])} |

    The server read `metadata.json` from RustFS and returned it, so the engine does
    not have to. A commit goes the other way: the engine sends "add this snapshot,
    but only if the current snapshot is still X", and Polaris does the
    compare-and-swap (module 2, section 4) on its side.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 4: Trino reads the table PyIceberg wrote

    Trino knows nothing about PyIceberg. It finds `sales.orders` by asking Polaris,
    exactly as in step 3. Trino's catalog `lab` is configured in
    `labs/03_catalogs/trino/catalog/lab.properties`.
    """)
    return


@app.cell
def _(TRINO, first_snapshot_id, sqlalchemy):
    _ = first_snapshot_id
    trino = sqlalchemy.create_engine(TRINO)
    return (trino,)


@app.cell
def _(mo, trino):
    _df = mo.sql(
        """
        SELECT count(*)      AS orders,
               min(order_ts) AS first_order,
               max(order_ts) AS last_order
        FROM sales.orders
        """,
        engine=trino,
    )
    return


@app.cell
def _(mo, trino):
    _df = mo.sql(
        """
        SELECT committed_at, snapshot_id, parent_id, operation
        FROM sales."orders$snapshots"
        ORDER BY committed_at
        """,
        engine=trino,
    )
    trino_saw_first_snapshot = True
    return (trino_saw_first_snapshot,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 5: Trino writes, and writes differently

    Module 2 cancelled order 400000 with PyIceberg's `overwrite`, which rewrote
    the whole March file (**copy-on-write**). Trino's `UPDATE` instead writes a small
    **delete file** that says "row 60160 of that March file is gone", plus a new
    data file holding the updated row (**merge-on-read**, format v2). Readers merge
    the two at query time.

    Oracle analogy: copy-on-write rebuilds the whole HCC compression unit.
    Merge-on-read marks the old row deleted and writes the new version elsewhere,
    the way an update to HCC data migrates the row. Reads pay for it until it is
    reorganized.
    """)
    return


@app.cell
def _(mo, trino, trino_saw_first_snapshot):
    _ = trino_saw_first_snapshot
    _df = mo.sql(
        """
        UPDATE sales.orders
        SET status = 'CANCELLED'
        WHERE order_id = 400000
        """,
        engine=trino,
    )
    trino_updated = True
    return (trino_updated,)


@app.cell
def _(mo, trino, trino_updated):
    _ = trino_updated
    _df = mo.sql(
        """
        SELECT CASE content WHEN 0 THEN 'data' WHEN 1 THEN 'position deletes' ELSE 'equality deletes' END AS file_type,
               count(*)          AS files,
               sum(record_count) AS records
        FROM sales."orders$files"
        GROUP BY 1
        ORDER BY 1
        """,
        engine=trino,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 6: compact March, then PyIceberg reads Trino's change

    Here is a real interoperability gap, found while building this lab: PyIceberg
    0.12 (with pyarrow 25) fails on the delete files Trino 483 writes ("Not yet
    implemented: DecodeArrow ... DeltaLengthByteArrayDecoder"). Both follow the
    spec, but they differ in which Parquet encodings they handle.

    So, before PyIceberg reads, Trino **compacts** March with `optimize`. That
    rewrites the files with the delete applied and drops the delete file. This is
    routine maintenance anyway (module 6), and it is why the engines you plan to
    mix belong on your catalog checklist.
    """)
    return


@app.cell
def _(mo, trino, trino_updated):
    _ = trino_updated
    _df = mo.sql(
        """
        ALTER TABLE sales.orders EXECUTE optimize
        WHERE order_ts >= TIMESTAMP '2025-03-01' AND order_ts < TIMESTAMP '2025-04-01'
        """,
        engine=trino,
    )
    march_compacted = True
    return (march_compacted,)


@app.cell
def _(march_compacted, mo, orders):
    _ = march_compacted
    orders.refresh()  # ask the catalog for the current metadata file again
    status_in_pyiceberg = orders.scan(row_filter="order_id = 400000").to_arrow()["status"][0].as_py()
    mo.md(f"""
    | Query (PyIceberg) | status of order 400000 |
    |---|---|
    | `orders.refresh()` then `orders.scan(row_filter="order_id = 400000")` | **{status_in_pyiceberg}** |

    Trino changed it, and PyIceberg sees the change, because both go through the same catalog.
    """)
    return (status_in_pyiceberg,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 7: a tag made in PyIceberg, read in Trino

    Module 2 step 6b created a tag (Oracle: a restore point). The tag lives in
    `metadata.json`, so every engine sees it. PyIceberg creates the tag
    `before_fix` on the first snapshot, and Trino reads the table as of that tag.
    """)
    return


@app.cell
def _(first_snapshot_id, orders, status_in_pyiceberg):
    _ = status_in_pyiceberg
    if orders.snapshot_by_name("before_fix") is None:  # once only: a second create fails
        orders.manage_snapshots().create_tag(first_snapshot_id, "before_fix").commit()
    tag_created = True
    return (tag_created,)


@app.cell
def _(mo, tag_created, trino):
    _ = tag_created
    _df = mo.sql(
        """
        SELECT 'before_fix' AS version, status FROM sales.orders FOR VERSION AS OF 'before_fix' WHERE order_id = 400000
        UNION ALL
        SELECT 'current'    AS version, status FROM sales.orders                              WHERE order_id = 400000
        """,
        engine=trino,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 8: two writers start from the same version

    This is module 2 exercise 4, for real. Writers `w1` and `w2` both load the
    table, so both start from the same snapshot. `w1` appends and commits first.
    When `w2` commits, the catalog rejects its compare-and-swap (HTTP 409), because
    the current snapshot is no longer the one `w2` started from. PyIceberg then
    reloads and retries on top of `w1`'s commit. The warning it logs is captured
    below.

    Oracle would make `w2` wait on a lock. Iceberg lets both work and sorts it out
    at commit time.
    """)
    return


@app.cell
def _(catalog, logging, mo, tag_created):
    _ = tag_created
    _log = []
    _handler = logging.Handler()
    _handler.emit = lambda record: _log.append(record.getMessage())
    logging.getLogger("pyiceberg.table").addHandler(_handler)

    w1 = catalog.load_table("sales.orders")
    w2 = catalog.load_table("sales.orders")  # same starting snapshot as w1
    _one_row = w1.scan(row_filter="order_id = 1").to_arrow()

    w1.append(_one_row)  # commits first
    w2.append(_one_row)  # its starting snapshot is stale now
    logging.getLogger("pyiceberg.table").removeHandler(_handler)

    writers_done = True
    mo.md(
        "Both appends committed. PyIceberg logged:\n\n"
        + ("\n".join(f"- `{_m}`" for _m in _log) or "- (nothing: no conflict this time)")
    )
    return (writers_done,)


@app.cell
def _(mo, trino, writers_done):
    _ = writers_done
    _df = mo.sql(
        """
        SELECT committed_at, snapshot_id, parent_id, operation,
               element_at(summary, 'added-records') AS added_records
        FROM sales."orders$snapshots"
        ORDER BY committed_at
        """,
        engine=trino,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    The last two rows are `w1` and `w2`. `w2`'s `parent_id` is `w1`'s snapshot,
    not the snapshot `w2` started from: the retry rebuilt `w2`'s commit on top of
    `w1`'s. Its data file was written once and reused.

    ## Step 9: maintenance is still your job

    Polaris stores the pointer and enforces the swap. It does not expire snapshots
    for you. Trino runs the job, here with a 0-second retention so you can see the
    effect (the default minimum is 7 days; `lab.properties` lowers it).
    """)
    return


@app.cell
def _(mo, trino, writers_done):
    _ = writers_done
    _df = mo.sql(
        """
        ALTER TABLE sales.orders EXECUTE expire_snapshots(retention_threshold => '0s')
        """,
        engine=trino,
    )
    snapshots_expired = True
    return (snapshots_expired,)


@app.cell
def _(mo, snapshots_expired, trino):
    _ = snapshots_expired
    _df = mo.sql(
        """
        SELECT s.snapshot_id, s.operation, r.name AS referenced_by
        FROM sales."orders$snapshots" s
        LEFT JOIN sales."orders$refs" r ON r.snapshot_id = s.snapshot_id
        ORDER BY s.committed_at
        """,
        engine=trino,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    Two snapshots survive: the current one (`main`) and the one the tag
    `before_fix` points to. **A tag pins its snapshot and its data files**, just as
    an Oracle guaranteed restore point keeps flashback logs. Expiry skips them
    until you drop the tag.

    ---
    # Exercises
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 1. Time travel by time instead of by tag.** This is the step 7 query
    with one change: `FOR TIMESTAMP AS OF` instead of `FOR VERSION AS OF 'before_fix'`.
    The time is the first snapshot's commit time plus one second, and that
    snapshot still exists (step 9 kept it for the tag). You might expect `PAID`.
    Instead Trino refuses. Why does time travel by timestamp fail, when the tag
    query in step 7, which reaches the same snapshot, still works? Hint: look at
    what `expire_snapshots` removes besides snapshots (lesson section 6).
    """)
    return


@app.cell
def _(mo, orders, snapshots_expired, trino):
    _ = snapshots_expired
    from datetime import datetime, timezone

    orders.refresh()
    _first_commit = datetime.fromtimestamp(orders.snapshots()[0].timestamp_ms / 1000, tz=timezone.utc)
    _as_of = _first_commit.strftime("%Y-%m-%d %H:%M:%S")
    try:
        _df = mo.sql(
            f"""
            SELECT 'as of {_as_of} UTC' AS version, status
            FROM sales.orders FOR TIMESTAMP AS OF TIMESTAMP '{_as_of} UTC' + INTERVAL '1' SECOND
            WHERE order_id = 400000
            """,
            engine=trino,
        )
    except Exception as _err:
        _df = mo.md(f"Trino refused: `{str(_err).splitlines()[0][:300]}`")
    _df
    return


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 2. Turn off the retry.** The step 8 cell again, with one change: the
    table property `commit.retry.num-retries` is set to `0` first. What happens to
    `w2` now? Which part of Iceberg rejected it: PyIceberg, Polaris or RustFS?
    """)
    return


@app.cell
def _(CommitFailedException, catalog, mo, snapshots_expired):
    _ = snapshots_expired
    with catalog.load_table("sales.orders").transaction() as _tx:
        _tx.set_properties({"commit.retry.num-retries": "0"})  # the one change from step 8

    _w1 = catalog.load_table("sales.orders")
    _w2 = catalog.load_table("sales.orders")  # same starting snapshot as _w1
    _one_row = _w1.scan(row_filter="order_id = 1").to_arrow()

    _w1.append(_one_row)  # commits first
    try:
        _w2.append(_one_row)  # its starting snapshot is stale now
        exercise2_result = "w2 committed"
    except CommitFailedException as _err:
        exercise2_result = f"CommitFailedException: {_err}"

    with catalog.load_table("sales.orders").transaction() as _tx:
        _tx.remove_properties("commit.retry.num-retries")  # back to the default (4 retries)
    mo.md(f"`w2.append(...)` result: **{exercise2_result[:300]}**")
    return (exercise2_result,)


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 3. Read without the catalog.** PyIceberg's `StaticTable` opens a
    table straight from a `metadata.json` path, with no catalog involved. The cell
    reads the row count from the current metadata file and from the `00001` file
    written by step 2. Only the path changes. Both reads succeed, but which one is
    right, and how would a reader know which file is current without a catalog?
    """)
    return


@app.cell
def _(S3_PROPS, StaticTable, catalog, exercise2_result, mo):
    _ = exercise2_result
    _table = catalog.load_table("sales.orders")
    _current = _table.metadata_location
    _first = next(_e.metadata_file for _e in _table.metadata.metadata_log if "/00001-" in _e.metadata_file)
    exercise3_rows = {}
    for _path in [_current, _first]:  # edit me
        exercise3_rows[_path.rsplit("/", 1)[1][:5]] = StaticTable.from_metadata(_path, properties=S3_PROPS).scan().to_arrow().num_rows
    mo.md(
        "| metadata file | rows |\n|---|---|\n"
        + "\n".join(f"| `{_k}...` | {_v:,} |" for _k, _v in exercise3_rows.items())
    )
    return (exercise3_rows,)


@app.cell
def _(mo):
    mo.md(r"""
    **Exercise 4. Pick a catalog.** Use the decision table in section 5 of the
    lesson. For each case, which catalog would you choose, and why?

    1. A team runs Trino and Spark on AWS. They want no servers to patch and
       compaction handled for them.
    2. You on a laptop, for this course: PyIceberg, DuckDB and chDB, no Docker.
    3. An on-premises company with Trino and Spark, no public cloud, and a DBA team
       that already runs PostgreSQL.
    """)
    return


if __name__ == "__main__":
    app.run()
