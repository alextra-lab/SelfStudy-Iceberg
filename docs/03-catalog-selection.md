# Module 3: Catalog selection

Goal: know what an Iceberg catalog does, which kinds exist, and how to choose one
without dragging in Hadoop. Lab: `labs/03_catalogs/lab.py`, which runs a real REST
catalog (Apache Polaris), S3-compatible storage (RustFS) and Trino in Docker.

## 1. What a catalog does

Module 2 showed that the catalog's core job is tiny: map a table name to its
current `metadata.json`, and change that pointer only with a compare-and-swap.
Everything else about the table lives in files next to the data.

In production, a catalog also takes on the jobs Oracle's data dictionary and
listener do:

| Job | Oracle | Iceberg catalog |
|---|---|---|
| Name to object | `DBA_TABLES` maps a name to a segment | Table name to current metadata file |
| Atomic commit | The redo and SCN machinery | Compare-and-swap on the pointer (lab step 8) |
| Namespaces | Schemas | Namespaces (`sales`) |
| Who can do what | `GRANT SELECT ON ...`, roles | Principals, roles and privileges in the catalog (the lab logs in as Polaris's bootstrap admin) |
| No direct disk access | Users never touch ASM | **Credential vending**: the catalog hands an engine short-lived storage keys that work for that one table only |
| Finding the database | The listener and `tnsnames.ora` | One URL every engine is configured with |

The last two matter most at scale. Without credential vending, every engine and
user needs long-lived keys to the whole bucket, which is like giving everyone
the ASM password.

## 2. The kinds of catalog

| Kind | How it commits | Engines | Verdict |
|---|---|---|---|
| **Hadoop (file-system) catalog** | Relies on atomic file rename, which S3 does not have | Spark, Flink | Unsafe on object storage. Avoid |
| **Hive Metastore** | Thrift service over an RDBMS, with locks | Almost all | Works, but drags in Hadoop libraries and a Java service built for Hive. This is the setup you don't want. Legacy |
| **JDBC / SQL catalog** | One row per table in PostgreSQL, MySQL or SQLite, swapped with a conditional `UPDATE` | Spark, Flink, Trino, PyIceberg (`SqlCatalog`, module 2). Not DuckDB | Simple and safe. But every engine needs the database password, and there are no grants or credential vending |
| **REST catalog** | The server does the swap. Engines speak one HTTP API (the Iceberg REST spec) | Trino, Spark, Flink, PyIceberg, DuckDB, Snowflake and others | **The standard.** Pick an implementation (section 3) |
| **DuckLake** (not Iceberg) | All metadata lives in a SQL database, not in files | DuckDB | A different design. See module 8 |

The REST spec matters because it moves the hard parts to the server: the commit
logic, access control, credential vending and multi-table commits. Engines only
need an HTTP client. A new catalog product then works with every engine that
speaks REST, the way any Oracle client works with any listener.

## 3. REST catalog implementations

Self-hosted:

| Catalog | Written in | Backing store | Notable |
|---|---|---|---|
| **Apache Polaris** (used in the lab) | Java | PostgreSQL (in-memory for dev) | Started at Snowflake. Fine-grained RBAC and credential vending. Snowflake's hosted version is Snowflake Open Catalog |
| **Lakekeeper** | Rust | PostgreSQL | A single small binary, easy to run. Credential vending for S3, Azure and GCS |
| **Project Nessie** | Java | PostgreSQL and others | Git-style branches and commits across *many* tables at once |
| **Apache Gravitino** | Java | RDBMS | A "catalog of catalogs" that federates Iceberg, Hive and others |
| **Unity Catalog OSS** | Java | RDBMS | Databricks' open-source catalog, with an Iceberg REST endpoint |

Managed:

| Service | Notes |
|---|---|
| **AWS Glue Data Catalog** | Has an Iceberg REST endpoint. You still run maintenance yourself |
| **Amazon S3 Tables** | A bucket type with a built-in catalog. Runs compaction, snapshot expiry and orphan-file cleanup for you |
| **Snowflake Open Catalog** | Hosted Polaris |
| **Databricks Unity Catalog** | Iceberg REST access to Unity-managed tables |
| **Google BigLake metastore** | Iceberg REST catalog on GCS |
| **Cloudflare R2 Data Catalog** | Managed REST catalog on R2, with managed compaction |
| **Starburst Galaxy** | Its own metastore for Iceberg, with scheduled maintenance features. Check current details with Starburst |

Product features in this space change quickly. Treat these tables as a
starting point and confirm details against each product's docs before choosing.

## 4. What the lab measured

The lab runs **Polaris 1.8.0**, **Trino 483**, **RustFS 1.0.1** and **PyIceberg
0.12** (October 2026). Some findings matter for choosing a catalog:

- **One table, two engines.** PyIceberg created and loaded the table, and Trino
  read, updated, compacted and expired it, all through Polaris.
- **Engines write differently.** Trino's `UPDATE` wrote a position-delete file
  (merge-on-read). PyIceberg 0.12 then failed to read it ("Not yet implemented:
  DecodeArrow ... DeltaLengthByteArrayDecoder"), so the lab compacts with Trino's
  `optimize` before PyIceberg reads. Test the exact engine versions you plan to
  mix.
- **Credential vending is not optional for every engine.** chDB 26.9 (ClickHouse's
  `DataLakeCatalog` engine) listed Polaris's tables, but every read failed: chDB
  always asks for vended credentials, and RustFS has no STS to issue them. PyIceberg
  and Trino could be told to use their own keys. On AWS S3, Polaris would vend
  credentials and this gap would close.
- **Polaris's admin role starts without table privileges.** The bootstrap admin
  could create the catalog and tables, but `DROP TABLE ... PURGE` was refused
  until `CATALOG_MANAGE_CONTENT` was granted (lab step 1). Plan your roles like
  Oracle grants.
- **Clients ask for vended credentials by default.** PyIceberg sends
  `X-Iceberg-Access-Delegation: vended-credentials` on every request. Polaris
  rejected the table create twice: first because the admin role lacked the
  delegation privilege, then, after a grant, because it had no STS to vend from. The lab sends an
  empty header so PyIceberg uses its own keys (the `S3_PROPS` cell).
- **MinIO's community image is no longer on Docker Hub.** The lab uses RustFS, an
  Apache-licensed S3-compatible server. Any S3 API store works: SeaweedFS, Garage,
  Ceph RGW or real S3.
- **In-memory Polaris forgets everything on restart.** Production Polaris needs
  PostgreSQL behind it, just as Oracle's dictionary needs durable storage.

## 5. How to choose

Ask these in order:

1. **Which engines must read and write?** Every engine on your list must speak the
   catalog's API. REST is the only kind they all share.
2. **Where does the data live?** On a cloud, the cloud's own catalog (Glue, S3
   Tables, BigLake) usually wins on credentials and permissions. On-premises,
   self-host Polaris or Lakekeeper.
3. **Who runs maintenance?** If nobody wants to schedule jobs, choose a catalog
   that runs them (S3 Tables, R2 Data Catalog, some vendor platforms).
   Otherwise, plan the jobs in module 6.
4. **Security model.** Do you need per-table grants and credential vending
   (Polaris, Lakekeeper, the cloud catalogs)? Or is "every engine has the bucket
   key" acceptable (JDBC)?
5. **Multi-table transactions or branches?** If you need git-style branches across
   many tables, look at Nessie. Iceberg's own branches and tags are per table.
6. **What can your team operate?** A REST catalog is a stateless service plus a
   PostgreSQL database. That is far less than a Hive Metastore with its Hadoop
   dependencies.

| Situation | Reasonable choice |
|---|---|
| Laptop, learning, one Python process | PyIceberg `SqlCatalog` on SQLite (module 2) |
| On-premises, several engines, existing PostgreSQL skills | Polaris or Lakekeeper on PostgreSQL |
| AWS, no servers to patch, maintenance included | S3 Tables (or Glue, plus your own maintenance jobs) |
| Already on Snowflake or Databricks | Their catalog, through its Iceberg REST endpoint |
| Starburst as the main engine | Galaxy's catalog, or any REST catalog Trino can reach |

## 6. Snapshots and catalogs: who expires what

Module 2 promised a comparison of catalogs on maintenance. A self-hosted REST
catalog stores the pointer and enforces the swap, and that is all. **Polaris,
Lakekeeper and Nessie do not expire snapshots or compact files on their own.**
An engine runs those jobs (Trino `EXECUTE expire_snapshots`, `optimize`,
`remove_orphan_files`, or Spark's `CALL system...`) on a schedule you set up. The
managed catalogs that advertise automatic maintenance, such as S3 Tables and R2
Data Catalog, run those same jobs for you.

Lab step 9 and exercise 1 show two details:

- A **tag pins its snapshot**. `expire_snapshots` with a 0-second retention kept
  the tagged snapshot and its files.
- Expiry also trims the table's **history** (the `snapshot-log` in
  `metadata.json`). The tagged snapshot still exists, but its history entry is
  gone, so `FOR TIMESTAMP AS OF` cannot find it any more. `FOR VERSION AS OF
  'before_fix'` still works. If you need "as of last month-end", tag it.

## 7. Run the lab

```bash
uv sync
docker compose -f labs/03_catalogs/docker-compose.yml up -d    # about 4 GB of images on first run
uv run marimo edit labs/03_catalogs/lab.py
```

What you should see:

| Step | Result |
|---|---|
| 0 | All three services answer |
| 1 | Catalog created (201, or 409 when it already exists) and the grant applied (201) |
| 2 | Table at `s3://lab/warehouse/sales/orders`, metadata file `00001` |
| 3 | Polaris returns `metadata-location`, format version 2, one snapshot |
| 4 | Trino counts 2,000,000 rows and sees PyIceberg's snapshot |
| 5 | After Trino's `UPDATE`: 13 data files and 1 position-delete file |
| 6 | After `optimize` on March, PyIceberg reads order 400000 as `CANCELLED` |
| 7 | Trino reads `PAID` at tag `before_fix` and `CANCELLED` now |
| 8 | PyIceberg logs "Commit failed due to a concurrent update, retrying (1/4)". `w2`'s parent is `w1` |
| 9 | Two snapshots remain: `main` and `before_fix` |

To reset everything: `docker compose -f labs/03_catalogs/docker-compose.yml down -v`.

## 8. Exercises

1. **Time travel by time instead of by tag.** Why does `FOR TIMESTAMP AS OF` fail
   after step 9 when the tag still works? (Section 6.)
2. **Turn off the retry.** With `commit.retry.num-retries = 0`, what happens to the
   second writer, and which component rejected it?
3. **Read without the catalog.** Open the current and the `00001` metadata files
   directly. Both work, so what is the catalog protecting you from?
4. **Pick a catalog** for three scenarios, using section 5.

## 9. Exercise answers

Try the exercises before reading this.

1. Expiry removed the expired snapshots and trimmed the snapshot log to entries
   after them. The tagged first snapshot survives, but its history entry is gone,
   and timestamp lookups search the history. Tags are the reliable way to keep a
   point in time.
2. `w2` fails with `CommitFailedException: Requirement failed: branch main has
   changed`. **Polaris** rejected it: PyIceberg sent its commit with the
   requirement "main is still snapshot X", and Polaris refused the swap because
   `w1` had already moved `main`. RustFS played no part. Retrying is the client's
   job, and with retries at 0, PyIceberg gives up.
3. The current file shows every committed row. `00001` shows the table as it was
   after step 2, and it reads as if it were current. Without a catalog, a reader
   has to guess which metadata file is the latest. Listing the folder and taking
   the highest number is the module 1 problem again, and it breaks the moment two
   writers race. The catalog's pointer is the single source of truth.
4. (a) S3 Tables, or Glue plus scheduled jobs if they want more control. (b)
   PyIceberg `SqlCatalog` on SQLite, as in module 2. (c) Polaris or Lakekeeper on
   their existing PostgreSQL, with maintenance scheduled in Trino or Spark.

Next: module 4 covers table design, including the row-group and file-size deep
dive.
