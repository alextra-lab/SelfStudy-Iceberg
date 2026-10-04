# Module 2: Iceberg basics

Goal: understand what an Iceberg table physically is, how a commit works, and how
Iceberg fixes the three module 1 failures (no atomic commit, no consistent reads,
no schema authority). Lab: `labs/02_iceberg_basics/lab.py`.

## 1. What Iceberg is (and is not)

Iceberg is a **table format specification**: rules for which files describe a
table and how to change them safely. It is not a server, a database or a storage
engine. There is nothing to install or babysit. Libraries implement the spec (Java,
Python, Rust, Go), and engines (Trino/Starburst, Spark, Flink, DuckDB, Snowflake and
others) use those libraries to read and write the same tables.

Oracle analogy: imagine Oracle published the exact on-disk format of its data
dictionary, control file and undo, so any vendor's SQL engine could safely read and
write your tables. That is what Iceberg does for files on object storage.

## 2. The metadata tree

Everything is files next to the data, plus one pointer in a catalog:

```
 catalog                    "sales.orders → .../metadata/00002-....metadata.json"
    │
    ▼
 metadata.json              schemas, partition specs, sort orders, properties,
    │                       list of snapshots, current-snapshot-id
    ▼
 manifest list (snap-*.avro)    one per snapshot: which manifests make up this version,
    │                           with the partition value range in each manifest
    ▼
 manifests (*-m0.avro)      one row per data file: path, partition value, row count,
    │                       per-column min/max and null counts
    ▼
 data files (*.parquet)     immutable, never updated in place
```

A query planner walks this tree top-down and prunes at every level: the manifest
list's partition ranges let it skip whole manifests, and the manifest's per-file
min/max lets it skip data files. Only then does it open Parquet files, where module
1's row group min/max skips more. Three levels of storage-index-style pruning, and
none of them needs a directory listing.

| Iceberg | Closest Oracle idea |
|---|---|
| Catalog entry | Data dictionary entry that says which segment is the table |
| metadata.json | The table's dictionary definition (columns, partitioning) plus its version history |
| Snapshot | The table as of one SCN |
| Manifest list | Segment header / extent map for that version |
| Manifest | Extent map entries, with a storage index for each extent |
| Data file | Extents of HCC compression units, never updated in place |

In lab step 3 you read each level: `metadata.json` with DuckDB's `read_json`, then
the manifest list and the manifests through PyIceberg's metadata tables.

## 3. Snapshots and time travel

Every commit produces a new **snapshot**: a complete, immutable version of the table.
The snapshot ID is Iceberg's SCN (module 1, section 5). A reader pins one snapshot
when it plans a query and reads only that snapshot's files, so writers never
disturb it.

Old snapshots stay until you expire them, so you can query the past:

| Oracle | Iceberg (Trino SQL) | Iceberg (PyIceberg) |
|---|---|---|
| `SELECT ... AS OF SCN n` | `SELECT ... FROM orders FOR VERSION AS OF n` | `orders.scan(snapshot_id=n)` |
| `SELECT ... AS OF TIMESTAMP t` | `SELECT ... FROM orders FOR TIMESTAMP AS OF t` | look up the snapshot current at `t`, then scan it |

Keeping old snapshots keeps their data files on disk, the way undo keeps old block
images. In lab step 7 there are 13 data files for 12 months: the March file replaced
in step 5 is still referenced by the first snapshot. Expiring snapshots and deleting
unreferenced files is maintenance (module 6).

## 4. How a commit works

A writer never edits a file. To commit, it:

1. Writes new data files (Parquet).
2. Writes a new manifest listing them, and a new manifest list for the new snapshot.
3. Writes a new `metadata.json` that adds the snapshot and makes it current.
4. Asks the catalog to swap the table's pointer from the metadata file it started
   from to the new one. This is a **compare-and-swap**: "set it to `00003` only if it
   is still `00002`".

Step 4 is the commit. Until it succeeds, nobody can see any of the new files.
After it succeeds, everybody planning a new query sees all of them. That is
atomicity with no locks held during the write.

If another writer committed first, the swap fails because the pointer is no longer
`00002`. The losing writer reloads the new metadata, checks whether the other commit
conflicts with its own change, and if not, writes a fresh manifest list and
`metadata.json` on top and tries the swap again. Its data files are reused, not
rewritten. This is **optimistic concurrency**: assume no conflict, detect it at
commit time, retry. Oracle is **pessimistic**: a session locks the rows it changes
and other sessions wait.

In lab step 5, `overwrite` (delete March, add new March) produced exactly one new
metadata file, `00002`. PyIceberg records two snapshots inside it, a `delete` and an
`append`, but since both arrived in one pointer swap, no reader ever had the
half-done `delete` state as current.

## 5. Hidden partitioning and partition evolution

An Iceberg partition spec is a list of **transforms** on real columns:

| Transform | Example | Partition value |
|---|---|---|
| `identity` | `region` | the value itself |
| `year` / `month` / `day` / `hour` | `month(order_ts)` | months since 1970-01 (660 = 2025-01) |
| `bucket(N)` | `bucket(16, customer_id)` | hash mod N, to spread high-cardinality keys |
| `truncate(W)` | `truncate(4, sku)` | first W characters, or values rounded down to W |

Queries filter on the **real column** (`order_ts`), and Iceberg applies the
transform to prune. Lab step 4 runs module 1's exercise 1 filter and plans **1 of
12** files instead of 12 of 12. That is how Oracle partition pruning always worked,
and what Hive-style folders could not do.

The spec is versioned. Changing it (lab exercise 2: month to day) affects only new
writes. Old files keep their old spec, and the planner prunes each file using the
spec it was written with. No rewrite of existing data, unlike module 1's Hive layout.

## 6. Schema evolution

Every column has a permanent **field ID** (lab step 1 sets them: `order_id` is 1,
`order_ts` is 2, and so on). Data files store field IDs, not just names, and readers
match columns by ID. So these are all metadata-only commits:

- Add a column (old files return NULL for it, lab step 7)
- Drop a column
- Rename a column (the ID does not change, so old files still match)
- Reorder columns
- Widen a type: `int` to `long`, `float` to `double`, larger decimal precision

Module 1's step 5c failure (a new column in some files broke readers) cannot happen,
because the schema lives in `metadata.json`, not in whatever file a reader opens
first. Oracle analogy: `ALTER TABLE ... ADD` and `RENAME COLUMN` change only the
data dictionary, not the blocks.

## 7. Format versions

The `format-version` field in `metadata.json` (lab step 3a shows `2`):

| Version | Adds |
|---|---|
| v1 | Analytic tables: append and overwrite whole files |
| v2 | Row-level deletes through delete files, so an `UPDATE` or `DELETE` can avoid rewriting whole data files (merge-on-read) |
| v3 | Deletion vectors, `variant` type for semi-structured data, row lineage, column default values, nanosecond timestamps, geospatial types |

Which version to choose, and copy-on-write vs merge-on-read for updates, is part of
table design (module 4).

## 8. The same operations in SQL (preview of Trino/Starburst)

The lab uses PyIceberg because it runs without any server. In Trino the same steps
are plain SQL:

```sql
CREATE TABLE iceberg.sales.orders (
    order_id    BIGINT,
    order_ts    TIMESTAMP(6),
    customer_id INTEGER,
    region      VARCHAR,
    amount      DOUBLE,
    status      VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['month(order_ts)']);

SELECT * FROM iceberg.sales."orders$snapshots";     -- lab step 5 history
SELECT * FROM iceberg.sales."orders$manifests";     -- lab step 3b
SELECT * FROM iceberg.sales."orders$files";         -- lab step 3c
SELECT * FROM iceberg.sales.orders FOR VERSION AS OF 1234567890;   -- lab step 6
ALTER TABLE iceberg.sales.orders ADD COLUMN channel VARCHAR;       -- lab step 7
```

You will run these against a real catalog in module 3.

## 9. Run the lab

```bash
uv sync
uv run marimo edit labs/02_iceberg_basics/lab.py
```

What you should see:

| Step | Result |
|---|---|
| 1 | After `CREATE TABLE`, one file: `metadata/00000-*.metadata.json` |
| 2 | 12 data files in `order_ts_month=...` folders, plus a manifest list and a manifest. The catalog pointer moves from `00000` to `00001` |
| 3c | One manifest row per data file, with `order_id` min/max per file |
| 4 | Filter on `order_ts` for March plans 1 of 12 files |
| 5 | Pinned reader and current reader both count 2,000,000. One new metadata file, `00002` |
| 6 | Order 400000 is `PAID` at the old snapshot and `CANCELLED` now |
| 7 | `add_column` rewrites no data files; existing rows have `channel` = NULL |

The last cell optionally reads the table with DuckDB's own `iceberg` extension. It
downloads the extension on first use, so it needs internet access.

## 10. Exercises

Each exercise has a cell at the bottom of the notebook.

1. **File skipping with manifest stats.** `order_id < 100000` and `amount < 1` are
   not partition filters. How many files does each plan, and why? Compare with
   module 1 step 4b, which made the same check on row groups inside one file.
2. **Partition evolution.** Switch the spec from month to day and append one day.
   Where do old and new files live, and do both kinds still prune?
3. **Time travel.** Count the rows at each snapshot. What does the `delete` snapshot
   show, and why did no reader ever see it?
4. **Two writers at once.** Two writers both start from metadata file `00002`. Using
   section 4, what happens to each? Compare with two Oracle sessions updating the
   same row.

Next: module 3 chooses a production catalog and runs Trino against it, without Hadoop.
