# Module 1: Modern data lake basics

Goal: understand the five layers of a modern lakehouse, and see with your own eyes
why a folder of Parquet files is not yet a table. Lab: `labs/01_data_lake/lab.py`.

## 1. From Exadata to the lakehouse in one picture

Exadata already split the database in two: **database servers** do SQL, **storage
cells** hold the data and do some filtering close to the disk (Smart Scan). A lakehouse
takes that split all the way and makes every layer an independent, swappable piece:

```
 ┌──────────────────────────────────────────────────────────────┐
 │ 5. Engines      Trino/Starburst, DuckDB, Spark, Flink, Snowflake│  ← "database servers", many vendors
 ├──────────────────────────────────────────────────────────────┤
 │ 4. Catalog      REST catalog, Glue, Unity, Polaris, Nessie     │  ← the data dictionary
 ├──────────────────────────────────────────────────────────────┤
 │ 3. Table format Iceberg (or Delta, Hudi, Paimon, DuckLake)     │  ← redo/undo + control file + segment map
 ├──────────────────────────────────────────────────────────────┤
 │ 2. File format  Parquet (ORC, Avro)                             │  ← the block format (think HCC)
 ├──────────────────────────────────────────────────────────────┤
 │ 1. Storage      S3, GCS, ADLS, MinIO                            │  ← "storage cells", but dumb and cheap
 └──────────────────────────────────────────────────────────────┘
```

The key difference from Exadata: in Oracle all five layers are one product with one
owner. In a lakehouse they are open specs, so the same table can be written by Spark
and read by Trino and DuckDB at the same time, and nobody owns your data but you.
The price is that you now have to understand each layer.

## 2. Layer 1: object storage

S3-style storage is not a filesystem. Know these properties because every design
decision above it follows from them:

| Property | Consequence |
|---|---|
| Objects are immutable: you write a whole object or nothing | Data files are never updated in place. Changes = new files. |
| No real directories, just key prefixes | "Listing a directory" is a paged API call, slow at scale. Table formats avoid listing. |
| No rename; copy + delete is not atomic | You can't do the classic "write to temp, rename into place" commit. |
| Cheap per GB, but a cost and latency per request | Many small files hurt (cost and speed). Target files of ~128 MB to 512 MB. |
| Throughput scales out with parallel requests | Engines read many files and byte ranges in parallel. |

Elasticsearch anchor: Lucene segments are also write-once. An update in ES is
"mark old doc deleted + write new doc in a new segment", and background **segment
merging** cleans up. Iceberg works the same way: new files + delete markers, and
**compaction** (module 6) is your segment merge.

This is also why Hadoop/HDFS is no longer needed: object storage replaced HDFS as
the storage layer, and the table format replaced the Hive Metastore's directory
conventions. No NameNode to babysit.

## 3. Layer 2: Parquet

Parquet is a columnar file format. A file is laid out as:

```
 file
 ├── row group 0 (e.g. ~122k rows in DuckDB, often 128 MB in Spark)
 │    ├── column chunk: order_id   → pages (dictionary / RLE / bit-packed, then zstd)
 │    ├── column chunk: order_ts
 │    └── ...
 ├── row group 1 ...
 └── footer: schema + per row group, per column: min, max, null count, offsets
```

Oracle mapping:

| Parquet | Oracle / Exadata |
|---|---|
| Column chunks + encoding + zstd | Hybrid Columnar Compression (HCC) compression units |
| Reading only the columns you SELECT (projection) | Smart Scan column projection |
| Footer min/max per row group | **Storage indexes** (per 1 MB region min/max in cell memory), or zone maps |
| Row group skipping by min/max | Storage index I/O elimination |

The big lesson from storage indexes carries over directly: **min/max skipping only
works if the data is clustered on the column you filter on.** In the lab, `order_id`
is sorted so a filter skips 16 of 17 row groups; `amount` is random so nothing is
skipped. That is why Iceberg tables have a **sort order** (module 4).

Oracle has exactly this pairing: **attribute clustering** (`CLUSTERING BY LINEAR ORDER`)
plus **zone maps**. A zone map only prunes if rows were loaded clustered on the
column you filter by. And you can only sort one way at a time: in exercise 2,
re-sorting by `amount` makes `amount < 1` read 1 of 17 row groups, but `order_id <
100000` drops to 17 of 17. Choose the sort order from your most common `WHERE`
clauses. (Iceberg also offers z-ordering, which clusters on several columns at
once at some cost to each one. Module 4.)

### Row group size: the HCC trade-off

A row group is a horizontal slice of rows, stored column by column inside, with
one min/max per column. The closest Exadata match is an **HCC compression unit**,
and its min/max plays the role of a storage index region. Choosing the size is
the same trade-off as HCC **Query Low** vs **Archive High**: bigger units compress
better but cost more work per lookup.

Exercise 3 on the lab's 2M rows, filtering `order_id < 100000`:

| Row group size | Row groups | File size | Row groups read | Rows read to find 100,000 |
|---|---|---|---|---|
| 10,000 | 196 | 21.6 MB | 10 | 100,000 |
| 50,000 | 40 | 21.5 MB | 2 | 100,000 |
| 122,880 (DuckDB default) | 17 | 21.4 MB | 1 | 122,880 |
| 500,000 | 4 | 20.8 MB | 1 | 500,000 |
| 1,000,000 | 2 | 19.4 MB | 1 | 1,000,000 |

- **Bigger row groups:** better compression, less footer metadata, fewer and
  larger reads. But skipping gets coarse: at 1,000,000 the engine reads 10 times
  more rows than it needs.
- **Smaller row groups:** precise skipping, but more metadata, more small reads,
  and slightly worse compression. On object storage, where every request has a
  cost and latency, too many small reads hurts.

This choice has a big impact on query performance, so it gets a full deep dive
with measured query timings in module 4.

**In Iceberg it is set per table**, as table properties stored in the table's
metadata, so every writer sees the same settings:

| Property | Default | Controls |
|---|---|---|
| `write.parquet.row-group-size-bytes` | 128 MB | Row group size |
| `write.target-file-size-bytes` | 512 MB | Data file size |
| `write.parquet.compression-codec` | zstd | Compression |

You set them in `CREATE TABLE` or change them with `ALTER TABLE ... SET PROPERTIES`.
As with HCC, a change applies only to **new writes**. Existing files keep their
layout until compaction rewrites them, just as `ALTER TABLE ... COMPRESS FOR` needs
an `ALTER TABLE ... MOVE` to recompress old data. One caveat: honoring the
properties is up to the engine doing the writing. Spark and PyIceberg follow
them, and Trino has some writer settings of its own. Module 4 tests which ones it
obeys.

## 4. Partitioning, the old way (Hive-style)

The classic data lake trick is one directory per partition value:

```
orders_by_month/month=2025-01/data_0.parquet
orders_by_month/month=2025-02/data_0.parquet
```

An engine that sees `WHERE month = '2025-03'` reads one directory. Oracle mapping:
list/range partitioning with partition pruning. But Hive-style partitioning has
well-known problems:

- The partition column is a **physical column you must filter on**. Filter on
  `order_ts` instead of `month` and you get no pruning. (Oracle would prune on the
  real column; so does Iceberg, through *hidden partitioning*, module 2.)
- Changing the scheme (month → day) means rewriting the whole table.
- Engines discover the table by **listing directories**, which is slow on S3.
- The schema lives in **directory names**. A typo or a renamed folder silently
  changes your data.

### Why the `=` in `month=2025-12`?

Normally you avoid special characters in names, and that rule still holds for
everything you name yourself (tables, columns, buckets, files). The `=` here is a
deliberate convention: the name carries data. With `hive_partitioning = true` the
engine parses `key=value` out of each path to rebuild the `month` column and prune
on it. Name the folder plain `2025-12` and DuckDB, Trino and Spark no longer know
which column that value belongs to.

Iceberg keeps the `key=value` folder names by default, but only so humans can read
them. Readers never parse the paths, because partition values are stored in the
manifests. You could flatten or rename the folders and Iceberg would not notice. The
`write.object-storage.enabled` table property goes further and puts hashed prefixes in
front of the paths to spread load across S3 partitions (module 4).

Rule of thumb: plain names for anything you type, and treat `key=value` paths as
tool-generated layout you never type by hand.

## 5. Layer 3: why a table format exists

The lab's step 5 shows three failures of "a table = whatever files are in this folder":

1. **Updates rewrite files.** Changing one value rewrote 3.5 MB.
2. **No atomic commit.** A reader that listed files between "add new file" and
   "delete old files" counted 2,178,560 rows in a 2,000,000 row table. No error.
   In Oracle terms: no read consistency, no SCN, no undo.
3. **No schema authority.** A writer added a column to some files. Readers either
   fail or must know to pass `union_by_name`. There is no data dictionary.

A table format fixes all three by keeping **metadata files next to the data** that
say exactly which data files make up the table at each point in time:

| Problem | Iceberg's answer | Oracle analogy |
|---|---|---|
| Which files are in the table? | Manifests list data files explicitly; no directory listing | Segment / extent map |
| Atomic changes | Each commit writes a new metadata file and swaps one pointer in the catalog (compare-and-swap) | Commit at an SCN |
| Consistent reads | A reader pins one snapshot and sees only its files | Read consistency via undo |
| History | Old snapshots remain until expired: time travel | Flashback query |
| Schema | Schema with column IDs in metadata; rename/add/drop safe | Data dictionary |
| Pruning without listing | Partition values and column min/max stored in manifests | Partition pruning + storage indexes, but planned before any data file is opened |

### Read consistency: SCNs and snapshots

The rule the step 5b reader broke is the one every RDBMS enforces: **a query must
never see into an operation that is still running. It sees only what was committed
as of the moment the query started.** A commit that lands while the query is
still running is invisible to it too.

Oracle enforces this with the **SCN (System Change Number)**, its internal logical
clock, a counter that increases with every commit. When a query starts, Oracle notes
the current SCN, and every block is read as of that SCN. If a block has changed
since, Oracle uses undo to rebuild the older version. Flashback query
(`SELECT ... AS OF SCN n`) is the same mechanism pointed at the past.

Iceberg's equivalent is the **snapshot**. Every commit creates a new snapshot with
its own ID. A reader pins one snapshot when it plans the query and reads only that
snapshot's files, so a writer adding and removing files mid-query changes nothing
for it. `FOR VERSION AS OF <snapshot_id>` is Iceberg's flashback query (module 2).

| Oracle | Iceberg |
|---|---|
| SCN | Snapshot ID |
| COMMIT | Atomic swap of the catalog's metadata pointer |
| Undo for consistent reads | Old data files kept until their snapshots are expired |
| `AS OF SCN` / `AS OF TIMESTAMP` | `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` |

## 6. Layer 4: the catalog

The table format needs one place that says "the current metadata file for
`sales.orders` is `s3://.../00042-....metadata.json`". Updating that pointer
atomically *is* the commit. That place is the **catalog**. Elasticsearch anchor:
it plays the role of an **index alias** that you flip atomically to a new index.

Catalogs range from a SQLite file (fine for learning, used in module 2) to REST
catalogs that also handle auth and credential vending (module 3).

## 7. Layer 5: engines

Because layers 1 to 4 are open, you pick engines per job:

- **DuckDB**: in-process, single node, superb for dev, tests, and medium data.
  Think "SQL*Plus with a vectorized columnar engine inside".
- **Trino / Starburst**: distributed MPP SQL, the closest thing to Exadata's
  parallel query across many nodes. Starburst is the commercial Trino distribution.
- **Spark / Flink**: heavy ETL and streaming writers.

## 8. Run the lab

The lab is a [marimo](https://marimo.io) notebook: a reactive notebook saved as a
plain `.py` file. Edit a cell and everything that depends on it reruns.

```bash
uv sync
uv run marimo edit labs/01_data_lake/lab.py   # opens the notebook in your browser
uv run python labs/01_data_lake/lab.py        # or run it headless as a script
```

What you should see (numbers from a reference run):

| Step | Result |
|---|---|
| 2 | CSV 98 MB vs Parquet 21 MB for the same 2M rows |
| 2 | Footer: each row group of `order_ts` covers about three weeks |
| 4a | `Scanning Files: 1/12` for `WHERE month = '2025-03'` |
| 4b | `order_id < 100000` reads 1 of 17 row groups; `amount < 1` reads 17 of 17 |
| 5b | rows before=2,000,000, during=2,178,560, after=2,000,000 |
| 5c | reading the new column fails unless the reader passes `union_by_name` |

## 9. Exercises

Each exercise has a ready-made cell at the bottom of the notebook. Edit it there.
Step 5 damages only a copy of the lake, so the exercises always see clean data.

1. In step 4a, filter on `order_ts BETWEEN '2025-03-01' AND '2025-03-31'` instead of
   `month = '2025-03'`. How many files are scanned now? Why? (This is the problem
   hidden partitioning solves.)
2. Rewrite the step 2 file with one change, `ORDER BY amount`, and run the exact
   step 4b check on it. The notebook shows both results side by side for
   `order_id < 100000` and `amount < 1`. Which filter can skip row groups now, and
   what does that tell you about choosing a sort order?
3. Drag the `ROW_GROUP_SIZE` slider from 10,000 to 1,000,000. How do file size and
   skipping change? Which Exadata trade-off does this remind you of?
4. Explain in two sentences, in Oracle terms, what the reader in step 5b was missing.

## 10. Exercise answers

1. **12 of 12 files.** `month` exists only in folder names, so the engine cannot tell
   that March `order_ts` values live in `month=2025-03` and must open every file.
   Iceberg's hidden partitioning fixes this (module 2).
2. **Sorting moves the benefit.** Sorted by `amount`, `amount < 1` reads 1 of 17 row
   groups and `order_id < 100000` reads 17 of 17, the reverse of step 4b. See
   attribute clustering and zone maps in section 3.
3. **It is the HCC compression unit trade-off.** See "Row group size" in section 3.
4. **Read consistency and an atomic commit.** The reader had no SCN to pin and the
   writer had no commit, so the reader saw half old files and half new ones. See
   "Read consistency" in section 5.

Next: module 2 builds the same table as Iceberg and opens the metadata files to
see exactly how it fixes step 5.
