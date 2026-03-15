# Loading_Large_Files_To_Fabric_Lakehouse_Super_Fast

This repo demonstrates a production-friendly pattern to bulk-load (lift-and-shift) large volumes of files from an S3 prefix into a Fabric Lakehouse Files folder while preserving the original folder structure (e.g., partition-style directories). The notebook uses Spark’s JVM bridge to call the Hadoop FileSystem API (S3AFileSystem) for efficient recursive listing and byte-for-byte copy, making it suitable for many files and large files during migrations, backfills, or initial lake onboarding.

# What this repo showcases

High-volume ingestion from S3 → OneLake/Lakehouse Files using s3a:// paths and Hadoop FS copy utilities
Recursive copy across a prefix (listFiles(..., recursive=True)) with folder structure preserved
Scalable throughput tuning knobs (connections, retries, paging) for large batch moves
Clear guidance on credential handling (do not hardcode secrets; use secret injection/env vars)
A reusable foundation for post-copy validation and downstream processing (e.g., create tables/views, Delta conversion)


This solution is designed to efficiently move very large volumes of Parquet files from Amazon S3 into Microsoft Fabric Lakehouse Files. Performance is achieved by combining parallelism, optimized S3A configurations, and filesystem‑level copy semantics instead of row‑by‑row ingestion.

1️⃣ Hadoop S3A Connector (Core Enabler)
The solution uses the Hadoop S3A filesystem (s3a://), which enables:

Parallel reads from S3
Streaming, byte‑for‑byte copy (no Spark shuffle, no serialization overhead)
High throughput for large files and many small files

hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

2️⃣ Parallelism & Connection Scaling
Connection Pool Size

hconf.set("fs.s3a.connection.maximum", "200")

Controls how many simultaneous HTTP connections can be opened to S3
Higher values = more parallel file transfers
Especially effective when copying many medium/small Parquet files

✅ Recommendation

Start with 100–200
Increase only if S3 throttling is not observed

3️⃣ Retry & Fault Tolerance Controls
Retry Attempts

hconf.set("fs.s3a.attempts.maximum", "10")
Automatically retries failed S3 operations
Critical for large, long‑running migrations
Helps tolerate transient network or throttling issues

✅ Best practice
Higher retries = better resiliency during bulk data movement.

4️⃣ Pagination Optimization for Large Buckets
Directory Listing Scaling

hconf.set("fs.s3a.paging.maximum", "1000")
Controls how many objects are listed per S3 API call
Important when copying hundreds of thousands or millions of files
Reduces API overhead during recursive listing

✅ Benefit
Fewer API calls → faster metadata enumeration → faster overall copy.

5️⃣ Recursive Copy with Folder Preservation

it = src_fs.listFiles(Path(s3_prefix_path), True)

True enables recursive traversal
All subfolders (e.g., partition directories) are preserved
Ideal for:

Hive‑style partitions
Event‑based or timestamped layouts
Lift‑and‑shift migrations


✅ Outcome
Zero transformation, zero schema assumptions—pure data landing.

6️⃣ Byte‑Level Copy (No Spark Shuffle)

FileUtil.copy(src_fs, Path(src_path), dst_fs, Path(dst_path), False, hconf)
Why this is fast

No DataFrame creation
No repartitioning
No serialization / deserialization
No executor memory amplification

✅ Result
This scales much better than a typical spark.read.parquet → write.parquet approach for raw ingestion.
