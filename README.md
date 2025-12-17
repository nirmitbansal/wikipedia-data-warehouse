**Overview**

This project implements a large-scale streaming ETL pipeline to process the English Wikipedia XML dump (~24 GB compressed) into warehouse-optimized Parquet files, followed by analytical querying using DuckDB. The pipeline is designed to be memory-safe, scalable, and aligned with real-world data engineering best practices.

**ETL Highlights**

Streaming processing (no full file loaded into memory).
Batch-based writes to avoid memory overflow.
122 Parquet files (optimal for warehouse analytics).
Avoids the small-file problem.
Restart-safe and scalable design.
