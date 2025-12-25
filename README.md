**📚 Wikipedia Data Warehouse**

An end-to-end data engineering project that processes large Wikipedia XML dumps into an analytics-ready data warehouse using Parquet and DuckDB.

**✨ Why This Project?**

✔ Designed for large-scale data processing
✔ Demonstrates real-world ETL + data warehousing concepts
✔ Optimized for analytics, performance, and memory efficiency
✔ Interview-ready data engineering architecture

**🧠 Project Overview**

This project builds a streaming ETL pipeline that ingests massive Wikipedia XML dumps, transforms them into structured datasets, and stores them in warehouse-optimized Parquet format for analytical querying using DuckDB.

🔹 No full file loading into memory
🔹 Handles millions of records efficiently
🔹 Built with scalability and performance in mind

**🚀 Key Features**

🔹 Streaming XML Processing

    • Processes Wikipedia dumps line-by-line

    • Avoids memory bottlenecks

🔹 Data Warehouse Ready Output

    • Writes data in Parquet (columnar format)

    • Optimized for analytics and BI tools

🔹 High-Performance Analytics

    • Uses DuckDB for fast SQL queries

    • No external database setup required

🔹 Production-Style ETL Design

    • Clear extract → transform → load separation

    • Easy to extend for cloud warehouses (BigQuery, Redshift, Snowflake)

**🏗️ Architecture Flow**

Wikipedia XML Dump
        ↓
Streaming XML Parser
        ↓
Data Transformation
        ↓
Parquet Files (Warehouse Layer)
        ↓
DuckDB Analytics


✔ Memory-efficient
✔ Scalable
✔ Analytics-friendly

## 📁 Project Structure

```text
wikipedia-data-warehouse/
│
├── etl/                     # ETL pipeline logic
│   ├── main.py              # Pipeline entry point
│   └── parser.py            # Streaming XML parser
│
├── data/                    # Output Parquet files
│   └── parquet/             # Warehouse layer
│
├── validate_parquet.py      # Parquet data validation
├── requirements.txt         # Project dependencies
└── README.md                # Project documentation
