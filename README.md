# Youm7 Big Data ETL Pipeline: From Scrapy to Airflow & Parquet

A production-grade Data Engineering project focused on the large-scale extraction, transformation, and automated orchestration of news data. This pipeline processes over **2,000,000+ articles** from the "seventh day" (اليوم السابع) archive (2013–2026), converting raw web data into an optimized analytical format.

## 📂 Project Directory Structure

The project follows a modular architecture to separate the Scrapy spider logic, Airflow orchestration, and Python transformation scripts. This structure is optimized for **Astro CLI** and Dockerized environments.

```
youm7-data-pipeline/
├── dags/
│   └── youm7_scrape_dag.py       # Airflow DAG (The Orchestrator)
├── include/
│   ├── youm7_scrape/             # Full Scrapy Project
│   │   ├── spiders/
│   │   │   └── archive_spider.py # Main Crawler Logic
│   │   └── settings.py           # Reactor & Concurrency Config
│   └── data/                     # Local Persistent Storage (Docker Volumes)
│       ├── raw/                  # Output: .jsonl (Extracted)
│       └── intermediate/         # Output: .parquet (Transformed)
├── task_scripts/                 # Reusable Business Logic
│   └── jsonl_to_parquet.py       # PyArrow Streaming & Normalization
├── plugins/                      # Airflow Custom Providers/Hooks
├── Dockerfile                    # System-level dependencies
├── packages.txt                  # OS-level packages (e.g., build-essential)
└── requirements.txt              # Python libraries (Scrapy, PyArrow, Polars)
```

### Path Management

To maintain environment-agnostic paths, the pipeline utilizes the `/usr/local/airflow/include` absolute path inside the Docker container. This ensures that the **BashOperator** (running Scrapy) and the **PythonOperator** (running the Transformer) are always looking at the same shared volume.

## 📌 Project Architecture

The project follows a modern ETL (Extract, Transform, Load) pattern designed for scalability and reliability:

1. **Extraction**: High-concurrency web scraping using **Scrapy** and **Asyncio**.
2. **Orchestration**: Workflow automation using **Apache Airflow** (managed via **Astro CLI**).
3. **Transformation**: Data cleaning and normalization using **PyArrow** and **Polars**.
4. **Loading**: Optimized storage in **Apache Parquet** format, partitioned by category and date.

## 🛠️ Technical Stack

- **Orchestration**: Apache Airflow (Astro CLI / Docker)
- **Web Scraping**: Scrapy, Twisted, Asyncio (Proactor Event Loop)
- **Data Processing**: PyArrow, Polars, JSON
- **Storage**: Apache Parquet (Columnar Storage)
- **Environment**: Linux/Docker (via WSL2/Windows)

## 🧩 Engineering Challenges & Solutions

### 1. Overcoming OS-Level Networking Limits

During the initial high-speed crawl (1,200+ pages/min), the pipeline hit a `ValueError: too many file descriptors`. This is a hard-coded limit in the Windows `select()` API.

- **The Solution**: Migrated the Scrapy reactor to `AsyncioSelectorReactor` and implemented the `WindowsProactorEventLoopPolicy`. This allowed the system to bypass the 64-connection limit and handle thousands of concurrent sockets.

### 2. High-Performance ETL with Streaming

Loading 270,000 JSON records into memory at once is inefficient and prone to OOM (Out of Memory) crashes.

- **The Solution**: Developed a **Streaming Parquet Writer** using `PyArrow`. The data is processed in chunks of 5,000 records, normalized on-the-fly, and appended to a columnar Parquet file. This ensures the pipeline can run on standard consumer hardware (16GB RAM) without failure.

### 3. Schema Evolution (2013–2026)

Over 13 years, the source website's HTML structure changed significantly, especially the media containers and article bodies.

- **The Solution**: Created a "Greedy Extraction" logic using complex CSS selectors and XPath to normalize diverse layouts into a single, strict **PyArrow Schema**, ensuring data consistency for downstream BI tools.

## ⛓️ Pipeline Orchestration (Airflow & Astro CLI)

To move beyond manual scripts, the project was migrated to an automated **Apache Airflow** environment managed via **Astro CLI**. This ensures the ETL process is idempotent, scheduled, and monitored.

### DAG Workflow Definition

- **Task 1: `setup_dirs`**: A Python task that ensures the local Docker volumes (`/raw`, `/intermediate`) are initialized before the pipeline starts.
- **Task 2: `run_scrapy_spider`**: A `BashOperator` that triggers the Scrapy engine. It uses Airflow **Jinja Templates** (`{{ ts_nodash }}`) to dynamically name output files based on the execution time.
- **Task 3: `convert_to_parquet`**: A decorated Python task that imports the custom transformation logic. It pulls the filename from the previous task's context, streams the JSONL data, and writes the final Parquet file.

### Data Integrity & Recovery

- **Checkpointing**: Implemented Scrapy’s `JOBDIR` state management, allowing the spider to pause and resume from the last crawled URL in case of network interruptions.
- **Deduplication**: The transformation layer includes an `article_id` uniqueness check to ensure that overlapping crawl windows do not result in duplicate records in the final analytical dataset.

## 📊 Analytical Data Schema

The project utilizes a strict **PyArrow Schema** to ensure the resulting Parquet files are ready for BigLake or BigQuery ingestion:

| Field          | Type           | Description                                                    |
| :------------- | :------------- | :------------------------------------------------------------- |
| `article_id`   | `string`       | Unique news ID from the source                                 |
| `url`          | `string`       | Original article URL                                           |
| `category`     | `string`       | News category (Politics, Art, Economy, etc.)                   |
| `title`        | `string`       | Article headline (Arabic)                                      |
| `publish_date` | `string`       | Scraped timestamp string                                       |
| `author`       | `string`       | Reporter or agency name                                        |
| `content`      | `string`       | Cleaned text body (Whitespace normalized)                      |
| `tags`         | `list<string>` | Array of associated keywords                                   |
| `media`        | `list<struct>` | Nested objects containing `type`, `url`, `alt`, and `provider` |

### Next Steps:

- [ ] **Cloud Ingestion**: Adding a `GCSObjectStorage` task to move Parquet files to Google Cloud Storage.
- [ ] **Data Warehouse**: Automating the load from GCS into **BigQuery** for SQL-based analysis.
- [ ] **BI Layer**: Connecting **Power BI** to the BigQuery dataset to visualize news trends in Egypt over the last 13 years.

```

```
