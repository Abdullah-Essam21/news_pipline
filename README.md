# Youm7 Archive ETL Pipeline (2013–2026)

A high-performance Data Engineering project focused on the large-scale extraction, transformation, and loading (ETL) of over **2,000,000 articles** from the "Seventh Day
" (اليوم السابع) news archive. This project demonstrates advanced web scraping techniques, multi-modal data handling, and OS-level performance optimization.

## 🚀 Key Features

- **Scalable Extraction**: Engineered a Scrapy-based spider capable of processing 1,200+ pages per minute.
- **Multi-Modal Data Support**: Dynamically extracts text, images, YouTube embeds, and internal MP3 audio files into a unified schema.
- **Resilient Parsing**: Implemented "greedy" text selection to maintain 100% data integrity across 13 years of evolving HTML structures (Politics, Economy, Horoscopes, etc.).
- **Optimized Storage**: Designed a columnar storage workflow converting raw JSONL output into **Apache Parquet** for high-speed analytical queries.
- **Windows Performance Tuning**: Bypassed the 64-file-descriptor limit in Windows by implementing the **Asyncio Proactor** event loop policy.

## 🛠️ Tech Stack

- **Framework**: Scrapy
- **Data Processing**: Python, Pandas, PyArrow
- **Storage Format**: JSONL, Apache Parquet
- **Networking**: Twisted, Asyncio (Proactor)

## 📊 Data Schema

The project utilizes a structured PyArrow schema to handle nested media metadata efficiently:

| Field | Type | Description |
| :--- | :--- | :--- |
| `article_id` | String | Unique identifier from the source |
| `title` | String | Arabic headline of the article |
| `content` | String | Full cleaned text body |
| `tags` | List | Associated keywords |
| `media` | List(Struct) | Unified list of images, videos, and audio with provider metadata |

## ⚙️ Challenges & Solutions

### The Windows "Select" Bottleneck
During the crawl of 270,000 links, the system hit a `ValueError: too many file descriptors`. This was resolved by switching the Scrapy reactor to `AsyncioSelectorReactor` and forcing the `WindowsProactorEventLoopPolicy`, allowing the spider to handle hundreds of concurrent sockets simultaneously.

### Handling Inconsistent Layouts
Over 13 years, the website layout shifted multiple times. I developed a flexible parsing logic that targets the core `#divcont` and `#articleBody` containers, ensuring successful extraction even when data was nested in non-standard `<span>` or `<div>` tags.

## 📈 Future Roadmap
- [ ] Implement an **Apache Airflow** DAG for daily automated updates.
- [ ] Build a **Power BI** dashboard to analyze economic and social trends in Egypt over the last decade.
- [ ] Deploy a sentiment analysis model to categorize news tone.
