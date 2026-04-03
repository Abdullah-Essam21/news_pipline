import os
import json
import pyarrow as pa
import pyarrow.parquet as pq

# Define the schema exactly as you have it
schema = pa.schema([
    ("article_id", pa.string()),
    ("url", pa.string()),
    ("category", pa.string()),
    ("title", pa.string()),
    ("publish_date", pa.string()),
    ("author", pa.string()),
    ("content", pa.string()),
    ("tags", pa.list_(pa.string())),
    ("media", pa.list_(
        pa.struct([
            ("type", pa.string()),
            ("url", pa.string()),
            ("alt", pa.string()),
            ("caption", pa.string()),
            ("provider", pa.string())
        ])
    ))
])

def normalize_record(r):
    """
    Standardizes Youm7 JSONL records for Parquet/PyArrow.
    Handles variations in media, missing fields, and null values.
    """
    if not r:
        return None

    # 1. Handle Tags (Must be a list of strings for the schema)
    # Even if missing, we return an empty list [] instead of None 
    # because pa.list_(pa.string()) expects a list object.
    raw_tags = r.get("tags")
    if isinstance(raw_tags, list):
        r["tags"] = [str(t).strip() for t in raw_tags if t is not None]
    else:
        r["tags"] = []

    # 2. Handle Media (Renamed/Mapped from 'media' or 'images')
    # We look for 'media' first (new scraper), then 'images' (old scraper)
    raw_media = r.get("media") or r.get("images")
    fixed_media = []
    
    if isinstance(raw_media, list):
        for item in raw_media:
            if not isinstance(item, dict):
                continue
            
            # We map fields to match the pa.struct exactly.
            # Using .get(key) defaults to None if the key is missing.
            fixed_media.append({
                "type":     item.get("type"),     # 'image', 'audio', etc.
                "url":      item.get("url"),      # source link
                "alt":      item.get("alt"),      # None for audio/video
                "caption":  item.get("caption"),  # None for audio/video
                "provider": item.get("provider", "internal") # Default value
            })
    r["media"] = fixed_media

    # 3. Top-Level Field Integrity
    # We ensure every key defined in your pa.schema exists in the dict.
    # If a key is missing, we set it to None (SQL NULL).
    expected_keys = [
        "article_id", 
        "url", 
        "category", 
        "title", 
        "publish_date", 
        "author", 
        "content"
    ]
    
    for key in expected_keys:
        # If the key is missing OR it's an empty string/whitespace, 
        # we convert it to a proper None for better data quality.
        val = r.get(key)
        if val is None or (isinstance(val, str) and not val.strip()):
            r[key] = None
        else:
            r[key] = str(val).strip()

    # 4. Mandatory Field Check (Critical for Data Engineering)
    # If the record has no ID or URL, it's a "ghost" record and should be dropped.
    if not r.get("article_id") or not r.get("url"):
        return None

    # Cleanup: Remove old keys that aren't in the schema to avoid PyArrow errors
    r.pop("images", None) 

    return r

def run_conversion_pipeline(input_path, output_path):
    """The main function called by Airflow"""
    writer = None
    batch = []
    chunk_size = 5000

    with open(input_path, "r", encoding='utf-8-sig') as f:
        for line in f:
            try:
                raw_data = json.loads(line)
                r = normalize_record(raw_data)
                if r: batch.append(r)
            except json.JSONDecodeError:
                continue

            if len(batch) >= chunk_size:
                table = pa.Table.from_pylist(batch, schema=schema)
                if writer is None: writer = pq.ParquetWriter(output_path, schema)
                writer.write_table(table)
                batch = []

        if batch:
            table = pa.Table.from_pylist(batch, schema=schema)
            if writer is None: writer = pq.ParquetWriter(output_path, schema)
            writer.write_table(table)

    if writer: writer.close()