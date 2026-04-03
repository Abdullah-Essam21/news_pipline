import polars as pl
import os
## The code worked on all the files except for investigations.parquet, which is the largest file. 
# It caused a memory error.


# # List of your files
# files = [
#     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\investigations.parquet"
# ]
# # files = [
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\arab.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\art.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\caricature.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\economy.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\investigations.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\politics.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\reports.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\television.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\urgent.parquet",
# #     r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\your_horoscope_today.parquet"
# # ]

# def process_file_low_ram(file_path):
#     if not os.path.exists(file_path):
#         return

#     # 1. Scan the file (Lazy)
#     lf = pl.scan_parquet(file_path)
    
#     # 2. Schema check (Fast, no data loaded)
#     if "date_parsed" in lf.collect_schema().names():
#         print(f"Skipping {os.path.basename(file_path)}: already processed.")
#         return

#     # 3. Define the transformations (Same as before)
#     month_map = {"يناير": "01", "فبراير": "02", "مارس": "03", "أبريل": "04", "مايو": "05", "يونيو": "06", "يوليو": "07", "أغسطس": "08", "سبتمبر": "09", "أكتوبر": "10", "نوفمبر": "11", "ديسمبر": "12"}
    
#     clean_expr = pl.col("publish_date").str.replace(r"^.*،\s*", "")
#     for ar, num in month_map.items():
#         clean_expr = clean_expr.str.replace(ar, num)
        
#     clean_expr = clean_expr.str.replace(r"\sص$", " AM").str.replace(r"\sم$", " PM")

#     # 4. STREAMING EXECUTION
#     temp_file = file_path + ".tmp"
    
#     try:
#         # sink_parquet triggers the "Streaming Engine"
#         # It processes data in chunks (e.g., 50,000 rows at a time)
#         (
#             lf.with_columns(
#                 clean_expr
#                 .str.strptime(pl.Datetime, "%d %m %Y %I:%M %p", strict=False)
#                 .alias("date_parsed")
#             )
#             .sink_parquet(temp_file) 
#         )
        
#         # Replace the old file with the new one
#         os.replace(temp_file, file_path)
#         print(f"Done: {os.path.basename(file_path)}")
        
#     except Exception as e:
#         if os.path.exists(temp_file):
#             os.remove(temp_file)
#         print(f"Error on {file_path}: {e}")

# for f in files:
#     process_file_low_ram(f)

#--------------------------------------------------------------------------------------------------------
# Increase the memory overhead limit for the streaming engine
os.environ["POLARS_STREAMING_CHUNK_SIZE"] = "10000" 

files = [
    r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\intermediate\investigations.parquet"
]

def process_file_low_ram(file_path):
    if not os.path.exists(file_path):
        return

    # 1. Lazy Scan
    lf = pl.scan_parquet(file_path)
    
    # 2. Optimized Month Mapping
    # Creating a replacement mapping for Arabic months to digits
    month_map = {
        "يناير": "01", "فبراير": "02", "مارس": "03", "أبريل": "04", 
        "مايو": "05", "يونيو": "06", "يوليو": "07", "أغسطس": "08", 
        "سبتمبر": "09", "أكتوبر": "10", "نوفمبر": "11", "ديسمبر": "12"
    }

    # 3. The Transformation Logic
    # We use .str.replace_many() which is significantly more memory-efficient 
    # than 12 individual .str.replace() calls in a loop.
    processed_lf = (
        lf.with_columns(
            pl.col("publish_date")
            .str.replace(r"^.*،\s*", "", literal=False) # Remove "Day, " prefix
            .str.replace_many(month_map)                # Multi-pattern match
            .str.replace(r"\sص$", " AM", literal=False)
            .str.replace(r"\sم$", " PM", literal=False)
            .str.strptime(pl.Datetime, "%d %m %Y %I:%M %p", strict=False)
            .alias("date_parsed")
        )
    )

    temp_file = file_path + ".tmp"
    
    try:
        # SINK triggers the streaming engine. 
        # We set a small row_group_size to ensure we don't hold too much in RAM.
        processed_lf.sink_parquet(
            temp_file, 
            compression="snappy",
            row_group_size=5000 
        )
        
        os.replace(temp_file, file_path)
        print(f"Success: {os.path.basename(file_path)}")
        
    except Exception as e:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        print(f"Failed on {file_path}: {e}")

for f in files:
    process_file_low_ram(f)