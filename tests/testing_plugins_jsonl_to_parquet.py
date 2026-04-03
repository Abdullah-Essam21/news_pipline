from plugins.jsonl_to_parquet import run_conversion_pipeline
import os

# Use your local Windows paths for this quick test
input_file = r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\raw\new_urgent.jsonl"
output_file = r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\raw\new_urgent.parquet"

if __name__ == "__main__":
    print("Starting local test...")
    run_conversion_pipeline(input_file, output_file)
    if os.path.exists(output_file):
        print(f"Success! Parquet file created at {output_file}")
    else:
        print("Failed: File was not created.")