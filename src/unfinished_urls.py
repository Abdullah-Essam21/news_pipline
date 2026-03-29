# import json
# from urllib.parse import unquote  # Import the decoder


# # txt & jsonl file paths
# # Configuration
# FILE_URLS_TXT = r"G:\coding\python\web_scraping\Youm7\youm7_scrape\extracted_links\reports_links.txt"
# FILE_SCRAPED_JSONL = r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\raw\reports.jsonl"
# FILE_MISSING_OUT = r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\raw\missing.txt"
# URL_KEY = "url"

# def find_missing_urls():
#     scraped_urls = set()

#     # 1. Load already scraped URLs and DECODE them
#     print(f"Reading scraped data from {FILE_SCRAPED_JSONL}...")
#     try:
#         with open(FILE_SCRAPED_JSONL, "r", encoding="utf-8") as f:
#             for line in f:
#                 line = line.strip()
#                 if line:
#                     try:
#                         data = json.loads(line)
#                         url = data.get(URL_KEY)
#                         if url:
#                             # unquote() turns %D8... back into Arabic characters
#                             decoded_url = unquote(url.strip())
#                             scraped_urls.add(decoded_url)
#                     except json.JSONDecodeError:
#                         continue 
#     except FileNotFoundError:
#         print(f"Warning: {FILE_SCRAPED_JSONL} not found.")

#     # 2. Compare against the TXT list
#     print(f"Comparing against {FILE_URLS_TXT}...")
#     missing_count = 0
    
#     with open(FILE_URLS_TXT, "r", encoding="utf-8") as f_in, \
#          open(FILE_MISSING_OUT, "w", encoding="utf-8") as f_out:
        
#         for line in f_in:
#             url = line.strip()
#             # We also unquote the input URL just in case some are already encoded
#             decoded_input_url = unquote(url)
            
#             if decoded_input_url and decoded_input_url not in scraped_urls:
#                 f_out.write(url + "\n") # Write the original URL format to the missing list
#                 missing_count += 1

#     print("-" * 30)
#     print(f"Success!")
#     print(f"Total scraped (unique): {len(scraped_urls)}")
#     print(f"Total missing: {missing_count}")
#     print(f"Missing URLs saved to: {FILE_MISSING_OUT}")

# if __name__ == "__main__":
#     find_missing_urls()


import json
from urllib.parse import unquote

# Configuration
# Changed from .txt to .jsonl
FILE_URLS_JSONL = r"G:\coding\python\web_scraping\Youm7\youm7_scrape\extracted_links\arab_article_links.jsonl"
FILE_SCRAPED_JSONL = r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\raw\arab.jsonl"
FILE_MISSING_OUT = r"G:\coding\python\web_scraping\Youm7\youm7_scrape\data\raw\missing_arab.jsonl"
URL_KEY = "url"

def find_missing_urls():
    scraped_urls = set()

    # 1. Load already scraped URLs (JSONL) and DECODE them
    print(f"Reading scraped data from {FILE_SCRAPED_JSONL}...")
    try:
        with open(FILE_SCRAPED_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    data = json.loads(line)
                    url = data.get(URL_KEY)
                    if url:
                        # Decode to handle percent-encoded Arabic characters
                        decoded_url = unquote(url.strip())
                        scraped_urls.add(decoded_url)
                except json.JSONDecodeError:
                    continue 
    except FileNotFoundError:
        print(f"Warning: {FILE_SCRAPED_JSONL} not found.")

    # 2. Compare against the JSONL source list
    print(f"Comparing against {FILE_URLS_JSONL}...")
    missing_count = 0
    
    try:
        with open(FILE_URLS_JSONL, "r", encoding="utf-8") as f_in, \
             open(FILE_MISSING_OUT, "w", encoding="utf-8") as f_out:
            
            for line in f_in:
                line = line.strip()
                if not line: continue
                
                try:
                    data = json.loads(line)
                    original_url = data.get(URL_KEY)
                    
                    if original_url:
                        # Normalize for comparison
                        decoded_input_url = unquote(original_url.strip())
                        
                        if decoded_input_url not in scraped_urls:
                            # Write the entire original dictionary back to the missing file
                            f_out.write(json.dumps(data, ensure_ascii=False) + "\n")
                            missing_count += 1
                except json.JSONDecodeError:
                    print("Skip: Malformed JSON line in source file.")
                    continue

        print("-" * 30)
        print(f"Success!")
        print(f"Total scraped (unique): {len(scraped_urls)}")
        print(f"Total missing: {missing_count}")
        print(f"Missing URLs saved to: {FILE_MISSING_OUT}")

    except FileNotFoundError:
        print(f"Error: Source file {FILE_URLS_JSONL} not found.")

if __name__ == "__main__":
    find_missing_urls()