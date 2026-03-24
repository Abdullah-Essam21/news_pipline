import subprocess
import os

# List all categories you want to process in this batch
categories_to_run = [  
    'urgent'
]

# Create a copy of the current system environment variables
env = os.environ.copy()
# Tell Python to look in the current directory for modules
env["PYTHONPATH"] = os.getcwd()

# Setup Directories
base_output = "extracted_information"
log_dir = os.path.join(base_output, "logs")

for folder in [base_output, log_dir]:
    if not os.path.exists(folder):
        os.makedirs(folder)
        print(f"Created directory: {folder}")

for cat in categories_to_run:
    print(f"--- Starting: {cat} ---")

    output_path = os.path.join(base_output, f"{cat}.jsonl")
    log_path = os.path.join(log_dir, f"{cat}.log")

    # The command:
    # -a category_key={cat} -> tells the spider which file to use
    # -o {output_path}      -> defines where the JSONL results go
    # --logfile {log_path}  -> defines where the Scrapy logs go
    cmd = [
        "scrapy", "crawl", "jsonl_downloader",
        "-a", f"category_key={cat}",
        "-o", output_path,
        "--logfile", log_path
    ]

    try:
        # Run the spider and wait for it to finish
        result = subprocess.run(cmd, check=True, env=env)
        print(f"DONE: {cat} | Log: {log_path}")
    except subprocess.CalledProcessError:
        print(f"FAILED: {cat} | Check log for details: {log_path}")

print("\nAll batch processes finished.")