import json
import time
import os
import sys
from datetime import datetime

PROGRESS_FILE = "progress.json"
FAILED_FILE = "failed_downloads.txt"

def clear_screen():
    if sys.stdout.isatty():
        os.system('cls' if os.name == 'nt' else 'clear')

def get_failed_count():
    if not os.path.exists(FAILED_FILE):
        return 0
    try:
        with open(FAILED_FILE, 'r', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0

def get_progress():
    if not os.path.exists(PROGRESS_FILE):
        return None
    try:
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def main():
    print("Waiting for scraper to start...")
    
    try:
        while True:
            data = get_progress()
            
            if data:
                clear_screen()
                last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                page = data.get('current_page', 0)
                row = data.get('current_row', 0)
                total = data.get('total_laws_approx', 0)
                processed = data.get('processed_count', 0)
                skipped = data.get('skipped_repealed', 0)
                left = data.get('files_left_approx', 0)
                
                failed = get_failed_count()
                total_handled = processed + skipped + failed
                
                percent = 0.0
                if total > 0:
                    percent = (total_handled / total) * 100
                
                print(f"=== Law Scraper Status ({last_updated}) ===")
                print("-" * 50)
                print(f"Current Page:      {page}")
                print(f"Current Row:       {row + 1}")
                print(f"Total Laws Found:  {total}")
                print(f"Files Processed:   {processed}")
                print(f"Repealed Skipped:  {skipped}")
                print(f"Failed/Errors:     {failed}")
                print(f"Total Handled:     {total_handled}")
                print(f"Files Remaining:   {max(0, total - total_handled)}")
                print("-" * 50)
                
                # Progress Bar
                bar_len = 40
                filled_len = int(bar_len * percent / 100)
                bar = '█' * filled_len + '-' * (bar_len - filled_len)
                print(f"Progress: |{bar}| {percent:.2f}%")
                print("-" * 50)
                print(f"Resume Point: Page {page}, Row {row + 1}")
                print("Press Ctrl+C to exit monitor")
            else:
                print(".", end="", flush=True)
                
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()
