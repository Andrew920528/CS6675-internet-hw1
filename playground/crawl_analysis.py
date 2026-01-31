import subprocess
import threading
import time
import datetime
import pandas as pd
import os
import re

# Environment: Java 11
print("Java version being used:")
subprocess.run(["java", "-version"])

# Global States
BASE_DIR = os.path.abspath(os.getcwd())
NUTCH_BIN = os.path.join(BASE_DIR, "nutch/bin/")
URLS_DIR = os.path.join(BASE_DIR, "urls")
CRAWL_DIR = os.path.join(BASE_DIR, "crawl")
ITERATION = 2
STATS_INTERVAL = 5  # seconds
OUTPUT_CSV = os.path.join(BASE_DIR, "output", "crawl_stats.csv")
stop_sampling = False
stats_log = []

def parse_nutch_stats(text):
    """
    Parses the full output of Nutch CrawlDbReader stats.
    Returns a dictionary of counts.
    """
    # Dictionary to store results with default zeros
    stats = {
        "total": 0,
        "unfetched": 0,  # status 1
        "fetched": 0,    # status 2
        "gone": 0,       # status 3
        "redir_temp": 0, # status 4
        "redir_perm": 0  # status 5
    }
    
    try:
        # 1. Parse Total URLs
        total_match = re.search(r'TOTAL urls:\s+(\d+)', text)
        if total_match:
            stats["total"] = int(total_match.group(1))
        
        # 2. Parse Statuses using a more flexible regex
        # This looks for 'status X', skips the label in (), and grabs the number
        patterns = {
            "unfetched": r'status 1 \(db_unfetched\):\s+(\d+)',
            "fetched": r'status 2 \(db_fetched\):\s+(\d+)',
            "gone": r'status 3 \(db_gone\):\s+(\d+)',
            "redir_temp": r'status 4 \(db_redir_temp\):\s+(\d+)',
            "redir_perm": r'status 5 \(db_redir_perm\):\s+(\d+)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                stats[key] = int(match.group(1))
                
    except Exception as e:
        print(f"Error during regex parsing: {e}")
        
    return stats

def get_single_stat_snapshot():
    """Helper to perform a single Nutch readdb call and return data."""
    timestamp = datetime.datetime.now()
    cmd = [os.path.join(NUTCH_BIN, "nutch"), "readdb", f"{CRAWL_DIR}/crawldb", "-stats"]
    
    try:
        # We increase timeout slightly for the final read to ensure it gets through
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
        data = parse_nutch_stats(result.stdout)
        return {
            "timestamp": timestamp.strftime("%H:%M:%S"),
            "pages_fetched": data["fetched"],
            "pages_total": data["total"],
            "gone": data["gone"],
            "redir_temp": data["redir_temp"],
            "redir_perm": data["redir_perm"]
        }
    except Exception as e:
        print(f"Final stat collection attempt failed: {e}")
        return None
    
def sample_crawl_stats():
    global stop_sampling
    while not stop_sampling:
        snapshot = get_single_stat_snapshot()
        if snapshot:
            stats_log.append(snapshot)
            print(f"[{snapshot['timestamp']}] Progress: {snapshot['pages_fetched']}/{snapshot['pages_total']}")
        time.sleep(STATS_INTERVAL)

def run_crawl():
    global stop_sampling
    
    # Using shell=True for the main crawl command is often more stable for Nutch
    crawl_script = os.path.join(NUTCH_BIN, "crawl")
    crawl_cmd = f'{crawl_script} -s {URLS_DIR} {CRAWL_DIR} {ITERATION}'

    start_time = time.time()
    start_dt = datetime.datetime.now()
    print(f"Crawl started at: {start_dt}")

    sampler = threading.Thread(target=sample_crawl_stats)
    sampler.start()

    try:
        # This will block until the crawl finish
        subprocess.run(crawl_cmd, shell=True, check=True)
    except Exception as e:
        print(f"Crawl failed: {e}")
    finally:
        stop_sampling = True
        sampler.join()
        print("Crawl process ended. Capturing final snapshot...")
        final_snapshot = get_single_stat_snapshot()
        if final_snapshot:
            stats_log.append(final_snapshot)
            print(f"Final Count: {final_snapshot['pages_fetched']} pages.")
            
    end_time = time.time()
    duration_sec = end_time - start_time
    print(f"Crawl finished. Total time: {duration_sec:.2f}s")
    
    return start_dt, datetime.datetime.now(), duration_sec

if __name__ == "__main__":
    start_dt, end_dt, duration_sec = run_crawl()

    if stats_log:
        df = pd.DataFrame(stats_log)
        df["crawl_start"] = start_dt
        df["crawl_end"] = end_dt
        df["duration_sec"] = duration_sec
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Successfully saved stats to {OUTPUT_CSV}")
    else:
        print("No stats collected.")