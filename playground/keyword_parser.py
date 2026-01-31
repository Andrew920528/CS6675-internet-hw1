import csv
import re
import os
from pathlib import Path
def parse_nutch_linkdb_to_csv(input_file, output_csv):
    """
    Parses a Nutch linkdb dump and extracts link, from_url, and anchor keywords.
    """
    results = []
    current_target_link = None
    source_pattern = re.compile(r'fromUrl:\s+(\S+)\s+anchor:\s*(.*?)\s*metadata:')

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # Check if this line is a target URL (ends with 'Inlinks:')
            if "Inlinks:" in line:
                current_target_link = line.replace("Inlinks:", "").strip()
                continue
            
            # Check if this line contains source/anchor info
            match = source_pattern.search(line)
            if match and current_target_link:
                from_url = match.group(1)
                anchor_text = match.group(2).strip()
                
                # If anchor is empty, we'll mark it as 'no-anchor' or leave blank
                keywords = anchor_text if anchor_text else ""
                
                results.append({
                    "link": current_target_link,
                    "from_url": from_url,
                    "keywords": keywords
                })

    # Write to CSV
    keys = ["link", "from_url", "keywords"]
    with open(output_csv, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(results)

    print(f"Successfully converted {len(results)} link entries to {output_csv}")

if __name__ == "__main__":
    BASE_DIR = os.path.abspath(os.getcwd())
    p = Path(os.path.join(BASE_DIR, "link_content"))
    segments = [entry.name for entry in p.iterdir() if entry.is_file()]
    # segments = ["part-r-00000"]
    # print(segments)
    for segment in segments:
        extension = segment.split(".")[-1]
        if extension == "crc":
            continue
        print(segment)
        path = os.path.join(BASE_DIR, "link_content", segment)
        output_path = os.path.join(BASE_DIR, "output", f"{segment}_keywords.csv")
        parse_nutch_linkdb_to_csv(path, output_path)
    
