#!/bin/bash

SEGMENTS_DIR="crawl/segments"
OUTPUT_BASE="output"

# Create base output dir if it doesn't exist
mkdir -p "$OUTPUT_BASE"

for SEGMENT in "$SEGMENTS_DIR"/*; do
  # Only process directories
  if [ -d "$SEGMENT" ]; then
    SEG_NAME=$(basename "$SEGMENT")
    OUT_DIR="$OUTPUT_BASE/$SEG_NAME"

    echo "Dumping segment: $SEG_NAME"
    mkdir -p "$OUT_DIR"

    nutch/bin/nutch readseg -dump "$SEGMENT" "$OUT_DIR"
  fi
done