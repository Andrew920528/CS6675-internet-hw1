# Setup

Please download Nutch from (https://nutch.apache.org/download/), extract the files and name it as "nutch". Make sure your java environment is set to Java 11.

For python, `pip install` missing packages as needed.

# Deliverables

1. `README.md` and executable code (follow the steps below to run the crawler & analysis) You can find the github repo here: (https://github.com/Andrew920528/CS6675-internet-hw1)
2. Screenshots: Please find them in the `screenshots` folder
3. Crawl Statistics: Please find the `crawl_analysis.pdf` in output folder, or manually run `crawl_analysis.py`
4. Reflection and discussion. Please find it in a separate hw submission. A copy of it is saved as `reflection.pdf` in this repo.
5. Seed url: Please find it in `urls/seed.txt`. It is innitialized as (https://www.cc.gatech.edu).

# Procedure

Run `playgorund/crawl_analysis.py`

- This generates screenshots of crawling every 5 seconds, and saves each screen shots to `output/crawl_stats.csv`

Run `nutch/bin/nutch readlinkdb crawl/linkdb/ -dump link_content` in the terminal

- This generates link information into link_content library

Run `playground/keyword_parser.py`

- This parses the keywords for each url into csv files

Run `plotting.ipynb`

- This gets all the output files and create plots and statistcs from them
