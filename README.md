# Amazon ASINs scraper

Python script for scraping information from Amazon with ASINs and writing it to database.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install requirements.

```bash
pip install -r requirements.txt
```

## Usage

```
asins_scraper.py -k <api_key> -u <db_user_name> -p <db_user_pass> -d <db_name> [-i <csv_file>]
```


For scraping you will need a key from Scraper API https://www.scraperapi.com/

PostgreSQL database on localhost with default port (5432) is used for storing scraped info.

File with name `asins.csv` is used by default if input ASINs CSV file is not provided.

## License
[MIT](https://choosealicense.com/licenses/mit/)