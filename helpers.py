import getopt
import re
import sys

import requests
from bs4 import BeautifulSoup
from scraper_api import ScraperAPIClient


def print_usage_and_exit():
    print("Usage:")
    print(
        "asins_scraper.py -k <api_key> -u <db_user_name> -p <db_user_pass> -d <db_name> [-i <csv_file>]"
    )
    print(
        "File with name 'asins.csv' is used by default if input ASINs CSV file is not provided"
    )
    print(
        "Scraper API https://www.scraperapi.com/ is used for scraping info from products' pages"
    )
    print(
        "PostgreSQL database on localhost with default port is used for storing scraped info"
    )
    sys.exit(1)


def print_error_and_exit(error):
    print(error)
    sys.exit(1)


def get_page_soup(client, url, parsed_checked_asin):
    try:
        product_page = client.get(url)

    except requests.exceptions.ConnectionError:
        print_error_and_exit("Page connection error")

    if "404" in product_page.__repr__():
        print("Product page not found, ASIN:", parsed_checked_asin)
        return None

    return BeautifulSoup(product_page.text, "lxml")


def prepare_text(page_elem, just_strip=True, prepare_num_of_reviews=False):
    if page_elem:

        elem_text = page_elem.text

        if just_strip:
            return elem_text.strip()

        elem_text = elem_text.replace(",", "").replace("+", "")

        if prepare_num_of_reviews:
            return elem_text.split("|")[1].strip().split(" ")[0]

        return elem_text.split(" ")[0].strip()


def check_opts_args(argv):
    # provide default option and argument for input file if it's not provided
    if "-i" not in argv:
        argv.extend(["-i", "asins.csv"])

    try:
        opts, _ = getopt.getopt(argv, "k:u:p:d:i:")

    except getopt.GetoptError:
        print_usage_and_exit()

    if "-k" and "-u" and "-p" and "-d" not in argv:
        print_usage_and_exit()

    if "-" in argv:
        print_usage_and_exit()

    return opts


def check_asins(parsed_asins):
    # check for exact 10 alphanumeric symbols long string
    asins_pattern = re.compile("^[A-Za-z0-9]{10}$")
    checked_asins = []

    for parsed_asin in parsed_asins:
        if asins_pattern.match(parsed_asin):
            checked_asins.append(parsed_asin)

        else:
            print("Not valid ASIN:", parsed_asin)

    return checked_asins


def connect_to_api(api_key):
    try:
        client = ScraperAPIClient(api_key)
        status = client.account()

    except requests.exceptions.ConnectionError:
        print_error_and_exit("Scraper API connection error")

    if "error" in status:
        print_error_and_exit("Scraper API key error")

    return client
