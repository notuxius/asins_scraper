import argparse
import re
import sys

import requests
from bs4 import BeautifulSoup
from scraper_api import ScraperAPIClient


def parse_args():
    arg_parser = argparse.ArgumentParser(
        description="Scrap information from Amazon with ASINs and write it to database"
    )

    arg_parser.add_argument(
        "-k",
        action="store",
        dest="api_key",
        required=True,
        type=str,
        help="Scraper API key",
    )

    arg_parser.add_argument(
        "-u",
        action="store",
        dest="db_user_name",
        required=True,
        type=str,
        help="Database user name",
    )

    arg_parser.add_argument(
        "-p",
        action="store",
        dest="db_user_pass",
        required=True,
        type=str,
        help="Database user password",
    )

    arg_parser.add_argument(
        "-d",
        action="store",
        dest="db_name",
        required=True,
        type=str,
        help="Database name",
    )

    arg_parser.add_argument(
        "-i",
        action="store",
        dest="csv_file",
        required=False,
        type=str,
        default="asins.csv",
        help="ASINs CSV file name",
    )

    return arg_parser.parse_args()


def print_error_and_exit(error):
    print(error)
    sys.exit(1)


def get_page_soup(client, url, parsed_checked_asin):
    page_type = ""

    if "/product-reviews/" in url:
        page_type = " reviews"

    print(f"Accessing product{page_type} page, ASIN:", parsed_checked_asin)

    try:
        product_page = client.get(url)

    except requests.exceptions.ConnectionError:
        print_error_and_exit("Page connection error")

    if product_page.status_code != 200 or product_page.is_redirect:
        print("Product page not found, ASIN:", parsed_checked_asin)
        return None

    if "Enter characters you see below" in product_page.text:
        print("CAPTCHA page is displayed, ASIN:", parsed_checked_asin)
        return None

    return BeautifulSoup(product_page.text, "lxml")


def prepare_text(page_elem, just_strip=True):
    if page_elem:

        elem_text = page_elem.text

        if just_strip:
            return elem_text.strip()

        elem_text = elem_text.replace(",", "").replace("+", "")

        num_of_reviews_pattern = re.compile(
            r"[0-9]+ global rating(s)? \| [0-9]+ global review(s)?"
        )

        if num_of_reviews_pattern.match(elem_text.strip()):
            return elem_text.split("|")[1].strip().split(" ")[0]

        return elem_text.split(" ")[0].strip()


def check_asins(parsed_asins):
    # exactly 10 alphanumeric symbols long asin is valid
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
