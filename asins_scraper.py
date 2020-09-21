# -*- coding: utf-8 -*-
import csv
import datetime
import getopt
import os
import re
import sys
from collections import OrderedDict

import requests
from bs4 import BeautifulSoup
from scraper_api import ScraperAPIClient
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.exc import IntegrityError, OperationalError


def print_usage_and_exit():
    print("Usage:")
    print(
        "asins_scraper.py -k <api_key> -u <db_user_name> -p <db_user_pass> -d <db_name> [-i <csv_file>]"
    )
    print(
        "File with name 'asins.csv' is used for parsing ASINs if CSV file is not provided"
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
        print("Product not found, ASIN:", parsed_checked_asin)
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


def get_product_info(client, url, parsed_checked_asin):
    print("Getting product info, ASIN:", parsed_checked_asin)
    product_page = get_page_soup(client, url, parsed_checked_asin)

    product_info = []

    if product_page:
        product_info.append(parsed_checked_asin)

        product = {
            "Product name:": ["productTitle", True],
            "Number of ratings:": [
                "acrCustomerReviewText",
                False,
            ],
            "Average rating:": ["acrPopover", False, "span"],
            "Number of questions:": ["askATFLink", False, "span"],
        }

        for info in product:
            elem_loc = product_page.find(id=f"{product[info][0]}")

            if elem_loc:
                try:
                    elem_loc = elem_loc.findChildren(product[info][2], recursive=False)[
                        0
                    ]

                except IndexError:
                    pass

            product_info.append(
                prepare_text(
                    elem_loc,
                    just_strip=product[info][1],
                )
            )

    return product_info


def get_reviews(client, url, parsed_checked_asin):
    print("Getting reviews, ASIN:", parsed_checked_asin)
    product_reviews_page = get_page_soup(client, url, parsed_checked_asin)

    reviews = []

    try:
        reviews.append(
            prepare_text(
                product_reviews_page.find(id="filter-info-section").div.span,
                just_strip=False,
                prepare_num_of_reviews=True,
            ),
        )

    except AttributeError:
        reviews.append(None)

    top_reviews = ["Top positive review:", "Top critical review:"]

    for top_review_index, _ in enumerate(top_reviews):
        try:
            reviews.append(
                prepare_text(
                    product_reviews_page.find_all(
                        "span", {"class": "a-size-base review-title a-text-bold"}
                    )[top_review_index],
                )
                + "\n"
                + prepare_text(
                    product_reviews_page.find_all(
                        "div", {"class": "a-row a-spacing-top-mini"}
                    )[top_review_index].findChildren("span", recursive=False)[0],
                ),
            )

        except IndexError:
            reviews.append(None)

    return reviews


def scrap_page(client, parsed_checked_asin):
    amazon_base_url = "https://www.amazon.com/"
    amazon_product_url = f"{amazon_base_url}dp/"
    amazon_product_reviews_url = f"{amazon_base_url}product-reviews/"

    scraped_info = []

    product = get_product_info(
        client, f"{amazon_product_url}{parsed_checked_asin}", parsed_checked_asin
    )

    if product:
        scraped_info.extend(product)

        scraped_info.extend(
            get_reviews(
                client,
                f"{amazon_product_reviews_url}{parsed_checked_asin}",
                parsed_checked_asin,
            )
        )

    else:
        return [None] * 8

    # print(scraped_info)
    return scraped_info


def parse_csv(csv_file):
    try:
        with open(csv_file, newline="") as csv_file_handle:
            parsed_list = list(csv.reader(csv_file_handle))
            flatten_list = [item for sub_list in parsed_list for item in sub_list]
            flatten_stripped_list_gen = (item.strip() for item in flatten_list)
            flatten_stripped_list = [item for item in flatten_stripped_list_gen if item]

            uniq_flatten_stripped_list = list(
                OrderedDict.fromkeys(flatten_stripped_list)
            )

            return uniq_flatten_stripped_list

    except OSError:
        print_error_and_exit("Input file read error")


def create_db(db_user_name, db_user_pass, db_name):
    DATABASE_URI = (
        f"postgres+psycopg2://{db_user_name}:{db_user_pass}@localhost:5432/{db_name}"
    )
    engine = create_engine(DATABASE_URI, echo=False)

    meta = MetaData()

    asins = Table(
        "asins",
        meta,
        Column("asin", String, primary_key=True),
    )

    product_info = Table(
        "product_info",
        meta,
        Column(
            "asin",
            String,
            ForeignKey("asins.asin", ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("created_at", DateTime),
        Column("name", String),
        Column("number_of_ratings", Integer),
        Column("average_rating", Float),
        Column("number_of_questions", Integer),
    )

    reviews = Table(
        "reviews",
        meta,
        Column(
            "asin",
            String,
            ForeignKey("asins.asin", ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("number_of_reviews", Integer),
        Column("top_positive_review", String),
        Column("top_critical_review", String),
    )

    meta.create_all(engine)
    db_conn = engine.connect()

    return db_conn, engine, asins, product_info, reviews


def init_db(db_conn, parsed_checked_asin, *tables):
    print("Writing ASIN to database:", parsed_checked_asin)

    for table in tables:
        try:
            db_conn.execute(
                table.insert(),
                [
                    {
                        "asin": parsed_checked_asin,
                    },
                ],
            )

        except IntegrityError:
            pass


def modify_db(
    db_conn,
    asins,
    product_info,
    reviews,
    init_parsed_asin,
    parsed_checked_asin,
    scraped_product_name,
    scraped_number_of_ratings,
    scraped_average_rating,
    scraped_number_of_questions,
    scraped_number_of_reviews,
    scraped_top_positive_review,
    scraped_top_critical_review,
):
    if parsed_checked_asin:
        print("Writing product info to database, ASIN:", parsed_checked_asin)

        db_conn.execute(
            product_info.update()
            .values(
                {
                    "created_at": datetime.datetime.now(),
                    "name": scraped_product_name,
                    "number_of_ratings": scraped_number_of_ratings,
                    "average_rating": scraped_average_rating,
                    "number_of_questions": scraped_number_of_questions,
                }
            )
            .where(product_info.c.asin == parsed_checked_asin)
        )

        print("Writing reviews to database, ASIN:", parsed_checked_asin)

        db_conn.execute(
            reviews.update()
            .values(
                {
                    "number_of_reviews": scraped_number_of_reviews,
                    "top_positive_review": scraped_top_positive_review,
                    "top_critical_review": scraped_top_critical_review,
                }
            )
            .where(reviews.c.asin == parsed_checked_asin)
        )

    else:
        print("Removing from database, ASIN:", init_parsed_asin)
        db_conn.execute(asins.delete().where(asins.c.asin == init_parsed_asin))


def drop_db_tables(engine, *tables):
    for table in tables:
        print("Removing table from database:", table)
        table.drop(engine)


def check_opts_args(argv):
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


def connect_to_api(api_key):
    try:
        client = ScraperAPIClient(api_key)
        status = client.account()

    except requests.exceptions.ConnectionError:
        print_error_and_exit("Scraper API connection error")

    if "error" in status:
        print_error_and_exit("Scraper API key error")

    return client


def regex_check(parsed_asins):
    asins_pattern = re.compile("^[A-Za-z0-9]{10}$")
    checked_asins = []

    for parsed_asin in parsed_asins:
        if asins_pattern.match(parsed_asin):
            checked_asins.append(parsed_asin)

        else:
            print("Not valid ASIN:", parsed_asin)

    return checked_asins


def main(argv):
    abs_path = os.path.abspath(__file__)
    dir_name = os.path.dirname(abs_path)

    for opt, arg in check_opts_args(argv):
        if opt == "-k":
            api_key = arg

        if opt == "-u":
            db_user_name = arg

        if opt == "-p":
            db_user_pass = arg

        if opt == "-d":
            db_name = arg

        if opt == "-i":
            csv_file = os.path.join(dir_name, arg)
            parsed_asins = parse_csv(csv_file)
            parsed_checked_asins = regex_check(parsed_asins)

    client = connect_to_api(api_key)

    try:
        db_conn, engine, asins, product_info, reviews = create_db(
            db_user_name, db_user_pass, db_name
        )

        for parsed_asin in parsed_checked_asins:
            init_parsed_asin = parsed_asin

            init_db(db_conn, parsed_asin, asins, product_info, reviews)
            modify_db(
                db_conn,
                asins,
                product_info,
                reviews,
                init_parsed_asin,
                *scrap_page(client, parsed_asin),
            )

        # drop_db_tables(engine, product_info, reviews, asins)

    except OperationalError:
        print_error_and_exit("Database connection error")


if __name__ == "__main__":
    main(sys.argv[1:])
