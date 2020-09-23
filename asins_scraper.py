# -*- coding: utf-8 -*-
import csv
import datetime
import os
import sys
from collections import OrderedDict
import argparse

from sqlalchemy.exc import IntegrityError, OperationalError

from helpers import (
    check_asins,
    connect_to_api,
    get_page_soup,
    prepare_text,
    print_error_and_exit,
)
from helpers_db import create_db


def get_product_info(client, url, parsed_checked_asin):
    product_page = get_page_soup(client, url, parsed_checked_asin)

    product_info = []

    if product_page:
        product_info.append(parsed_checked_asin)

        # page elements locators for scraping
        product = {
            "product name": ["productTitle", True],
            "number of ratings": [
                "acrCustomerReviewText",
                False,
            ],
            "average rating": ["acrPopover", False, "span"],
            "number of questions": ["askATFLink", False, "span"],
        }

        for info in product:
            print(f"Getting {info}, ASIN:", parsed_checked_asin)
            # scraping product info by elements ids
            elem_loc = product_page.find(id=product[info][0])

            if elem_loc:
                # TODO refactor into one locator
                try:
                    # more accurate locator for elements that require it
                    elem_loc = elem_loc.findChildren(product[info][2], recursive=False)[
                        0
                    ]

                # if it's not product name or number or ratings
                # or if there are no product questions or ratings
                except IndexError:
                    pass

                product_info.append(
                    prepare_text(
                        elem_loc,
                        # just strip info or not
                        just_strip=product[info][1],
                    )
                )

            else:
                product_info.append(None)

    return product_info


def get_reviews(client, url, parsed_checked_asin):
    product_reviews_page = get_page_soup(client, url, parsed_checked_asin)

    reviews = []

    # scraping number of reviews
    print("Getting number of reviews, ASIN:", parsed_checked_asin)
    try:
        reviews.append(
            prepare_text(
                product_reviews_page.find(id="filter-info-section").div.span,
                just_strip=False,
                prepare_num_of_reviews=True,
            ),
        )

    # if there are no product number of reviews
    except AttributeError:
        reviews.append(None)

    top_reviews = ["top positive review", "top critical review"]

    for top_review_index, top_review in enumerate(top_reviews):
        try:
            print(f"Getting {top_review}, ASIN:", parsed_checked_asin)
            # scraping top reviews headings
            reviews.append(
                prepare_text(
                    product_reviews_page.find_all(
                        "span", {"class": "a-size-base review-title a-text-bold"}
                    )[top_review_index],
                )
                + "\n"
                # scraping top reviews text
                + prepare_text(
                    product_reviews_page.find_all(
                        "div", {"class": "a-row a-spacing-top-mini"}
                    )[top_review_index].findChildren("span", recursive=False)[0],
                ),
            )

        except (IndexError, AttributeError):
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
        # TODO use keyword args for modify_db if product was not found
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

        # if asin is already in db
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
                    "number_of_ratings": scraped_number_of_ratings
                    or product_info.c.number_of_ratings.default.arg,
                    "average_rating": scraped_average_rating
                    or product_info.c.average_rating.default.arg,
                    "number_of_questions": scraped_number_of_questions
                    or product_info.c.number_of_questions.default.arg,
                }
            )
            .where(product_info.c.asin == parsed_checked_asin)
        )

        print("Writing reviews to database, ASIN:", parsed_checked_asin)

        db_conn.execute(
            reviews.update()
            .values(
                {
                    "number_of_reviews": scraped_number_of_reviews
                    or reviews.c.number_of_reviews.default.arg,
                    "top_positive_review": scraped_top_positive_review
                    or reviews.c.top_positive_review.default.arg,
                    "top_critical_review": scraped_top_critical_review
                    or reviews.c.top_critical_review.default.arg,
                }
            )
            .where(reviews.c.asin == parsed_checked_asin)
        )

    else:
        print("Removing from database, ASIN:", init_parsed_asin)
        db_conn.execute(asins.delete().where(asins.c.asin == init_parsed_asin))


def erase_db_tables(engine, *tables):
    for table in tables:
        print("Removing table from database:", table)
        table.drop(engine)


def main():
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

    parsed_args = arg_parser.parse_args()

    abs_path = os.path.abspath(__file__)
    dir_name = os.path.dirname(abs_path)

    api_key = parsed_args.api_key
    db_user_name = parsed_args.db_user_name
    db_user_pass = parsed_args.db_user_pass
    db_name = parsed_args.db_name
    csv_file = os.path.join(dir_name, parsed_args.csv_file)

    parsed_asins = parse_csv(csv_file)
    parsed_checked_asins = check_asins(parsed_asins)

    client = connect_to_api(api_key)

    try:
        db_conn, engine, asins, product_info, reviews = create_db(
            db_user_name, db_user_pass, db_name
        )

        for parsed_asin in parsed_checked_asins[:8]:
            init_parsed_asin = parsed_asin

            # write asins to db
            init_db(db_conn, parsed_asin, asins, product_info, reviews)

            # write product info and reviews to db with corresponding asins
            modify_db(
                db_conn,
                asins,
                product_info,
                reviews,
                init_parsed_asin,
                # return product info and reviews for asin
                *scrap_page(client, parsed_asin),
            )

        # erase_db_tables(engine, product_info, reviews, asins)

    except OperationalError:
        print_error_and_exit("Database connection error")


if __name__ == "__main__":
    main()
