# -*- coding: utf-8 -*-
import csv
import datetime
import os
from collections import OrderedDict

from sqlalchemy.exc import OperationalError, ProgrammingError

from helpers import (
    check_asins,
    connect_to_api,
    get_page_soup,
    parse_args,
    prepare_text,
    print_error_and_exit,
)
from helpers_db import asin_exists_in_table, create_db_tables, init_db, list_db_tables


def get_product_info(client, url, scraped_asin):
    product_page = get_page_soup(client, url, scraped_asin)

    product_info = []

    if product_page:
        product_info.append(scraped_asin)

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
            print(f"Getting {info}, ASIN:", scraped_asin)
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


def get_reviews(client, url, scraped_asin):
    product_reviews_page = get_page_soup(client, url, scraped_asin)

    reviews = []

    # scraping number of reviews
    print("Getting number of reviews, ASIN:", scraped_asin)
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
            print(f"Getting {top_review}, ASIN:", scraped_asin)
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


def scrap_page(client, parsed_asin):
    amazon_base_url = "https://www.amazon.com/"
    amazon_product_url = f"{amazon_base_url}dp/"
    amazon_product_reviews_url = f"{amazon_base_url}product-reviews/"

    scraped_info = []

    product_info = get_product_info(
        client, f"{amazon_product_url}{parsed_asin}", parsed_asin
    )

    if product_info:
        scraped_info.extend(product_info)

        reviews = get_reviews(
            client,
            f"{amazon_product_reviews_url}{parsed_asin}",
            parsed_asin,
        )

        scraped_info.extend(reviews)

    return scraped_info


def parse_csv(csv_file):
    try:
        parsed_asins = csv.DictReader(open(csv_file))
        parsed_list = []
        for parsed_asin in parsed_asins:
            stripped_parsed_asin = parsed_asin["asin"].strip()
            if stripped_parsed_asin:
                parsed_list.append(stripped_parsed_asin)

        uniq_parsed_list = list(OrderedDict.fromkeys(parsed_list))

        return uniq_parsed_list

    except OSError:
        print_error_and_exit("Input file read error")


def modify_db(
    db_conn,
    db_engine,
    scraped_info,
    *db_tables,
):
    asins, product_info, reviews = db_tables

    (
        scraped_asin,
        scraped_product_name,
        scraped_number_of_ratings,
        scraped_average_rating,
        scraped_number_of_questions,
        scraped_number_of_reviews,
        scraped_top_positive_review,
        scraped_top_critical_review,
    ) = scraped_info

    if scraped_asin:
        for db_table in db_tables:
            if not asin_exists_in_table(db_engine, db_table, scraped_asin):
                db_conn.execute(
                    db_table.insert(),
                    [
                        {
                            "asin": scraped_asin,
                        },
                    ],
                )

        print("Writing product info to database, ASIN:", scraped_asin)
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
            .where(product_info.c.asin == scraped_asin)
        )

        print("Writing reviews to database, ASIN:", scraped_asin)
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
            .where(reviews.c.asin == scraped_asin)
        )


def main():
    parsed_args = parse_args()

    abs_path = os.path.abspath(__file__)
    dir_name = os.path.dirname(abs_path)

    api_key = parsed_args.api_key
    db_user_name = parsed_args.db_user_name
    db_user_pass = parsed_args.db_user_pass
    db_name = parsed_args.db_name
    csv_file = os.path.join(dir_name, parsed_args.csv_file)

    client = connect_to_api(api_key)

    try:
        # connect to database
        meta, db_engine, db_conn = init_db(db_user_name, db_user_pass, db_name)

        # use excisting database tables or create new ones
        db_tables = list_db_tables(meta, db_engine) or create_db_tables(meta, db_engine)

        if db_tables and db_conn:
            parsed_asins = check_asins(parse_csv(csv_file))

            for parsed_asin in parsed_asins:

                # get product info and reviews for asin
                scraped_info = scrap_page(client, parsed_asin)

                # write product info and reviews for asin to database
                if scraped_info:
                    modify_db(
                        db_conn,
                        db_engine,
                        scraped_info,
                        *db_tables,
                    )

        else:
            print_error_and_exit("Database error")

    except (OperationalError, ProgrammingError):
        print_error_and_exit("Database connection error")


if __name__ == "__main__":
    main()
