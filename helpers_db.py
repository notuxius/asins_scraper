import datetime

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
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists


def asin_exists_in_table(db_engine, db_table, asin):
    Session = sessionmaker(bind=db_engine)
    session = Session()

    return session.query(exists().where(db_table.c.asin == asin)).scalar()


def init_db(db_user_name, db_user_pass, db_name):
    db_uri = (
        f"postgres+psycopg2://{db_user_name}:{db_user_pass}@localhost:5432/{db_name}"
    )
    # connect to db with provided credentials and db name
    db_engine = create_engine(db_uri, echo=False)
    db_conn = db_engine.connect()

    meta = MetaData()

    return meta, db_engine, db_conn


def create_list_db_tables(meta, db_engine):
    Table(
        "asins",
        meta,
        Column("asin", String, primary_key=True),
    )

    Table(
        "product_info",
        meta,
        Column(
            "asin",
            String,
            ForeignKey("asins.asin", ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("created_at", DateTime, onupdate=datetime.datetime.now()),
        Column("name", String, nullable=False, default="", onupdate=""),
        Column("number_of_ratings", Integer, nullable=False, default=0, onupdate=0),
        Column("average_rating", Float, nullable=False, default=0.0, onupdate=0.0),
        Column("number_of_questions", Integer, nullable=False, default=0, onupdate=0),
    )

    Table(
        "reviews",
        meta,
        Column(
            "asin",
            String,
            ForeignKey("asins.asin", ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("number_of_reviews", Integer, nullable=False, default=0, onupdate=0),
        Column("top_positive_review", String, nullable=False, default="", onupdate=""),
        Column("top_critical_review", String, nullable=False, default="", onupdate=""),
    )

    meta.create_all(db_engine)

    return meta.sorted_tables