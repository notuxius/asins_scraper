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


def create_db(db_user_name, db_user_pass, db_name):
    DATABASE_URI = (
        f"postgres+psycopg2://{db_user_name}:{db_user_pass}@localhost:5432/{db_name}"
    )
    # connect to db with provided credentials and db name
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
        Column("created_at", DateTime, nullable=False, default=datetime.datetime.now()),
        Column("name", String, nullable=False, default=""),
        Column("number_of_ratings", Integer, nullable=False, default=0),
        Column("average_rating", Float, nullable=False, default=0.0),
        Column("number_of_questions", Integer, nullable=False, default=0),
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
        Column("number_of_reviews", Integer, nullable=False, default=0),
        Column("top_positive_review", String, nullable=False, default=""),
        Column("top_critical_review", String, nullable=False, default=""),
    )

    meta.create_all(engine)
    db_conn = engine.connect()

    return db_conn, engine, asins, product_info, reviews
