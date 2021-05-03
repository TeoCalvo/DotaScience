import argparse
import os
import datetime
import sys

import sqlalchemy
import pandas as pd

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())
sys.path.insert(0, os.getenv("DOTASCIENCE"))

from backpack import db

def process_data(dt_ref, spark):
    query_path = os.path.join( os.getenv("ECHO_SLAM") , "query.sql")
    select_query = db.import_query(query_path)
    query = select_query.format(dt_ref=dt_ref)
    table_path = os.path.join( os.getenv("CONTEXT"), "tb_book_player")

    ( spark.sql(query)
           .repartition(1)
           .write
           .option("mergeSchema", "true")
           .mode("overwrite")
           .format("parquet")
           .save(os.path.join(table_path, f"dt_ref={dt_ref}"))
    )

    return True


def main(date, date_stop):
    
    spark = db.create_spark_session()
    tb_path = os.path.join(os.getenv("RAW"), "tb_match_player")
    view_name  = db.register_temp_view(spark, tb_path)
    process_data(date, spark)


if __name__ == "__main__":

    date_now = datetime.datetime.now().strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Data para extração", type=str, default=date_now)
    args = parser.parse_args()

    main(args.date, args.date)
