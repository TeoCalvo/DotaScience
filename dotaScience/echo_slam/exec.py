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

def exec_loop(dt_start, dt_end, spark):
    date_start = datetime.datetime.strptime(dt_start, "%Y-%m-%d")
    date_end = datetime.datetime.strptime(dt_end, "%Y-%m-%d")

    while date_end >= date_start:
        dt_start = date_start.strftime("%Y-%m-%d")
        print(dt_start)
        process_data(dt_start, spark)
        date_start += datetime.timedelta(days=1)
    return True


def main(date_start, date_stop):
    
    spark = db.create_spark_session()

    tb_path = os.path.join(os.getenv("RAW"), "tb_match_player")
    view_name  = db.register_temp_view(spark, tb_path)

    if args.create:
        process_data(date_start, spark)
    else:
        exec_loop( date_start, date_stop, spark )

if __name__ == "__main__":

    date_now = datetime.datetime.now().strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser()
    parser.add_argument("--date_start", help="Data para extração", type=str, default=date_now)
    parser.add_argument("--create", help="Define se a tabela deve ser criada", action="store_true")
    parser.add_argument("--date_stop", help="Define a data final para processo")
    args = parser.parse_args()

    main(args.date_start, args.date_stop)
