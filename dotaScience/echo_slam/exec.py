import argparse
import os
import datetime
import sys

import sqlalchemy
import pandas as pd
from pyspark.sql import SparkSession

ECHO_DIR = os.path.dirname(os.path.abspath(__file__))
DOTA_DIR = os.path.dirname(ECHO_DIR)
BASE_DIR = os.path.dirname(DOTA_DIR)

# Define o caminho de nosso projeto
sys.path.insert(0, DOTA_DIR)

from backpack import db

def insert_data(dt_ref, spark, mode="overwrite"):
    select_query = db.import_query(os.path.join(ECHO_DIR, "query.sql"))
    query = select_query.format(dt_ref = dt_ref)
    (spark.sql(query)
          .repartition(1)
          .write
          .mode(mode)
          .format("delta")
          .partitionBy("partition_year", "partition_month", "partition_day")
          .save(os.path.join(os.getenv("DATA_CONTEXT"), "tb_book_player"))
    )
    return True

def exec_loop(dt_start, dt_end, spark):
    date_start = datetime.datetime.strptime(dt_start, "%Y-%m-%d")
    date_end = datetime.datetime.strptime(dt_end, "%Y-%m-%d")

    while date_end >= date_start:
        dt_start = date_start.strftime("%Y-%m-%d")
        print(dt_start)        
        insert_data(dt_start, spark, "append")
        date_start += datetime.timedelta(days=1)
    return True

date_now = datetime.datetime.now().strftime("%Y-%m-%d")

parser = argparse.ArgumentParser()
parser.add_argument("--date", help="Data para extração", type=str, default=date_now)
parser.add_argument("--create", help="Define se a tabela deve ser criada", action="store_true")
parser.add_argument("--date_end", help="Define a data final para processo")
args = parser.parse_args()

spark = ( SparkSession.builder
                      .appName("Dota Spark")
                      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
                      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                      .getOrCreate()
)

(spark.read
      .format("parquet")
      .load(os.path.join(os.getenv("DATA_PROCEDED"), "tb_match_player"))
      .createOrReplaceTempView("tb_match_player")
)

if args.create:
    insert_data(args.date, spark, "overwrite")
else:
    exec_loop( args.date, args.date_end, spark )

