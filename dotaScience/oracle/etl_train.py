import os
import sys
import pandas as pd
import sqlalchemy
import dotenv
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

ORACLE_DIR = os.path.dirname(os.path.abspath(__file__))
#ORACLE_DIR = os.path.join( os.path.abspath("."), "dotaScience, oracle")
SRC_DIR = os.path.dirname(ORACLE_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, "data")

DATA_TRAIN_DIR = os.path.join(DATA_DIR, "train")

CONTEXT_DATA_DIR = os.path.join(DATA_DIR, "context")
PROCEDED_DATA_DIR = os.path.join(DATA_DIR, "proceded")

sys.path.insert(0, SRC_DIR)

from backpack import db

dotenv.load_dotenv(dotenv.find_dotenv())

def load_view(spark, path):

    (spark.read
          .format("parquet")
          .load(path)
          .createOrReplaceTempView(path.split("/")[-1]))

def load_views(spark, paths):
    for p in paths:
        load_view(spark, p)

def create_abt( date_start, date_end, spark ):
    query_path = os.path.join(ORACLE_DIR, "etl_query.sql" )
    query = db.import_query( query_path )
    query = query.format( date_start=date_start, date_end=date_end )

    ( spark.sql(query)
           .repartition(1)
           .write
           .mode("overwrite")
           .format("parquet")
           .save( os.path.join(CONTEXT_DATA_DIR, "tb_abt_oracle") ) )
    
    return True


def abt_to_csv(spark):

    ( spark.read
           .format("parquet")
           .load( os.path.join(CONTEXT_DATA_DIR, "tb_abt_oracle"))
           .repartition(1)
           .write
           .options(header=True)
           .mode("overwrite")
           .format("csv")
           .save( os.path.join( DATA_TRAIN_DIR, "tb_abt_oracle" ) ) )

    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date_start", "-s", help="Data de inicio para extração")
    parser.add_argument("--date_end", "-e", help="Data de fim para extração")
    args = parser.parse_args()

    if not os.path.exists(DATA_TRAIN_DIR):
        os.mkdir(DATA_TRAIN_DIR)
    
    spark = ( SparkSession.builder
                        .appName("Dota Spark")
                        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                        .getOrCreate()
    )

    from delta.tables import *

    paths = [ os.path.join(PROCEDED_DATA_DIR, "tb_match_player"),
                os.path.join(CONTEXT_DATA_DIR, "tb_book_player") ]

    load_views(spark, paths)

    print("Criando tabela de ABT no banco de dados.\nIsso pode levar um tempo...")
    create_abt(args.date_start, args.date_end, spark)
    print("ok.")

    print("Salvando base em csv...")
    abt_to_csv(spark)
    print("Salvando base em csv...")
