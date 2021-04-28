import json
import os
import sys
import argparse

import numpy as np
import pandas as pd
import sqlalchemy

from pymongo import MongoClient
from pyspark.sql import SparkSession
import pyspark

import dotenv
from tqdm import tqdm

dotenv.load_dotenv( dotenv.find_dotenv() )

sys.path.insert(0, os.getenv("DOTASCIENCE"))

from backpack import utils as bpu
from backpack import db

def import_columns():
    path = os.path.join(os.getenv("MAGIC_WAND"), "db.json")
    with open(path, "r") as open_file:
        dict_db = json.load(open_file)
    return dict_db["columns"]

def get_players(match_data, columns):
    df_full = pd.DataFrame(columns=list(columns.keys()))
    for p in match_data["players"]:
        data = {c:[p[c]] for c in p if c in columns}
        df = pd.DataFrame(data)
        df_full = df_full.append(df)

    df_full["dt_match"] = pd.to_datetime(df_full["start_time"], unit="s")
    df_full["match_year"] = df_full["dt_match"].apply(lambda x: x.year)
    df_full["match_month"] = df_full["dt_match"].apply(lambda x: x.month)
    df_full = df_full.replace("^None", np.nan, regex=True)
    df_full = df_full.astype(str)
    return df_full.reset_index(drop=True)

def insert_players(data, spark, mode="overwrite"):
    sdf = spark.createDataFrame( data )
    ( sdf.repartition(1)
         .write
         .mode(mode)
         .format("parquet")
         .option("mergeSchema", "true")
         .partitionBy("match_year", "match_month")
         .save(os.path.join(os.getenv("RAW") , "tb_match_player"))
    )
    return True

def get_match_list(spark, db_collection):

    query = '''
    SELECT DISTINCT match_id as id_list
    from tb_match_player
    '''

    try:
        (spark.read
              .format("parquet")
              .load(os.path.join(os.getenv("RAW"), "tb_match_player"))
              .createTempView("tb_match_player")
        )
        match_ids_spark = spark.sql(query).toPandas()["id_list"].astype(int).tolist()
        print(len(match_ids_spark))

    except pyspark.sql.utils.AnalysisException as err:
        print("Erro:", err)
        match_ids_spark = []
    
    match_list = db_collection.find({"match_id" : {"$nin": match_ids_spark}})
    return match_list

def main():

    dotenv.load_dotenv(dotenv.find_dotenv())

    mongo_client = MongoClient(os.getenv("MONGODB_IP"), int(os.getenv("MONGODB_PORT")))
    mongo_database = mongo_client["dota_raw"]
    details_collection = mongo_database["pro_match_details"]

    spark = db.create_spark_session()
    cursor = get_match_list(spark, details_collection)
    print(cursor.count())

    columns = import_columns()
    df = pd.DataFrame()

    for i in tqdm(cursor):
        df = df.append( get_players(i, columns) )

        if df.shape[0] >= 50000:
            insert_players(df, spark, "append")
            df = pd.DataFrame()
    
    if 0 < df.shape[0] < 50000:
        insert_players(df, spark, "append")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", "-c", help="Chunk size value", type=int, default=50000)
    main()