import json
import os
import sys

import dotenv
import requests
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

sys.path.insert(0, os.getenv("DOTASCIENCE"))

from backpack import db

spark = db.create_spark_session()

tb_live_games = db.register_temp_view( spark, os.path.join( os.getenv("RAW"), "tb_live_games") )
tb_book = db.register_temp_view( spark, os.path.join( os.getenv("CONTEXT"), "tb_book_player") )

(spark.sql(f"SELECT * FROM {tb_book} limit 100")
     .toPandas()
     .to_excel(os.path.join(os.getenv("DATA"), "tb_book_sample.xlsx"))
)

query = db.import_query( os.path.join( os.getenv("ORACLE"), "ml", "predict", "etl_predict.sql" ))
df_predict = spark.sql(query).toPandas()

model = pd.read_pickle(os.path.join(os.getenv("ORACLE"), "models", "model.pkl"))

pred = model["model"].predict_proba(df_predict[model["features"]])
df_predict["proba_radiant"] = pred[:,1]

query_teams = db.import_query( os.path.join(os.getenv("ORACLE"), "ml", "predict", "get_teams.sql") )
df_matches = spark.sql(query_teams).toPandas()
df_matches = df_matches.dropna(subset=['radiant_team', "dire_team"], how="all")

df_final = df_matches.merge(df_predict, how='left', on="match_id")

df_final.to_excel(os.path.join(os.getenv("DATA"), "df_final.xlsx"))

columns = ["match_id", "radiant_team", "dire_team", "prob_radiant"]
df_final[columns].to_excel(os.path.join( os.getenv("DATA"), "df_predict.xlsx"))