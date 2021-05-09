import json
import os
import sys
import datetime

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

# Live games
tb_live_games = db.register_temp_view( spark, os.path.join( os.getenv("RAW"), "tb_live_games") )
spark.table(tb_live_games).toPandas().to_csv( os.path.join(os.getenv("DATA"), "live_games.csv" ))

# Book de variávels
tb_book = db.register_temp_view( spark, os.path.join( os.getenv("CONTEXT"), "tb_book_player") )
print(spark.sql(f"SELECT COUNT(*) FROM {tb_book}").toPandas())

# ETL
query = db.import_query(os.path.join(os.getenv("ORACLE"), "ml", "predict", "etl_predict.sql"))
df_predict = spark.sql(query).toPandas()
df_predict.to_csv(os.path.join(os.getenv("DATA"), "pre_predict.csv"))

# Importa o modelo de ML
model = pd.read_pickle(os.path.join(os.getenv("ORACLE"), "models", "model.pkl"))
pred = model["model"].predict_proba(df_predict[model["features"]])
df_predict["proba_radiant"] = pred[:,1]

# Traz as infos dos times
query_teams = db.import_query( os.path.join(os.getenv("ORACLE"), "ml", "predict", "get_teams.sql") )
df_matches = spark.sql(query_teams).toPandas()
df_matches = df_matches.dropna(subset=['radiant_team', "dire_team"], how="all")

df_final = df_matches.merge(df_predict, how='left', on=["match_id"])

df_final["update_at"] = datetime.datetime.now()
columns = ["match_id", "radiant_team", "dire_team", "proba_radiant", "update_at"]
df_final[columns].to_csv(os.path.join( os.getenv("DATA"), "df_predict.csv"), index=False)