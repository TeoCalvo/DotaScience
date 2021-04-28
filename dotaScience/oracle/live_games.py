import json
import os

import dotenv
import requests
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

#ORACLE_DIR = os.path.dirname(os.path.abspath(__file__))
#SRC_DIR = os.path.dirname(ORACLE_DIR)

def get_live_games():
    api_key = os.getenv("VALVE_API")
    url = f"http://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/?key={api_key}"
    response = requests.get(url)
    data = response.json()
    return data

def get_team_players_id(data):
    full_data = {}
    for d in data["result"]["games"]:
        full_data[d["match_id"]] = get_players_id(d)

    return full_data

def get_players_id( match_data ):
    teams = ["radiant", "dire"]
    teams_ids = {}

    try:
        for t in teams:
            players = match_data['scoreboard'][t]["players"]
            teams_ids[t] = [i['account_id'] for i in players]
        return teams_ids
    except:
        return {}

def dict_to_table(match_players_dict):
    df = pd.DataFrame(match_players_dict)
    radiant = df.T[["radiant"]].explode("radiant").rename(columns={"radiant":"player_id"})
    dire = df.T[["dire"]].explode("dire").rename(columns={"dire":"player_id"})

    radiant["isRadiant"] = 1
    dire["isRadiant"] = 0

    full_match = pd.concat( [radiant, dire], axis=0)
    full_match = full_match.reset_index().rename(columns={"index":"match_id"})

    full_match["player_id"] = full_match["player_id"].replace(0, np.nan)

    return full_match

def open_spark_con():
    spark = ( SparkSession.builder
                          .appName("Dota Spark")
                          .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
                          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                          .getOrCreate()
    )
    return spark

def load_view(spark, path):
    (spark.read
          .format("parquet")
          .load(path)
          .createOrReplaceTempView(path.split("/")[-1]))

# Pega dado da api
data = get_live_games()

# pega dados de ids de players
match_players_data = get_team_players_id(data)

# Transforma em tabular
df_players_id = dict_to_table(match_players_data)

# Abre conexão com spark
spark = open_spark_con()
from delta.tables import *

# registra uma view
book_path = "/home/teo/Documentos/ensino/twitch/projetos/DotaScience/data/context/tb_book_player"
load_view(spark, book_path)

spark.createDataFrame(df_players_id).createOrReplaceTempView("match_players")

with open("/home/teo/Documentos/ensino/twitch/projetos/DotaScience/dotaScience/oracle/etl_predict.sql", "r") as open_file:
    query = open_file.read()

df_predict = spark.sql(query).toPandas()

model = pd.read_pickle("/home/teo/Documentos/ensino/twitch/projetos/DotaScience/dotaScience/oracle/models/model.pkl")

pred = model["model"].predict_proba( df_predict[ model["features"] ] )

df_predict["proba_radiant"] = pred[:,1]

print(df_predict)