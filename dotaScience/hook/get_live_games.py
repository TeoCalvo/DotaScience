import json
import os
import sys

import dotenv
import requests

import pandas as pd
import numpy as np

import dotenv
from tqdm import tqdm

dotenv.load_dotenv(dotenv.find_dotenv())
sys.path.insert(0, os.getenv("DOTASCIENCE"))

from backpack import db

def get_live_games():
    api_key = os.getenv("VALVE_API")
    url = f"http://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/?key={api_key}"
    response = requests.get(url)
    data = response.json()
    return data


def transform_player(raw):
    df_players = pd.DataFrame(raw['players'])
    df_players["lobby_id"] = raw["lobby_id"]
    df_players["match_id"] = raw["match_id"]
    df_players["spectators"] = raw["spectators"]
    df_players["league_id"] = raw["league_id"]
    return df_players

def transform_teams(raw):
    teams = []
    for k, v in {0:"radiant_team", 1:"dire_team"}.items():
        try:
            data = raw[v]
            data['team'] = k
            teams.append( pd.Series(data).to_frame().T )
        except KeyError:
            print(f"Time {v} não encontrado")

    try:
        df_teams = pd.concat(teams, axis=0)
        return df_teams

    except ValueError:
        print("Não foram encontrados times")
        return pd.DataFrame()

def transform(raw):
    df_players = transform_player(raw)
    df_teams = transform_teams(raw)
    try:
        df_full = df_players.merge(df_teams, how='left')
    
    except pd.errors.MergeError as err:
        print("Não foi possível cruzar as infos com a base de time")
        df_full = df_players.copy()
    
    df_full = df_full.fillna("").astype(str)
    df_full = df_full[df_full["match_id"].astype(int)>0]
    return df_full

def save_data(data, spark):
    
    tb_path = os.path.join( os.getenv("RAW"), "tb_live_games", f"match_id={data['match_id'][0]}")
    
    df = spark.createDataFrame(data)
    ( df.repartition(1)
        .write
        .mode("overwrite")
        .format("parquet")
        .save(tb_path) )

    return True

if __name__ == "__main__":

    spark = db.create_spark_session()

    data = get_live_games()
    for g in tqdm(data["result"]["games"]):
        df = transform(g)
        
        if df.shape[0] > 0:
            cols = ["account_id","name","hero_id","team"]
            save_data(df, spark)