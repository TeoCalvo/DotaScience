import pandas as pd
import sqlalchemy
from pymongo import MongoClient
import os
import dotenv
from tqdm import tqdm

def parse_match(match_data):
    new_data = {}
    for c in data:
        try:
            _ = len(data[c])
        except TypeError:
            new_data[c] = [data[c]]
    return pd.DataFrame(new_data)

def parse_player(player_data):
    
    columns = [ "match_id",
                "player_slot",
                "account_id",
                "assists",
                "deaths",
                "denies",
                "firstblood_claimed",
                "gold",
                "gold_per_min",
                "gold_spent",
                "hero_damage",
                "hero_healing",
                "hero_id",
                "last_hits",
                "level",
                #"max_hero_hit",
                "pred_vict",
                "roshans_killed",
                "tower_damage",
                "towers_killed",
                "xp_per_min",
                "personaname",
                "name",
                "radiant_win",
                "start_time",
                "duration",
                "game_mode",
                "patch",
                "region",
                "isRadiant",
                "win",
                "total_gold",
                "total_xp",
                "kills_per_min",
                "kda",
                "neutral_kills",
                "tower_kills",
                "tower_kills",
                "courier_kills",
                "lane_kills",
                "hero_kills",
                "observer_kills",
                "sentry_kills",
                "roshan_kills",
                "necronomicon_kills",
                "ancient_kills",
                "buyback_count",
                "observer_uses",
                "sentry_uses",
                "lane_efficiency",
                "lane_efficiency_pct",
                "lane",
                "lane_role",
                "purchase_tpscroll",
                "actions_per_min",
                "rank_tier" ]

    df = pd.DataFrame({c:[player_data[c]] for c in player_data if c in columns})
    df["dt_match"] = pd.to_datetime( df["start_time"], unit="s" )
    columns = df.columns.tolist()
    columns.sort()
    df = df[columns]

    df_standard = pd.DataFrame(columns=columns)
    return df_standard.append(df)

def get_players(match_data):
    dfs = []
    for p in match_data["players"]:
        dfs.append( parse_player( p ) )
    return pd.concat(dfs)

def insert_players( data, con ):
    data.to_sql("tb_match_player", con, if_exists="append", index=False)
    return True

def open_connection_mariadb():
    ip = os.getenv("MARIADB_IP")
    port = os.getenv("MARIADB_PORT")
    pswd = os.getenv("MARIADB_PSWD")
    user = os.getenv("MARIADB_USER")
    dbname = os.getenv("MARIADB_DATABASE")
    con = sqlalchemy.create_engine( f"mysql+pymysql://{user}:{pswd}@{ip}/{dbname}" )
    return con


def get_match_list(con, db_collection):


    query = '''
    SELECT DISTINCT match_id as id_list
    from tb_match_player
    '''
   
    try:
        match_ids_maria = pd.read_sql_query(query, con)["id_list"].tolist()
    except:
        match_ids_maria = []
    
    match_list = db_collection.find({"match_id" : {"$nin": match_ids_maria}})
    return match_list

def main():

    dotenv.load_dotenv(dotenv.find_dotenv())

    mongo_client = MongoClient( os.getenv("MONGODB_IP"), int(os.getenv("MONGODB_PORT")) )
    mongo_database = mongo_client["dota_raw"]
    details_collection = mongo_database["pro_match_details"]

    con_maria = open_connection_mariadb()
    cursor = get_match_list( con_maria, details_collection)

    for c in tqdm(cursor):
        df_players = get_players(c)
        insert_players(df_players, con_maria)

if __name__ == "__main__":
    main()
