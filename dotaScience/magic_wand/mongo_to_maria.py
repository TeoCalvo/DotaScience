import pandas as pd
import sqlalchemy
from pymongo import MongoClient
import os
import dotenv
from tqdm import tqdm
import json
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname( __file__ )))

from backpack import db

def import_columns():
    path = os.path.join( os.path.dirname(os.path.abspath(__file__)), "db.json")
    with open( path, "r" ) as open_file:
        dict_db = json.load( open_file )
    return dict_db["columns"]

def get_players(match_data, columns):
    df_full = pd.DataFrame(columns=columns)
    for p in match_data["players"]:
        data = {c:[p[c]] for c in p if c in columns}
        df = pd.DataFrame(data)
        df_full = df_full.append(df)

    df_full["dt_match"] = pd.to_datetime( df_full["start_time"], unit="s" )    
    return df_full

def insert_players( data, con, create=False ):
    if create:
        data.to_sql("tb_match_player", con, if_exists="replace", index=False)
    else:
        data.to_sql("tb_match_player", con, if_exists="append", index=False)        
    return True

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

    con_maria = db.open_mariadb()
    cursor = get_match_list( con_maria, details_collection)

    columns = import_columns()

    for c in tqdm(cursor):
        df_players = get_players(c, columns)
        insert_players(df_players, con_maria)

if __name__ == "__main__":
    main()
