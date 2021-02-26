import requests
from pymongo import MongoClient
import dotenv
import os
import time
import datetime
import argparse

def get_matches_batch(**kwargs):
    '''Captura lista de partidas pro players.
    Caso seja passada um id de partida, a coleta é realizada a partir desta'''
    
    url = "https://api.opendota.com/api/proMatches"
    if len(kwargs) != 0:
        url += "?" + "&".join( f"{k}={v}" for k,v in kwargs.items())

    data = requests.get(url).json()
    return data

def save_matches(data, db_collection):
    '''Salva lista de partidas no banco de dados'''
    db_collection.insert_many(data)
    return True

def get_and_save(min_match_id=None, max_match_id=None, db_collection=None ):
    
    kwargs = {}

    if API_KEY is not None and len(API_KEY) != 0:
        kwargs["api_key"] = API_KEY

    if min_match_id is not None:
        kwargs["less_than_match_id"] = min_match_id
    
    data_raw = get_matches_batch(**kwargs)

    if len(data_raw) == 0:
        print("Não há mais partidas neste modo de coleta.")
        return None

    data = [i for i in data_raw if "match_id" in i]

    if len(data) == 0:
        print("Não foram coletadas mais partidas, talves o limite de requisições tenha sido atingido.")
        return data

    if max_match_id is not None:
        data = [i for i in data if i["match_id"] > max_match_id]
        if len(data) == 0:
            print("Não há mais partidas recentes.")
            return None
    
    save_matches(data, db_collection)
    print(len(data), "--" , datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return data

def get_oldest_matches(db_collection):
    min_match_id = db_collection.find_one(sort=[("match_id",1)])["match_id"]
    data = {}
    while data is not None:
        data = get_and_save(min_match_id=min_match_id, db_collection=db_collection)
        min_match_id = min([i["match_id"] for i in data])

def get_newest_matches(db_collection):
    try:
        max_match_id = db_collection.find_one(sort=[("match_id",-1)])["match_id"]
    except TypeError:
        max_match_id = 0

    data = get_and_save(max_match_id=max_match_id, db_collection=db_collection)
    
    try:
        min_match_id = min([i["match_id"] for i in data])
    except ValueError as err:
        return None
    except TypeError as err:
        return None   
    
    while min_match_id > max_match_id and data is not None:
        data = get_and_save(min_match_id=min_match_id, db_collection=db_collection)
        min_match_id = min([i["match_id"] for i in data])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--how", choices=["oldest", "newest"], default="newest")
    args = parser.parse_args()

    mongodb_client = MongoClient(MONGODB_IP, MONGODB_PORT)
    mongodb_database = mongodb_client["dota_raw"]

    if args.how == "oldest":
        get_oldest_matches(mongodb_database["pro_match_history"])
    
    elif args.how == "newest":
        get_newest_matches(mongodb_database["pro_match_history"])

if __name__ == "__main__":

    # Carrega o dotenv
    dotenv.load_dotenv(dotenv.find_dotenv())

    API_KEY = os.getenv("API_KEY")
    MONGODB_IP = os.getenv("MONGODB_IP")
    MONGODB_PORT = int(os.getenv("MONGODB_PORT"))

    main()