import requests
from pymongo import MongoClient
import dotenv
import os
import time
import datetime
import argparse

def get_matches_batch(min_match_id=None):
    '''Captura lista de partidas pro players.
    Caso seja passada um id de partida, a coleta é realizada a partir desta'''
    
    url = "https://api.opendota.com/api/proMatches"
    if min_match_id is not None:
        url += f"?less_than_match_id={min_match_id}"

    data = requests.get(url).json()
    
    return data

def save_matches(data, db_collection):
    '''Salva lista de partidas no banco de dados'''
    for d in data:
        db_collection.delete_one( {"match_id": d["match_id"] } )
        db_collection.insert_one( d )
    return True

def get_and_save( min_match_id=None, max_match_id=None, db_collection=None ):
    data_raw = get_matches_batch(min_match_id=min_match_id)
    data = [i for i in data_raw if "match_id" in i]

    if len(data) == 0:
        print("Limite excedido de requests!")
        return False, data

    if max_match_id is not None:
        data = [i for i in data if i["match_id"] > max_match_id]
        if len(data) == 0:
            print("Todas novas partidas foram adicionadas!")
            return False, data
    
    save_matches(data, db_collection)
    min_match_id = min([i["match_id"] for i in data])
    print(len(data), "--" , datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(1)
    return True, data
    
def get_oldest_matches(db_collection):
    min_match_id = db_collection.find_one(sort=[("match_id",1)])["match_id"]
    count = 1
    while True:
        check, data = get_and_save(min_match_id=min_match_id, db_collection=db_collection)
        if not check:
            break
        
        min_match_id = min([i["match_id"] for i in data])
        count += 1

def get_newest_matches(db_collection):
    try:
        max_match_id = db_collection.find_one(sort=[("match_id",-1)])["match_id"]
    except TypeError:
        max_match_id = 0

    _, data = get_and_save(max_match_id=max_match_id, db_collection=db_collection)
    
    try:
        min_match_id = min([i["match_id"] for i in data])
    except ValueError:
        return
    
    count = 0
    while min_match_id > max_match_id:
        check, data = get_and_save(min_match_id=min_match_id, db_collection=db_collection)
        if not check:
            break
        
        min_match_id = min([i["match_id"] for i in data])
        count += 1

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--how", choices=["oldest", "newest"])
    args = parser.parse_args()

    # Carrega o dotenv
    dotenv.load_dotenv(dotenv.find_dotenv())

    MONGODB_IP = os.getenv("MONGODB_IP")
    MONGODB_PORT = int(os.getenv("MONGODB_PORT"))

    mongodb_client = MongoClient(MONGODB_IP, MONGODB_PORT)
    mongodb_database = mongodb_client["dota_raw"]

    if args.how == "oldest":
        get_oldest_matches(mongodb_database["pro_match_history"])
    
    elif args.how == "newest":
        get_newest_matches(mongodb_database["pro_match_history"])

if __name__ == "__main__":
    main()