import requests
from pymongo import MongoClient
import os
import dotenv
from tqdm import tqdm
import time
import json

def get_data(match_id, **kwargs):

    try:
        url = f"https://api.opendota.com/api/matches/{match_id}"

        if len(kwargs) != 0:
            url += "?" + "&".join( f"{k}={v}" for k,v in kwargs.items())
        
        response = requests.get(url)
        data = response.json()
        return data

    except json.JSONDecodeError as err:
        print(err)
        return None   

def save_data(data, db_collection):
    try:
        db_collection.insert_one(data)
        return True
    except KeyError as err:
        print(err)
        return False

def find_match_ids(mongodb_database):
    collection_history = mongodb_database["pro_match_history"]
    collection_details = mongodb_database["pro_match_details"]

    match_history = set([i["match_id"] for i in collection_history.find({}, {"match_id":1})])
    match_details = set([i["match_id"] for i in collection_details.find({}, {"match_id":1})])

    match_ids = list(match_history - match_details)
    return match_ids

def main(api_key):
    mongodb_client = MongoClient(MONGODB_IP, MONGODB_PORT)
    mongodb_database = mongodb_client["dota_raw"]

    for match_id in tqdm(find_match_ids(mongodb_database)):
        data = get_data(match_id, api_key = api_key)

        try:
            _ = data["match_id"]
            save_data(data, mongodb_database["pro_match_details"])

        except KeyError:
            try:
                err = data["error"]
                print(err)
                continue
            except KeyError:
                time.sleep(2)
                print(data)

        except TypeError:
            print(None)
            continue

if __name__ == "__main__":
    # Carrega o dotenv
    dotenv.load_dotenv(dotenv.find_dotenv())

    API_KEY = os.getenv("API_KEY")
    MONGODB_IP = os.getenv("MONGODB_IP")
    MONGODB_PORT = int(os.getenv("MONGODB_PORT"))
    
    main(API_KEY)