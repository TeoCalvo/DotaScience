import requests
from pymongo import MongoClient
import os
import dotenv
from tqdm import tqdm
import time
import json

def get_data(match_id):

    try:
        url = f"https://api.opendota.com/api/matches/{match_id}"
        response = requests.get(url)
        data = response.json()
        return data

    except json.JSONDecodeError as err:
        print(err)
        return None   

def save_data(data, db_collection):
    try:
        db_collection.delete_one({"match_id":data["match_id"]})
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

def main():
    mongodb_client = MongoClient("localhost", 27017)
    mongodb_database = mongodb_client["dota_raw"]

    for match_id in tqdm(find_match_ids(mongodb_database)):
        data = get_data(match_id)

        if data is None:
            continue

        if save_data(data, mongodb_database["pro_match_details"]):
            time.sleep(0.5)
        else:
            print(data)
            time.sleep(60)

if __name__ == "__main__":
    main()