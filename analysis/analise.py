from pymongo import MongoClient
import pandas as pd

client = MongoClient("localhost", 27017)
db = client["dota_raw"]

col = db["pro_match_details"]

match_id = [ i["match_id"] for i in col.find( {} , {"match_id"}) ]

x = pd.value_counts(match_id)

col.delete_many( { "match_id": {"$in":x[ x > 1 ].index.tolist()} } )