import os
import sqlalchemy
import datetime
import argparse
import sys
import pandas as pd

ECHO_DIR = os.path.dirname(os.path.abspath(__file__))
DOTA_DIR = os.path.dirname(ECHO_DIR)
BASE_DIR = os.path.dirname(DOTA_DIR)

# Define o caminho de nosso projeto
sys.path.insert(0, DOTA_DIR)

from backpack import db

def insert_data(query, dt_ref, con):
    query = query.format( insert= "INSERT INTO TB_VUC_SAFRAS", date = dt_ref)
    con.execute( query )
    return True

def create_data(dt_ref, con):

    create_query = db.import_query(os.path.join(ECHO_DIR, "create.sql"))
    select_query = db.import_query(os.path.join(ECHO_DIR, "query.sql"))

    query = create_query.format(query=select_query.format( insert= "", date=dt_ref))
    db.execute_multi_queries(con, query)
    return True

date_now = datetime.datetime.now().strftime("%Y-%m-%d")

parser = argparse.ArgumentParser()
parser.add_argument("--date", help="Data para extração", type=str, default=date_now)
parser.add_argument("--create", help="Define se a tabela deve ser criada", action="store_true")
args = parser.parse_args()

con = db.open_mariadb()

if args.create:
    create_data(args.date, con)
else:
    insert_data(args.date, con)

