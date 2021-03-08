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

def insert_data(dt_ref, con):
    select_query = db.import_query(os.path.join(ECHO_DIR, "query.sql"))
    con.execute(f"DELETE FROM TB_VUC_SAFRAS WHERE dt_ref = '{dt_ref}'")
    query = "INSERT INTO TB_VUC_SAFRAS\n" + select_query.format(dt_ref = dt_ref)
    con.execute( query )
    return True

def create_data(dt_ref, con):

    create_query = db.import_query(os.path.join(ECHO_DIR, "create.sql"))
    select_query = db.import_query(os.path.join(ECHO_DIR, "query.sql"))

    query = create_query.format(query=select_query.format( dt_ref=dt_ref))
    db.execute_multi_queries(con, query)
    return True


def exec_loop(dt_start, dt_end, con):
    date_start = datetime.datetime.strptime(dt_start, "%Y-%m-%d")
    date_end = datetime.datetime.strptime(dt_end, "%Y-%m-%d")

    while date_end >= date_start:
        dt_start = date_start.strftime("%Y-%m-%d")
        print(dt_start)        
        insert_data(dt_start, con)
        date_start += datetime.timedelta(days=1)

    return True

date_now = datetime.datetime.now().strftime("%Y-%m-%d")

parser = argparse.ArgumentParser()
parser.add_argument("--date", help="Data para extração", type=str, default=date_now)
parser.add_argument("--create", help="Define se a tabela deve ser criada", action="store_true")
parser.add_argument("--date_end", help="Define a data final para processo")
args = parser.parse_args()

con = db.open_mariadb()

if args.create:
    create_data(args.date, con)
else:
    exec_loop( args.date, args.date_end, con )

