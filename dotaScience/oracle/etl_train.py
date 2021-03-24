import os
import sys
from prefect import task, Parameter, Flow
import pandas as pd
import sqlalchemy
import dotenv
import argparse

ORACLE_DIR = os.path.dirname(__file__)
BASE_DIR = os.path.dirname(ORACLE_DIR)

sys.path.insert(0, BASE_DIR)

from backpack import db

dotenv.load_dotenv(dotenv.find_dotenv())

def create_abt( date_start, date_end ):
    query_path = os.path.join(ORACLE_DIR, "etl_query.sql" )
    query = db.import_query( query_path )
    con = db.open_mariadb()
    db.execute_multi_queries(con,
                             query,
                             date_start = date_start,
                             date_end = date_end)
    return True

def abt_to_csv(name):
    con = db.open_mariadb()
    df = pd.read_sql(name, con)
    df.to_csv(os.path.join(ORACLE_DIR, "train.csv"))
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date_start", "-s", help="Data de inicio para extração")
    parser.add_argument("--date_end", "-e", help="Data de fim para extração")
    args = parser.parse_args()

    print("Criando tabela de ABT no banco de dados.\nIsso pode levar um tempo...")
    create_abt(args.date_start, args.date_end)
    print("ok.")

    print("Salvando base em csv...")
    abt_to_csv( "tb_abt_oracle" )
    print("Salvando base em csv...")