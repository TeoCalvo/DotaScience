import os
import dotenv
import sqlalchemy

dotenv.load_dotenv(dotenv.find_dotenv())

def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def open_mariadb():
    ip = os.getenv("MARIADB_IP")
    port = os.getenv("MARIADB_PORT")
    pswd = os.getenv("MARIADB_PSWD")
    user = os.getenv("MARIADB_USER")
    dbname = os.getenv("MARIADB_DATABASE")
    con = sqlalchemy.create_engine( f"mysql+pymysql://{user}:{pswd}@{ip}/{dbname}" )
    return con