import os
import dotenv
import sqlalchemy

dotenv.load_dotenv(dotenv.find_dotenv())

def import_query(path):
    '''Importa uma determinada query'''
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def open_mariadb():
    '''Abre conexão com banco de dados MariDB usando as informações do .env'''
    ip = os.getenv("MARIADB_IP")
    port = os.getenv("MARIADB_PORT")
    pswd = os.getenv("MARIADB_PSWD")
    user = os.getenv("MARIADB_USER")
    dbname = os.getenv("MARIADB_DATABASE")
    con = sqlalchemy.create_engine( f"mysql+pymysql://{user}:{pswd}@{ip}/{dbname}" )
    return con

def execute_query(con, query, **kwargs):
    query_fmt = query.format(**kwargs)
    con.execute(query_fmt)
    return True

def execute_multi_queries(con, query, **kwargs):
    query_fmt = query.format(**kwargs)
    for q in query_fmt.split(";")[:-1]:
        execute_query(con, q)
    return True