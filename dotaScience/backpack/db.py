import os
import dotenv
import sqlalchemy
from pyspark.sql import SparkSession

dotenv.load_dotenv(dotenv.find_dotenv())

def import_query(path):
    '''Importa uma determinada query'''
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def connect_maria_aws():

    user = os.getenv("MARIA_DB_USER")
    password = os.getenv("MARIA_DB_PASS")
    host = os.getenv("MARIA_DB_HOST")
    port = os.getenv("MARIA_DB_PORT")
    dbname = "teomewhy"

    string_con = f"mariadb+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8mb4"
    conn = sqlalchemy.create_engine(string_con)

    return conn

def execute_query(con, query, **kwargs):
    query_fmt = query.format(**kwargs)
    con.execute(query_fmt)
    return True

def execute_multi_queries(con, query, **kwargs):
    query_fmt = query.format(**kwargs)
    for q in query_fmt.split(";")[:-1]:
        execute_query(con, q)
    return True

def create_spark_session():
    spark = ( SparkSession.builder
                      .appName("Dota Spark")
                      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
                      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                      .getOrCreate()
    )
    return spark

def register_temp_view(spark, tb_path):

    view_name = "_".join(tb_path.split("/")[-2:])
    ( spark.read
           .format("parquet")
           .load(tb_path)
           .createOrReplaceTempView(view_name)
    )

    return view_name