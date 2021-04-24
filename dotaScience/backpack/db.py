import os
import dotenv
import sqlalchemy
from pyspark.sql import SparkSession

def import_query(path):
    '''Importa uma determinada query'''
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

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
    spark = (SparkSession.builder
                         .appName("Spark for Dota")
                         .getOrCreate())
    return spark