import os
import sys
import dotenv
import argparse

dotenv.load_dotenv(dotenv.find_dotenv())

sys.path.insert(0, os.getenv("DOTASCIENCE"))

from backpack import db

def create_abt( date_start, date_end, spark ):

    tb_book_player = os.path.join( os.getenv("CONTEXT"), "tb_book_player")
    view_book_player = db.register_temp_view(spark, tb_book_player)
    
    tb_match_player_path = os.path.join(os.getenv("RAW"), "tb_match_player")
    view_player_match  = db.register_temp_view(spark, tb_match_player_path)

    query_path = os.path.join(os.getenv("ORACLE"), "ml", "training", "etl_query.sql" )
    query = db.import_query( query_path )
    query = query.format( date_start=date_start, date_end=date_end )

    ( spark.sql(query)
           .repartition(1)
           .write
           .mode("overwrite")
           .format("parquet")
           .save(os.path.join(os.getenv("CONTEXT"), "tb_abt_oracle")))
    
    return True

def abt_to_csv(spark):

    (spark.read
          .format("parquet")
          .load( os.path.join(os.getenv('CONTEXT'), "tb_abt_oracle"))
          .repartition(1)
          .write
          .options(header=True)
          .mode("overwrite")
          .format("csv")
          .save(os.path.join(os.getenv('CONTEXT'), "tb_abt_oracle_csv")))

    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date_start", "-s", help="Data de inicio para extração")
    parser.add_argument("--date_end", "-e", help="Data de fim para extração")
    args = parser.parse_args()

    spark = db.create_spark_session()

    print("Criando tabela de ABT no banco de dados.\nIsso pode levar um tempo...")
    create_abt(args.date_start, args.date_end, spark)
    print("ok.")

    print("Salvando base em csv...")
    abt_to_csv(spark)
    print("Base salva em csv.")
