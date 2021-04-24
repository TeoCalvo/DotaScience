import os
import dotenv
import sys

dotenv.load_dotenv(dotenv.find_dotenv())

sys.path.insert(0, os.getenv("DOTASCIENCE"))

from backpack import utils as bpu
from backpack import db

CONFIG = bpu.import_toml()

def create_match_player_view(spark):
    raw_match_players = (spark.read
                            .format("parquet")
                            .load(os.path.join( CONFIG["path"]["raw"], "tb_match_player")) 
                        )

    raw_match_players.createOrReplaceTempView("raw_match_player")

def make_proceeded(spark):
    query_path = os.path.join(os.getenv("MAGIC_WAND"), "select_with_datatypes.sql" )
    query = db.import_query(query_path)

    ( spark.sql(query)
        .repartition(1)
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("partition_year", "partition_month")
        .save( os.path.join( os.getenv("PROCEEDED"), "tb_match_player")) )

def main():
    
    spark = db.create_spark_session()
    create_match_player_view(spark)
    make_proceeded(spark)

if __name__ == "__main__":
    main()