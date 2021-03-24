import os
from pyspark.sql import SparkSession
import dotenv

dotenv.load_dotenv( dotenv.find_dotenv() )

spark = (SparkSession.builder
                     .appName("Spark fo Dota")
                     .getOrCreate())

raw_match_players = (spark.read
                          .format("parquet")
                          .load(os.path.join(os.getenv("DATA_RAW"), "tb_match_player")) 
                    )

raw_match_players.createOrReplaceTempView("raw_match_player")

query_path = os.path.join(os.getenv("BASE_DIR"), "dotaScience", "magic_wand", "select_with_datatypes.sql" )

with open( query_path ) as open_file:
    query = open_file.read()

( spark.sql(query)
       .repartition(1)
       .write
       .mode("overwrite")
       .format("parquet")
       .partitionBy("partition_year", "partition_month")
       .save( os.path.join(os.getenv("DATA_PROCEDED"), "tb_match_player")) )
