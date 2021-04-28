import os
import dotenv
import sys

dotenv.load_dotenv(dotenv.find_dotenv())

sys.path.insert(0, os.getenv("DOTASCIENCE"))

from backpack import db
from backpack import utils as bu

spark = db.create_spark_session()

df = ( spark.read
            .format("parquet")
            .load( os.path.join(os.getenv("RAW"), "tb_match_player") )
            .collect()
)

( df.repartition(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("match_year", "match_month")
    .save(os.path.join(os.getenv("RAW") , "tb_match_player"))
)