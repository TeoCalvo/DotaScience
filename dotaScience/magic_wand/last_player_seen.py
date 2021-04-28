import os
import sys

import dotenv

dotenv.load_dotenv( dotenv.find_dotenv() )

sys.path.insert(0, os.getenv("DOTASCIENCE"))

from backpack import db
from backpack import utils as butils

con = db.connect_maria_aws()

spark = db.create_spark_session()

query_path = os.path.join(os.getenv("MAGIC_WAND"), "last_player_seen.sql")
query = db.import_query(query_path)

tb_path = os.path.join(os.getenv("CONTEXT"), "tb_book_player")
name = db.register_temp_view( spark, tb_path)

df = spark.sql(query.format(table = name)).toPandas()

df = df.set_index("account_id")

df.to_sql( "tb_last_player_seen", con, 
                                schema="teomewhy",
                                index=True,
                                if_exists="replace" )

print(df)