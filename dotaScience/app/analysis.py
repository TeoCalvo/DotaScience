# %%

import dotenv
import os
import pandas as pd
import sys

dotenv.load_dotenv(dotenv.find_dotenv())

sys.path.insert(0, "../")

from backpack import db
