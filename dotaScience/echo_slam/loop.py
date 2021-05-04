import argparse
import os
import datetime
import sys

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

sys.path.insert(0, os.getenv("DOTASCIENCE"))

from echo_slam import run

def main(start, stop):

    start_dt = datetime.datetime.strptime(start, "%Y-%m-%d")
    stop_dt = datetime.datetime.strptime(stop, "%Y-%m-%d")

    while start_dt <= stop_dt:
        print(start)
        run.run(start)
        start_dt += datetime.timedelta(days=1)
        start = datetime.datetime.strftime(start_dt, "%Y-%m-%d")
        
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="Data de início da extração", default="2021-01-01")
    parser.add_argument("--stop", help="Data de fim da extração", default=None)

    args = parser.parse_args()

    start = args.start
    stop = args.stop if args.stop is not None else start

    main(start, stop)
