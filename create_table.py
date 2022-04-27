import argparse
import pandas as pd

import psycopg2 as pg
import os
import pdb
from collections import defaultdict
import sqlparse
import time

from sqlalchemy import create_engine

ALIAS_TO_TABS = {}
ALIAS_TO_TABS["n"] = "name"
ALIAS_TO_TABS["mi"] = "movie_info"
ALIAS_TO_TABS["t"] = "title"

CREATE_TEMPLATE = "CREATE TABLE {TABLE_NAME} AS {SEL_SQL}"

NEW_NAME_FMT = "{INP}_{DATA_KIND}"
ENGINE_CMD_FMT = """postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}"""

# WK = "tpch1"
WK = "ceb"
#WK = "tpcds1G"
#WK = "tpcds"
#WK = "job"
#WK = "tpch"
#WK = "stack"

if WK in ["ceb", "job"]:
    DBNAME="imdb"
else:
    DBNAME = WK

USER="ceb"
DBHOST="localhost"
PORT=5432
PWD="password"

WORKLOADS = {}
WORKLOADS["stack"] = "scraping-queries/sqls"
WORKLOADS["tpcds"] = "data/tpcds/all/"
WORKLOADS["tpcds1G"] = "data/tpcds1/all/"
WORKLOADS["tpch"] = "data/tpch/all/"
WORKLOADS["tpch1"] = "data/tpch1/all/"
WORKLOADS["job"] = "data/job/all_job/sqls"
WORKLOADS["ceb"] = "data/ceb-all/sqls/"


def read_flags():
    parser = argparse.ArgumentParser()

    parser.add_argument("--inp_fn", type=str, required=False,
            default="./data/new_data/n.csv")

    parser.add_argument("--data_kind", type=str, required=False,
            default="test", help="suffix for the table")

    parser.add_argument("--shuffle", type=int, required=False,
            default=0, help="")

    return parser.parse_args()

def main():
    assert os.path.exists(args.inp_fn)
    # df = pd.read_csv(args.inp_fn)
    # basetable = os.path.basename(args.inp_fn).replace(".csv", "")
    # new_table_name = NEW_NAME_FMT.format(INP = basetable,
                                     # DATA_KIND = args.data_kind)

    # print(new_table_name)
    # con = pg.connect(user=USER, host=DBHOST, port=PORT,
            # password=PWD, database=DBNAME)
    # cursor = con.cursor()

    # f = open(args.inp_fn, "r")
    # cursor.copy_from(f, new_table_name, sep=',')
    # f.close()

    # con.close()
    # cursor.close()

    df = pd.read_csv(args.inp_fn)

    empty_cols = [col for col in df.columns if df[col].isnull().all()]
    print("Dropping columns: ", empty_cols)
    # Drop these columns from the dataframe
    df.drop(empty_cols,
            axis=1,
            inplace=True)

    df = df.drop(columns="Unnamed: 0")

    basetable = os.path.basename(args.inp_fn).replace(".csv", "")

    new_table_name = NEW_NAME_FMT.format(INP = basetable,
                                     DATA_KIND = args.data_kind)

    if "shuffle" in args.data_kind:
        df = df.sample(frac=1.0)

    if args.data_kind == "true_cols":
        inp_table = ALIAS_TO_TABS[basetable]
        cols = ",".join(list(df.keys()))
        cols = "id," + cols
        sel_sql = "SELECT {} FROM {}".format(cols, inp_table)
        print(sel_sql)
        create_sql = CREATE_TEMPLATE.format(TABLE_NAME = new_table_name,
                                            SEL_SQL = sel_sql)
        print(create_sql)
        con = pg.connect(user=USER, host=DBHOST, port=PORT,
                password=PWD, database=DBNAME)

        cursor = con.cursor()
        cursor.execute(create_sql)
        con.commit()

        cursor.close()
        con.close()
    else:
        engine_cmd = ENGINE_CMD_FMT.format(USER = USER,
                                           PWD = PWD,
                                           HOST = DBHOST,
                                           PORT = PORT,
                                           DB = DBNAME)

        engine = create_engine(engine_cmd)
        df.to_sql(new_table_name, engine)


args = read_flags()
main()
