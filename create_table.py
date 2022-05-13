import argparse
import pandas as pd
import numpy as np

import psycopg2 as pg
import os
import pdb
from collections import defaultdict
import sqlparse
import time
import random
import copy
import uuid
import logging
from sqlalchemy import create_engine

ALIAS_TO_TABS = {}
ALIAS_TO_TABS["n"] = "name"
ALIAS_TO_TABS["n2"] = "name"
ALIAS_TO_TABS["mi"] = "movie_info"
ALIAS_TO_TABS["t"] = "title"

CREATE_TEMPLATE = "CREATE TABLE {TABLE_NAME} AS {SEL_SQL}"

DROP_TEMPLATE = "DROP TABLE IF EXISTS {TABLE_NAME}"
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

    # parser.add_argument("--shuffle", type=int, required=False,
            # default=0, help="")

    return parser.parse_args()

def main():
    # assert os.path.exists(args.inp_fn)

    if os.path.exists(args.inp_fn):
        df = pd.read_csv(args.inp_fn)

        empty_cols = [col for col in df.columns if df[col].isnull().all()]
        print("Dropping columns: ", empty_cols)
        # Drop these columns from the dataframe
        df.drop(empty_cols,
                axis=1,
                inplace=True)

        df = df.drop(columns="Unnamed: 0")

    basetable = os.path.basename(args.inp_fn).replace(".csv", "")
    pg_orig_table = ALIAS_TO_TABS[basetable]

    new_table_name = NEW_NAME_FMT.format(INP = basetable,
                                     DATA_KIND = args.data_kind)
    drop_sql = DROP_TEMPLATE.format(TABLE_NAME = new_table_name)
    con = pg.connect(user=USER, host=DBHOST, port=PORT,
            password=PWD, database=DBNAME)

    cursor = con.cursor()
    cursor.execute(drop_sql)
    con.commit()

    cursor.close()
    con.close()

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
    elif args.data_kind == "random_domain":
        # update each column to be from same domain but randomly chosen
        newdf = copy.deepcopy(df)
        for key in df.keys():
            domain = list(set(df[key].dropna()))
            newdf[key] = np.array([random.choice(domain) for _ in range(len(df))])

        df = newdf
        engine_cmd = ENGINE_CMD_FMT.format(USER = USER,
                                           PWD = PWD,
                                           HOST = DBHOST,
                                           PORT = PORT,
                                           DB = DBNAME)

        engine = create_engine(engine_cmd)
        df.to_sql(new_table_name, engine)

    elif args.data_kind == "random_domain2":
        # update each column to be from same domain but randomly chosen
        # newdf = copy.deepcopy(df)

        for key in df.keys():
            domain = list(set(df[key].dropna()))
            domain.append(None)
            df[key] = np.array([random.choice(domain) for _ in range(len(df))])

        # df = newdf
        print(df.head(30))
        engine_cmd = ENGINE_CMD_FMT.format(USER = USER,
                                           PWD = PWD,
                                           HOST = DBHOST,
                                           PORT = PORT,
                                           DB = DBNAME)

        engine = create_engine(engine_cmd)
        df.to_sql(new_table_name, engine)

    elif args.data_kind == "random_domain3":

        engine_cmd = ENGINE_CMD_FMT.format(USER = USER,
                                           PWD = PWD,
                                           HOST = DBHOST,
                                           PORT = PORT,
                                           DB = DBNAME)

        engine = create_engine(engine_cmd)
        df = pd.read_sql_query('select * from {}'.format(pg_orig_table),con=engine)
        print(df.keys())

        for key in df.keys():
            domain = list(set(df[key].dropna()))
            domain.append(None)
            df[key] = np.array([random.choice(domain) for _ in range(len(df))])

        print(df.head(30))
        # engine_cmd = ENGINE_CMD_FMT.format(USER = USER,
                                           # PWD = PWD,
                                           # HOST = DBHOST,
                                           # PORT = PORT,
                                           # DB = DBNAME)

        # engine = create_engine(engine_cmd)
        df.to_sql(new_table_name, engine)

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