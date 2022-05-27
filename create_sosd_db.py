import argparse
import pandas as pd
import numpy as np
import string

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

CREATE_INDEX_FMT = """CREATE INDEX ON {TABLE_NAME} ({COL_NAME});"""
CREATE_TABLE_FMT = """CREATE TABLE {TABLE_NAME} ({COL_NAME} numeric);"""
COPY_CSV_FMT = """copy {TABLE_NAME} FROM '{PATH}' WITH (FORMAT csv);"""

DROP_TEMPLATE = "DROP TABLE IF EXISTS {TABLE_NAME}"
# ENGINE_CMD_FMT = """postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}"""

# WKS = ["wiki_ts_200M_uint32"]
WKS = ["books_200M_uint64", "fb_200M_uint64", "lognormal_200M_uint64",
        "normal_200M_uint64", "osm_cellids_200M_uint64",
        "uniform_dense_200M_uint64", "uniform_sparse_200M_uint64",
        "wiki_ts_200M_uint64"
        ]

# WKS = ["books_200M_uint32", "lognormal_200M_uint32",
        # "normal_200M_uint32",
        # "uniform_dense_200M_uint32",
        # "uniform_sparse_200M_uint32",
        # "wiki_ts_200M_uint32"
        # ]

SOSD_DIR = "/flash1/pari/SOSD/data/"

QFN_TMP1 = "{}_equality_lookups_1M"
QFN_TMP2 = "{}_equality_lookups_10M"

USER="ceb"
DBHOST="localhost"
PORT=5432
PWD="password"
DBNAME="sosd"

# engine_cmd = ENGINE_CMD_FMT.format(USER = USER,
                                   # PWD = PWD,
                                   # HOST = DBHOST,
                                   # PORT = PORT,
                                   # DB = DBNAME)
# # engine = create_engine(engine_cmd)

con = pg.connect(user=USER, host=DBHOST, port=PORT,
        password=PWD, database=DBNAME)
cursor = con.cursor()

for wk in WKS:
    fn = os.path.join(SOSD_DIR, wk)
    csv_name = fn + ".csv"
    copy_sql = COPY_CSV_FMT.format(TABLE_NAME = wk,
                                   PATH = csv_name)
    create_index_sql = CREATE_INDEX_FMT.format(TABLE_NAME = wk,
                                               COL_NAME = wk)
    print(create_index_sql)
    try:
        cursor.execute(create_index_sql)
    except Exception as e:
        print(e)

    continue

    if not os.path.exists(csv_name):
        assert False
        data = np.fromfile(fn, dtype=np.uint64)
        print(wk, len(data))
        # ignoring first number which seems to be size
        data = data[1:-1]

        np.savetxt(csv_name, data, fmt='%i', delimiter=",")

    drop_sql = DROP_TEMPLATE.format(TABLE_NAME=wk)
    # print(drop_sql)
    cursor.execute(drop_sql)

    create_table_sql = CREATE_TABLE_FMT.format(TABLE_NAME = wk,
                                               COL_NAME = wk)
    print(create_table_sql)
    cursor.execute(create_table_sql)

    # cursor.execute(copy_sql)
    print(copy_sql)

con.close()
cursor.close()
