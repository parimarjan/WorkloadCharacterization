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

CREATE_INDEX_FMT = """CREATE INDEX {IDX_NAME} ON {TABLE_NAME} ({COL_NAME});"""
CREATE_TABLE_FMT = """CREATE TABLE {TABLE_NAME} ({COL_NAME} numeric);"""
# COPY_CSV_FMT = """copy {TABLE_NAME} FROM '{PATH}' WITH (FORMAT csv);"""
COPY_CSV_FMT = """psql -d {DB} -U {U} -h localhost -p {PORT} -c "\COPY {TAB} FROM {CSVPATH} WITH (FORMAT csv)";"""

DROP_TEMPLATE = "DROP TABLE IF EXISTS {TABLE_NAME}"
# ENGINE_CMD_FMT = """postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}"""

WKS = ["books_200M_uint64", "fb_200M_uint64", "lognormal_200M_uint64",
        # "normal_200M_uint64",
        "osm_cellids_200M_uint64",
        "wiki_ts_200M_uint64"
        ]

SOSD_DIR = "/spinning/pari/SOSD/data/"

QFN_TMP1 = "{}_equality_lookups_1M"
QFN_TMP2 = "{}_equality_lookups_10M"

USER="ceb"
DBHOST="localhost"
PORT=5434
PWD="password"
DBNAME="sosd"


for wk in WKS:
    con = pg.connect(user=USER, host=DBHOST, port=PORT,
            password=PWD, database=DBNAME)
    cursor = con.cursor()

    fn = os.path.join(SOSD_DIR, wk)
    csv_name = fn + ".csv"

    if not os.path.exists(csv_name):
        # assert False
        data = np.fromfile(fn, dtype=np.uint64)
        print(wk, len(data))
        # ignoring first number which seems to be size
        data = data[1:-1]
        np.savetxt(csv_name, data, fmt='%i', delimiter=",")


    create_table_sql = CREATE_TABLE_FMT.format(TABLE_NAME = wk,
                                               COL_NAME = wk)
    print(create_table_sql)
    cursor.execute(create_table_sql)
    con.commit()

    os.environ["PGPASSWORD"] = "password"
    copy_shell = COPY_CSV_FMT.format(DB = DBNAME,
                                     U = USER,
                                     PORT = PORT,
                                     TAB = wk,
                                     CSVPATH = csv_name)
    print(copy_shell)
    p = sp.Popen(copy_shell, shell=True)
    p.wait()


    index_name = "idx_" + wk
    create_index_sql = CREATE_INDEX_FMT.format(TABLE_NAME = wk,
                                               IDX_NAME = index_name,
                                               COL_NAME = wk)
    print(create_index_sql)

    try:
        cursor.execute(create_index_sql)
    except Exception as e:
        print(e)

    con.commit()
    con.close()
    cursor.close()
