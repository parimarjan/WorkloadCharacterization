import os
import sys
import pandas as pd
import time
import psycopg2 as pg
import numpy as np

import argparse
import pdb
import uuid
import logging

NEW_NAME_FMT = "{INP}_{DATA_KIND}"

WORKLOADS = {}
WORKLOADS["stack"] = "scraping-queries/sqls"
WORKLOADS["tpcds"] = "data/tpcds/all/"
WORKLOADS["tpcds1G"] = "data/tpcds1/all/"
WORKLOADS["tpch"] = "data/tpch/all/"
WORKLOADS["tpch1"] = "data/tpch1/all/"
WORKLOADS["job"] = "data/job/all_job/sqls"
WORKLOADS["ceb"] = "data/ceb-all/sqls/"

ALIAS_TO_TABS = {}
ALIAS_TO_TABS["n"] = "name"
ALIAS_TO_TABS["n2"] = "name"
ALIAS_TO_TABS["mi"] = "movie_info"
ALIAS_TO_TABS["t"] = "title"

def read_flags():
    parser = argparse.ArgumentParser()

    parser.add_argument("--result_dir", type=str, required=False,
            default="./results")
    parser.add_argument("--inp_to_eval", type=str, required=False,
            default="n")

    parser.add_argument("--data_kind", type=str, required=False,
            default="", help="suffix for the table")

    parser.add_argument("--reps", type=int, required=False,
            default=3)

    parser.add_argument("--num_queries", type=int, required=False,
            default=-1)

    parser.add_argument("--workload", type=str, required=False,
            default="ceb")

    parser.add_argument("--db_name", type=str, required=False,
            default="imdb")
    parser.add_argument("--db_host", type=str, required=False,
            default="localhost")
    parser.add_argument("--user", type=str, required=False,
            default="ceb")
    # parser.add_argument("--user", type=str, required=False,
            # default="pari")
    parser.add_argument("--pwd", type=str, required=False,
            default="password")
    parser.add_argument("--port", type=int, required=False,
            default=5432)

    return parser.parse_args()


LOG_FMT = "{EHASH} - {INP} - {REP} --> {TIME}"

def run_filters(sqls, ehashes, inputs):
    exec_times = []
    con = pg.connect(user=args.user, host=args.db_host, port=args.port,
            password=args.pwd, database=args.db_name)
    cursor = con.cursor()

    rcs = []

    for ri in range(args.reps):
        for si, sql in enumerate(sqls):
            eh = ehashes[si]
            inp = inputs[si]

            if args.data_kind != "":
                table_name = ALIAS_TO_TABS[inp]
                new_tab = NEW_NAME_FMT.format(INP = inp,
                                              DATA_KIND = args.data_kind)
                sql = sql.replace(table_name, new_tab, 1)
                # print(sql)

            start = time.time()
            try:
                cursor.execute(sql)
            except Exception as e:
                logging.error(str(e))
                logging.error(sql)
                pdb.set_trace()

            output = cursor.fetchall()
            rc = output[0][0]

            exec_time = time.time() - start
            exec_times.append(exec_time)
            logline = LOG_FMT.format(EHASH=eh, INP = inp, REP = ri,
                    TIME = exec_time)
            logging.info(logline)

            if len(rcs) != len(sqls):
                rcs.append(rc)

    logging.info("Total execution time: {}".format(np.sum(exec_times)))

    return rcs

def qerr_func(ytrue, yhat):
    errors = np.maximum((ytrue / yhat), (yhat / ytrue))
    return errors

def main():

    filename = uuid.uuid4().hex + ".log"
    filename = os.path.join(args.result_dir, filename)
    # logging.basicConfig(filename=filename, filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    # handler = logging.StreamHandler(sys.stdout)
    # handler.setLevel(logging.INFO)

    file_handler = logging.FileHandler(filename=filename)
    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler.setLevel(logging.INFO)
    stdout_handler.setLevel(logging.INFO)

    handlers = [file_handler, stdout_handler]
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=handlers
    )

    QDIR = WORKLOADS[args.workload]
    DATADIR = os.path.join(QDIR, "dfs")
    EXPRFN = os.path.join(DATADIR, "expr_df.csv")
    exprdf = pd.read_csv(EXPRFN)
    inps = args.inp_to_eval.split(",")

    exprdf = exprdf[exprdf.input.isin(inps)]
    exprdf = exprdf[~exprdf["filtersql"].str.contains("like")]
    exprdf = exprdf[~exprdf["filtersql"].str.contains("LIKE")]

    if args.num_queries != -1 and args.num_queries < len(exprdf):
        exprdf = exprdf.head(args.num_queries)

    logging.info(str(args))
    logging.info("Number of sqls to evaluate: {}".format(len(exprdf)))

    sqls = exprdf["filtersql"].values
    ehashes = exprdf["exprhash"].values
    inputs = exprdf["input"].values
    truey = exprdf["RowCount"].values

    rcs = run_filters(sqls, ehashes, inputs)
    rcs = np.array(rcs)
    rcs = np.maximum(1, rcs)

    qerrs = qerr_func(truey, rcs)
    logging.info("QError, mean: {}, median: {}, 90p: {}".format(
        np.mean(qerrs), np.median(qerrs), np.percentile(qerrs, 90)))


    # TODO: evaluate cardinality accuracy
    for qi, rc in enumerate(rcs):
        if rc == 1:
            print(sqls[qi])

args = read_flags()
main()
