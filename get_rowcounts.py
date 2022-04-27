import pandas as pd
import psycopg2 as pg
import os
import pdb

# WK = "tpch1"
WK = "ceb"
# WK = "tpcds1G"
#WK = "tpcds"
#WK = "job"
#WK = "tpch"
#WK = "stack"

USER="pari"
DBHOST="localhost"
PORT=5433
PWD=""
DBNAME=WK

WORKLOADS = {}
WORKLOADS["stack"] = "scraping-queries/sqls"
WORKLOADS["tpcds"] = "data/tpcds/all/"
WORKLOADS["tpcds1G"] = "data/tpcds1/all/"
WORKLOADS["tpch"] = "data/tpch/all/"
WORKLOADS["tpch1"] = "data/tpch1/all/"
WORKLOADS["job"] = "data/job/all_job/sqls"
WORKLOADS["ceb"] = "data/ceb-all/sqls/"

QDIR = WORKLOADS[WK]
DATADIR = os.path.join(QDIR, "dfs")
EXPRFN = os.path.join(DATADIR, "expr_df.csv")
exprdf = pd.read_csv(EXPRFN)
# print(exprdf.keys())

def get_rowcount(sql):
    if sql == "X":
        return -1,-1

    con = pg.connect(user=USER, host=DBHOST, port=PORT,
            password=PWD, database=DBNAME)
    cursor = con.cursor()
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
        print(sql)
        return -1,-1

    output = cursor.fetchall()
    rc = output[0][0]

    totalsql = sql[0:sql.find("WHERE")]

    cursor.execute(totalsql)
    output = cursor.fetchall()
    total = output[0][0]

    return rc,total

def get_rowcounts(sqls):
    # TODO: parallel version of this
    rcs = []
    totals = []
    for si, sql in enumerate(sqls):
        rc,total = get_rowcount(sql)
        rcs.append(rc)
        totals.append(total)

    return rcs,totals

sqls = exprdf["filtersql"].values
print("going to execute ", len(sqls))
rowcs,totals = get_rowcounts(sqls)

### update estimates
exprdf["RowCount"] = rowcs
exprdf["InputCardinality"] = totals
tmp = exprdf[exprdf["RowCount"] == -1]
print("Number of failed expressions: ", len(tmp))
exprdf["Selectivity"] = exprdf.apply(lambda x: float(x["RowCount"]) / x["InputCardinality"] ,axis=1)

tmp2 = exprdf[exprdf["RowCount"] != -1]
print(tmp2["Selectivity"].describe(percentiles=[0.75, 0.5, 0.9, 0.99]))

exprdf.to_csv(EXPRFN, index=False)
pdb.set_trace()
