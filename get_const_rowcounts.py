import pandas as pd
import psycopg2 as pg
import os
import pdb
from collections import defaultdict
import sqlparse
import time

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

QDIR = WORKLOADS[WK]
DATADIR = os.path.join(QDIR, "dfs")
# EXPRFN = os.path.join(DATADIR, "expr_df.csv")
# exprdf = pd.read_csv(EXPRFN)
# print(exprdf.keys())

EXPRFN = os.path.join(DATADIR, "expr_df.csv")
exprdf = pd.read_csv(EXPRFN)

LITERAL_DATA_FN = os.path.join(DATADIR, "literal_df.csv")

CARDCACHE = {}

def get_total(sql):
    if sql == "X":
        return -1,-1

    con = pg.connect(user=USER, host=DBHOST, port=PORT,
            password=PWD, database=DBNAME)
    cursor = con.cursor()

    totalsql = sql[0:sql.find("WHERE")]
    cursor.execute(totalsql)
    output = cursor.fetchall()
    total = output[0][0]

    return total

MII2_FIXED = "mii2.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND mii2.info::float"
MII1_FIXED = "mii1.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND mii1.info::float"
MII_FIXED = "mii.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND mii.info::float"

def get_rowcount(sql):
    global CARDCACHE
    if sql == "X":
        return -1,-1
    # mii2 WHERE mii2.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND

    if "mii2.info::float" in sql:
        sql = sql.replace("mii2.info::float", MII2_FIXED)
    elif "mii1.info::float" in sql:
        sql = sql.replace("mii1.info::float", MII1_FIXED)
    elif "mii.info::float" in sql:
        sql = sql.replace("mii.info::float", MII_FIXED)

    if sql in CARDCACHE:
        return CARDCACHE[sql]

    con = pg.connect(user=USER, host=DBHOST, port=PORT,
            password=PWD, database=DBNAME)
    cursor = con.cursor()
    try:
        cursor.execute(sql)
    except Exception as e:
        # print("Exception!")
        print(e)
        print(sql)
        # if "mii" not in sql:
        pdb.set_trace()
        return -1,-1

    output = cursor.fetchall()
    rc = output[0][0]

    CARDCACHE[sql] = rc
    return rc

def parse_filter(sql):
    coldata = defaultdict(list)
    sql = sql[sql.find("WHERE")+5:]
    ands = sql.split("AND")

    for curexpr in ands:
        curexpr = curexpr.strip()
        if "between" in curexpr.lower():
            print("skipping between")
            continue

        if " in " in curexpr.lower():
            colname = curexpr[0:curexpr.lower().find(" ")]
            ## old code
            # colvalstr = curexpr[curexpr.find("(")+1:]
            # colvalstr = colvalstr.replace(")", "", 1)
            # colvals = colvalstr.split(",")

            # for cv in colvals:
                # if "'" not in cv:
                    # print(cv)
                    # pdb.set_trace()
                # coldata[colname].append(" = " + cv)

            sqp = sqlparse.parse(curexpr)
            sqp = sqp[0][0]

            par_token = None
            for idx, token in enumerate(sqp):
                if "Parenthesis" in str(type(token)):
                    par_token = token
                    break

            assert par_token is not None
            for token in par_token:
                if "Identifier" in str(type(token)):
                    # print(dir(token))
                    # pdb.set_trace()
                    # allvals = token.value.split(",")
                    # print(allvals)
                    flats = token.flatten()
                    for val in flats:
                        val = str(val)
                        if val == "," or val == " ":
                            continue
                        # print(val)
                        coldata[colname].append(" = " + val)

            continue

        if len(curexpr.replace(" ", "")) == 0:
            continue

        sqp = sqlparse.parse(curexpr)
        sqp = sqp[0][0]

        colname, literal = None,None
        op = None

        for idx, token in enumerate(sqp):
            if "Identifier" in str(type(token)):
                colname = str(token)

            elif "Comparison" in str(token.ttype):
                op = str(token)

            elif "Literal" in str(token.ttype):
                literal = str(token)

            elif "Function" in str(type(token)) or \
                    "Operation" in str(type(token)):
                literal = str(token)

        if literal is None:
            print("literal None!")
            print(token)
            # continue
            pdb.set_trace()

        if op is None:
            print("op None")
            pdb.set_trace()

        if colname is None:
            print("colname None")
            print(str(sqp))
            pdb.set_trace()

        coldata[colname].append(op + " " + literal)

    return coldata

def get_rowcounts_consts(sqls, exprhashes, inputs, jobids):
    countdata = defaultdict(list)

    for si, sql in enumerate(sqls):
        if si % 100 == 0:
            print(si)
        coldata = parse_filter(sql)
        ehash = exprhashes[si]
        expr_inp = inputs[si]
        expr_jid = jobids[si]
        # per column-literal pair
        # per column,column - literal,literal pairs
        for curcol, colvals in coldata.items():
            newsql_start = sql[0:sql.find("WHERE")-1]
            col_total_sql = newsql_start
            totalc = get_rowcount(col_total_sql)

            newsql_start += " WHERE " + curcol + " "

            for cv in colvals:
                newsql = newsql_start + cv
                rc = get_rowcount(newsql)
                countdata["exprhash"].append(ehash)
                countdata["RowCount"].append(rc)
                countdata["InputCardinality"].append(totalc)
                countdata["RowSql"].append(newsql)
                countdata["Column"].append(curcol)
                countdata["input"].append(expr_inp)
                countdata["jobid"].append(expr_jid)

                cv = cv.strip()
                op = cv[0:cv.find(" ")]
                colval = cv[cv.find(" ")+1:]
                countdata["Op"].append(op)
                countdata["Value"].append(colval)

        # combining colvals from two columns
        seencols = set()
        for curcol, colvals in coldata.items():
            for curcol2, colvals2 in coldata.items():
                if curcol == curcol2:
                    continue
                allcols = [curcol,curcol2]
                allcols = str(allcols.sort())
                if allcols in seencols:
                    continue
                seencols.add(allcols)

                col_total_sql = sql[0:sql.find("WHERE")-1]
                totalc = get_rowcount(col_total_sql)

                for cv in colvals:
                    newsql_start = sql[0:sql.find("WHERE")-1]

                    newsql_start += " WHERE " + curcol + " " + cv

                    for cv2 in colvals2:
                        newsql = newsql_start + " AND " + curcol2 + " " + cv2
                        rc = get_rowcount(newsql)

                        countdata["exprhash"].append(ehash)
                        countdata["RowCount"].append(rc)
                        countdata["InputCardinality"].append(totalc)
                        countdata["RowSql"].append(newsql)
                        countdata["Column"].append(curcol + "|" + curcol2)
                        countdata["input"].append(expr_inp)
                        countdata["jobid"].append(expr_jid)

                        cv = cv.strip()
                        op = cv[0:cv.find(" ")]
                        colval = cv[cv.find(" ")+1:]

                        cv2 = cv2.strip()
                        op2 = cv2[0:cv2.find(" ")]
                        colval2 = cv2[cv2.find(" ")+1:]

                        countdata["Op"].append(op + "|" + op2)
                        countdata["Value"].append(colval + "|" + colval2)

    countdf = pd.DataFrame(countdata)
    return countdf

sqls = exprdf["filtersql"].values
jobids = exprdf["jobid"].values
inputs = exprdf["input"].values
aliases = exprdf["alias"].values
exprhashes = exprdf["exprhash"].values

## for each sql expression, divide into each AND operator, and each IN (...)
print(len(sqls))

start = time.time()
rdata = get_rowcounts_consts(sqls, exprhashes, inputs, jobids)

print("took: ", time.time()-start)

rdata["Selectivity"] = rdata.apply(lambda x: float(x["RowCount"]) / x["InputCardinality"] ,axis=1)
tmp2 = rdata[rdata["RowCount"] != -1]
print(tmp2["Selectivity"].describe(percentiles=[0.75, 0.5, 0.9, 0.99]))
print(rdata.keys())

print(LITERAL_DATA_FN)
rdata.to_csv(LITERAL_DATA_FN, index=False)

pdb.set_trace()

