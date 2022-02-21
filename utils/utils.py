import re
import dateutil.parser as dp
from collections import defaultdict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
import hashlib
import copy
import time
from multiprocessing import Pool, cpu_count
import random
import os, errno

import sqlparse

COUNT_TMP = "SELECT COUNT(*) from {TABLE} AS {ALIAS} WHERE {FILTER}"

def make_dir(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def deterministic_hash(string):
    return int(hashlib.sha1(str(string).encode("utf-8")).hexdigest(), 16)

def extract_values(obj, key):
    """Recursively pull values of specified key from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Return all matching values in an object."""

        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == key:
                    arr.append(v)
                #elif isinstance(v, (dict, list)):
                #    extract(v, arr, key)
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)

#                 if isinstance(v, dict):
#                     extract(v, arr, key)
#                 elif k == key:
#                     arr.append(v)

        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)

        return arr

    results = extract(obj, arr, key)
    return results

def column_info(row):
    expr = row[FILTER_FIELD].values[0]
    d = json.loads(expr)
    col_names = extract_values(d, "name")
    col_ops = extract_values(d, "expOperator")
    num_cols = len(col_names)

def is_int(num):
    try:
        int(num)
        return True
    except:
        return False


def is_num(val):
    try:
        float(val)
        return True
    except:
        return False

# import enchant
# enchantD = enchant.Dict("en_US")
import re

#import nltk
#nltk.download('words')
#from nltk.corpus import words as nltkwords

URLPAT = "((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*"

def get_like_kind(curop):
    '''
    kinds: prefix,suffix,infix;
    dtypes: path, words, word list / x_y, almost-words (?), serial-ids, num
    like_casting: toupper etc. if it has "to" and "( )";

    TODO: almost words dtype
    '''
    curop = curop.lower()
    likekind = "unknown"
    likecast = 0
    likedtype = "unknown"
    likecol = "unknown"

    if "upper" in curop or "lower" in curop or "invariant" in curop:
        likecast = 1

    if "like" in curop and "likeutil" not in curop:
        print("!!Like in Op, but LikeUtil not!!")
        print(curop)
        print("********************")

    if ".contains" in curop:
        likecol = curop[0:curop.find(".")]
        # print(curop)
        # print(likecol)
        curop = curop[curop.find("contains"):]
        opval = curop[curop.rfind("(")+1:-2]
        likekind = "contains"

    elif "likeutil" in curop:
        curop = curop[curop.find("like"):]
        likecol = curop[curop.find("(")+1:curop.find(",")]
        opval = curop[curop.find("\"")+1:]
        if "\"" not in opval:
            opval = curop[curop.find("\"")+1:]
        else:
            opval = opval[:opval.find("\"")]

        ## choosing opval, because curval could have additional likes with ||
        ## etc.
        if opval.count("%") == 0:
            likekind = "no%"
        elif opval.count("%") == 2:
            likekind = "contains"
        elif opval.count("%") == 1:
            if opval[0] == "%":
                likekind = "ends"
            elif opval[-1] == "%":
                likekind = "starts"
            else:
                likekind = "no%"

        elif opval.count("%") > 2:
            print("*******")
            print(curop)
            print("opval: ", opval)
            print("*******")
            likekind = "multi%"
        else:
            assert False

    elif "starts" in curop:
        likecol = curop[0:curop.find(".starts")]
        likekind = "starts"
        curop = curop[curop.find("starts"):]
        opval = curop[curop.find("\"")+1:curop.rfind("\"")]

    elif "ends" in curop:
        likecol = curop[0:curop.find(".ends")]
        likekind = "ends"
        curop = curop[curop.find("ends"):curop.find("h")]
        opval = curop[curop.find("\"")+1:curop.rfind("\"")]

    else:
        assert False

    opval = opval.replace("%", "")
    opval = opval.replace('"', '')
    opval = opval.replace("@", "")

    if is_num(opval):
        likedtype = "num"
    elif len(opval) <= 2:
        likedtype = "short"
    elif opval[0] == ".":
        likedtype = "extension"
    elif re.match(URLPAT, opval) is not None:
        if ("cosmos" in opval or "adl" in opval) and "/" in opval:
            likedtype = "path"
        else:
            likedtype = "url"

    elif opval.count("/") >= 2 or opval.count("\\") >= 2:
        likedtype = "path"

    # elif enchantD.check(opval):
    elif False:
        likedtype = "word"

    elif opval.count("-") >= 2 or \
            opval.count(":") >= 2:
        likedtype = "serial"
    elif "-" in opval or "_" in opval or " " in opval or "," in opval or ":" in opval:
        if "-" in opval:
            allvals = opval.split("-")
        elif "_" in opval:
            allvals = opval.split("_")
        elif " " in opval:
            allvals = opval.split(" ")
        elif "," in opval:
            allvals = opval.split(",")
        elif ":" in opval:
            allvals = opval.split(":")

        validwords = 0
        for v1 in allvals:
            if v1 == "":
                continue
            # if enchantD.check(v1):
            if False:
                validwords += 1
        if validwords >= 2:
            likedtype = "words"
    elif "0x" in opval:
        likedtype = "hex"
    else:
        validwords = 0
        start = 0
        prevstart = 0
        for oi,_ in enumerate(opval):
            cword = opval[start:oi+1]
            pword = opval[prevstart:oi+1]
            # if enchantD.check(cword) and len(cword) >= 3:
            if False:
                validwords += 1
                prevstart = start
                start = oi+1

            # extend previous word
            # elif enchantD.check(pword) and len(pword) >= 3:
            elif False:
                start = oi+1

        if validwords >= 2:
            likedtype = "words"

    return likecol, likekind, likedtype, likecast, curop, opval



def is_num(val):
    try:
        float(val)
        return True
    except:
        return False

def is_a_date(val):
    val = val.replace("'", "")
    if "date" in val:
        return True

    elif is_num(val):
        num = float(val)
        if num > 1900 and num < 2010:
            return True

    elif "+" in val:
        cvals = val[0:val.find("+")]
        if is_num(cvals):
            num = float(cvals)
            if num > 1900 and num < 2010:
                return True

    elif "-" in val:
        cvals = val[0:val.find("-")]
        if is_num(cvals):
            num = float(cvals)
            if num > 1900 and num < 2010:
                return True

    return False

def remove_ints(s):
    return ''.join([i for i in s if not i.isdigit()])

#URLPAT = "((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*"
def get_like_type(op, vals):
    likekind = "-1"
    likedtype = "-1"

    if "like" not in op.lower():
        return likedtype, likekind

    vals = vals.replace("'", "")
    vals = vals.replace("(", "")
    vals = vals.replace(")", "")

    if vals.count("%") == 0:
        likekind = "no%"
    elif vals.count("%") == 2:
        if vals[0] == "%" and vals[-1] == "%":
            likekind = "contains"
        else:
            likekind = "x%y%x"
    elif vals.count("%") == 1:
        if vals[0] == "%":
            likekind = "ends"
        elif vals[-1] == "%":
            likekind = "starts"
        else:
            likekind = "x%x"
    else:
        nlikes = vals.count("%")
        likekind = str(nlikes) + "%"

    opval = vals.replace("%", "")

    if is_num(opval):
        likedtype = "num"
    elif len(opval) <= 2:
        likedtype = "short"
    elif opval[0] == ".":
        likedtype = "extension"
    elif re.match(URLPAT, opval) is not None:
        if ("cosmos" in opval or "adl" in opval) and "/" in opval:
            likedtype = "path"
        else:
            likedtype = "url"

    elif opval.count("/") >= 2 or opval.count("\\") >= 2:
        likedtype = "path"

    #elif enchantD.check(opval):
    elif False:
        likedtype = "word"

    elif opval.count("-") >= 2 or \
            opval.count(":") >= 2:
        likedtype = "serial"
    elif "-" in opval or "_" in opval or " " in opval or "," in opval or ":" in opval:
        if "-" in opval:
            allvals = opval.split("-")
        elif "_" in opval:
            allvals = opval.split("_")
        elif " " in opval:
            allvals = opval.split(" ")
        elif "," in opval:
            allvals = opval.split(",")
        elif ":" in opval:
            allvals = opval.split(":")

        validwords = 0
        for v1 in allvals:
            if v1 == "":
                continue
            #if enchantD.check(v1):
            if False:
                validwords += 1
        if validwords >= 2:
            likedtype = "words"
    elif "0x" in opval:
        likedtype = "hex"
    else:
        validwords = 0
        start = 0
        prevstart = 0
        for oi,_ in enumerate(opval):
            cword = opval[start:oi+1]
            pword = opval[prevstart:oi+1]
            #if enchantD.check(cword) and len(cword) >= 3:
            if False:
                validwords += 1
                prevstart = start
                start = oi+1

            # extend previous word
            #elif enchantD.check(pword) and len(pword) >= 3:
            elif False:
                start = oi+1

        if validwords >= 2:
            likedtype = "words"
        else:
            likedtype = "unknown"

    return likedtype, likekind

def get_discrete_type_sql(op, vals):
    dtype = "-1"
    dkind = "-1"
    #op = op.lower()
    assert op != ""
    if op in ["=", "!=", "<>", "in"]:
        if op == "=":
            dkind = "eq"
        elif op in ["!=", "<>"]:
            #print("neq!!")
            dkind = "neq"
        else:
            dkind = "in"

        if "select" in vals:
            dtype = "sql"
        else:
            if "(" in vals:
                vals = vals.replace("(", "")
                vals = vals.replace(")", "")
                vals = vals.split(",")
                vals = vals[0]

            if is_a_date(vals):
                dtype = "date"
            elif is_num(vals):
                dtype = "num"
            else:
                dtype = "string"

    return dtype, dkind


def get_discrete_type(vals):
    if len(vals) == 0:
        return ""

    val = str(vals[0])
    val = val.replace("@", "")
    val = val.replace("\"", "")
    val = val.replace("\'", "")

    if val.lower() == "null" or val.lower() == "none" \
        or val.lower() == "na" or "empty" in val.lower() \
        or val == "":
        return "null"

    elif is_num(val) or "System.Date" in val:
        return "num"

#     elif "System.Date" in val:
#         return "date"

#     elif "empty" in val.lower() or val == "":
#         return "empty"

#     elif "System" in val:
#         return "system"
#    elif "System"
    elif val.lower() == "true" or val.lower() == "false":
        return "bool"

    else:
        #print(val)
        return "string"

def datetime_to_secs(val):
    # if "system.date" in val.lower():
    assert "system.date" in val.lower()
    t = val[val.find("/*")+2:val.find("*/")]
    parsed_t = dp.parse(t)

    try:
        t_in_seconds = parsed_t.timestamp()
    except Exception as e:
        if "0" in str(e):
            # return "date", 0.0
            return 0.0
        else:
            assert False
    return t_in_seconds

def get_cont_dtype_sql(val):
    def _get_op(val):
        if "+" in val:
            return "+"
        elif "-" in val:
            return "-"
        elif "/" in val:
            return "/"
        elif "*" in val:
            return "*"
        else:
            return None

    val = val.lower()
    if val[-1] == "d" or val[-1] == "f" \
            or val[-1] == "l":
        val = val[0:-1]

    op = _get_op(val)
    if op is not None:
        # opvals = val.split(op)
        # assert len(opvals) == 2
        # float(opvals[0])
        try:
            res = eval(val)
            # print("eval succeeded!")
            # print(res)
            return "float", float(res)
        except Exception as e:
            # print(e)
            # print("failed eval!")
            # print(val)
            # print(type(val))
            return "func", val

    # FIXME: needs a better characterization of date
    elif "date" in val:
        return "date", val

    elif is_int(val):
        val = int(val)
        return "int", val
    elif is_num(val):
        val = float(val)
        return "float", val
    elif "proto" in val.lower():
        return "protobuf", None
    else:
        if "-" in val:
            allvals = val.split("-")
            if len(allvals) == 3:
                return "date2", None
            else:
                return "str", None
        else:
            return "str", None

def get_cont_dtype(val):
    val = val.lower()
    if val[-1] == "d" or val[-1] == "f" \
            or val[-1] == "l":
        val = val[0:-1]

    if "system.date" in val:
        t = val[val.find("/*")+2:val.find("*/")]
        parsed_t = dp.parse(t)
        try:
            t_in_seconds = parsed_t.timestamp()
        except Exception as e:
            if "0" in str(e):
                return "date", 0.0
            else:
                assert False
        return "date", t_in_seconds

    elif is_int(val):
        val = int(val)
        return "int", val
    elif is_num(val):
        val = float(val)
        return "float", val
    elif "proto" in val.lower():
        return "protobuf", None
    else:
        if "-" in val:
            allvals = val.split("-")
            if len(allvals) == 3:
                return "date2", None
            else:
                return "str", None
        else:
            return "str", None

'''
< : 1/0
> : 1/0
(can be both);
< term: None or L number
> term:
difference: None OR < minus > term

What if multiple non contiguous ranges?? ---> 1/0
What if multiple columns with different ranges? ---> 1/0

float vs long vs int vs date vs str;

How will we tell whether moving window style filters OR sth else?
'''
def parse_cont_vals(ops, vals, col_names):

    # values to return: one for each column
    ## type: range, lt, gt
    ## dtype:
    ## range: None, or actual value
    ret = {}

    cont_idxs = []

    cur_comb_op = ""
    for oi, op in enumerate(ops):
        if op is None:
            continue

        if op == "And" or op == "Or":
            cur_comb_op = op

        if ">" in op or "<" in op:
            cont_idxs.append(oi)
            col = col_names[oi]
            val = vals[oi]
            if col == "":
                continue
            if val == "":
                continue

            if isinstance(val, list):
                if len(val) == 0:
                    continue
                assert len(val) == 1
                val = val[0]

            val = val.replace("@", "")
            val = val.replace("\"", "")
            val = val.replace("\'", "")

            if len(val) == 0:
                continue

            #dtype, pval = "-1", "-1"
            dtype, pval = get_cont_dtype(val)

            if col not in ret:
                ret[col] = {}
            ret[col]["dtype"] = dtype
            if pval is not None and ">" in op:
                ret[col]["gt"] = pval
            elif pval is not None and "<" in op:
                ret[col]["lt"] = pval

            ret[col]["comb_op"] = cur_comb_op

    for col in ret:
        if "comb_op" in ret[col] and ret[col]["comb_op"] == "Or":
            ctype = "discont"

        elif "lt" in ret[col] and "gt" in ret[col]:
            ctype = "range"
            #assert ret[col]["comb_op"] == "And"
            if "comb_op" not in ret[col]:
                print(vals)
                print(ops)
                assert False

            elif ret[col]["comb_op"] != "And":
                print(vals)
                print(ops)
                assert False

        elif "lt" in ret[col]:
            ctype = "lt"
        elif "gt" in ret[col]:
            ctype = "gt"
        else:
            ctype = "other"

        if "lt" in ret[col] and "gt" in ret[col]:
            crange = ret[col]["lt"] - ret[col]["gt"]
            if crange < 0:
                # if ret[col]["comb_op"] != "Or":
                    # print("******************")
                    # print(ops)
                    # print(vals)
                    # print(col)
                    # print(ret[col])
                    # print(crange)
                    # print(ret[col]["lt"], ret[col]["gt"])
                    # print("******************")
                # assert ret[col]["comb_op"] == "Or"
                ret[col]["range"] = crange
            else:
                ret[col]["range"] = crange

        ret[col]["cont_type"] = ctype

    return ret


def isalikeop(curop):
    curop = curop.lower()

    # if "like" in curop and "likeutil" not in curop:
        # print(curop)

    if "likeutil" in curop or "starts" in curop \
        or ".contains" in curop or "endswith" in curop:
        return True

    return False

def check_is_id_sql(colname, const):
    if ("_id" in colname.lower()) and is_int(const):
        is_id = 1
    else:
        is_id = 0
    return is_id

def check_is_id(colname, const):
    if ("Id" in colname or "ID" in colname) and is_int(const):
        is_id = 1
    else:
        is_id = 0
    return is_id

def handle_complex_ops(complex_op):
    '''
    FIXME: should not do lowercase until we have extracted column name, as
    otherwise, columname case might be lost?
    '''
    def _handle_complex_ops(complex_op):
        if "&&" in complex_op:
            allops = complex_op.split("&&")
            for op in allops:
                _handle_complex_ops(op)
        elif "||" in complex_op:
            allops = complex_op.split("||")
            for op in allops:
                _handle_complex_ops(op)
        else:
            # handle the single op
            if isalikeop(complex_op):
                likecol,likekind,likedtype,likecast,curop,likeconst = get_like_kind(complex_op)
                opvals["column2"].append(likecol)
                opvals["op"].append("LIKE")
                opvals["constant"].append(likeconst)
                opvals["opstring"].append(complex_op)
                opvals["dtype"].append(likedtype)
                opvals["optype"].append(likekind)
                assert likekind != "="
                is_id = check_is_id(likecol, likeconst)
                opvals["is_id"].append(is_id)

            elif "scopeinternalinoperator" in complex_op:
                optype = "="
                # TODO: check again
                if "not" in complex_op or "!" in complex_op:
                    optype = "!="

                inexpr = complex_op[complex_op.find("inoperator"):]
                incolumn = inexpr[inexpr.find("("):inexpr.find(",")]
                invals = inexpr[inexpr.find(","):]
                invals = invals[1:invals.find(")")]
                child_vals = invals.split(",")
                num_discrete_const = len(child_vals)
                dtype = get_discrete_type(child_vals[0])
                is_id = check_is_id(incolumn, child_vals[0])

                for cv in child_vals:
                    opvals["column2"].append(incolumn)
                    opvals["op"].append(optype)
                    opvals["constant"].append(cv)
                    opvals["opstring"].append(inexpr)
                    opvals["dtype"].append(dtype)
                    opvals["optype"].append(optype)
                    opvals["is_id"].append(is_id)

            elif "regex" in complex_op:
                pass

            elif "==" in complex_op or "!=" in complex_op:
                if "null" in complex_op:
                    return

                # ignore cast etc.
                if "==" in complex_op:
                    cmpop = "="
                else:
                    cmpop = "!="

                col = complex_op[0:complex_op.find(cmpop)]
                const = complex_op[complex_op.find(cmpop)+2:]
                if "." in col:
                    col = col[0:col.find(".")]
                col = col.replace("(", "")
                is_id = check_is_id(col, const)

                opvals["column2"].append(col)
                opvals["op"].append(cmpop)
                opvals["constant"].append(const)
                opvals["opstring"].append(complex_op)
                opvals["dtype"].append(get_discrete_type([const]))
                opvals["optype"].append(cmpop)
                opvals["is_id"].append(is_id)

            elif ">" in complex_op or "<" in complex_op:
                pass
                # print(complex_op)
            else:
                # ignoring these
                return

    complex_op = complex_op.lower()
    opvals = defaultdict(list)
    # likeinfo = defaultdict(list)

    _handle_complex_ops(complex_op)
    return opvals

# def parse_filter_exprs(df, INP_FIELD, FILTER_FIELD):

    # # def handle_likeop(curop):
        # # curop = curop.lower()
        # # if "LIKE" in inp_to_op_kind[inp]:
            # # inp_to_op_kind[inp]["LIKE"] += 1
        # # else:
            # # inp_to_op_kind[inp]["LIKE"] = 1
        # # assert len(fvs) == 0
        # # # typeall += "like,"
        # # likecol, likekind, likedtype, likecast, curop, likeconst = get_like_kind(curop)

        # # likeop = 1
        # # if likeconst in inp_to_like_consts[inp]:
            # # inp_to_like_consts[inp][likeconst] += 1
        # # else:
            # # inp_to_like_consts[inp][likeconst] = 1

        # # return likeop, likekind, likedtype,likecast,curop,likeconst
    # # cur_row = None

    # inp_to_filter_cols = defaultdict(set)
    # inp_to_pcols = defaultdict(set)
    # inp_to_all_cols = defaultdict(set)

    # inp_to_ops = defaultdict(set)
    # inp_to_num_cols = defaultdict(list)

    # inp_to_discrete_consts = defaultdict(dict)
    # inp_to_continuous_consts = defaultdict(dict)
    # inp_to_op_kind = defaultdict(dict)
    # inp_to_in_consts = defaultdict(dict)
    # inp_to_like_consts = defaultdict(dict)

    # num_ops_all = []
    # num_filter_cols_all = []
    # num_cols_all = []
    # num_cols_sel = []
    # num_unique_ops_all = []
    # num_pcols = []

    # like_ops = []
    # like_lens = []
    # like_dtype = []
    # like_kind = []
    # like_casting = []
    # like_const = []

    # discrete_ops = []
    # discrete_eqs = []
    # discrete_noneqs = []
    # discrete_types = []
    # discrete_types_all = []
    # types_all = []

    # nullchecks = []

    # cont_ops = []
    # cont_dates = []
    # cont_others = []

    # cont_types = []
    # cont_dtypes = []
    # cont_ranges = []
    # cont_cols = []

    # complex_ops = []
    # complex_ops_num = []

    # udf_ops = []
    # in_ops = []
    # regex_ops = []
    # equal_dates = []
    # num_discrete_consts = []
    # num_like_cols = []
    # num_likes = []

    # num_err = 0

    # for idx, row in df.iterrows():
        # cur_row = row
        # expr = row[FILTER_FIELD]
        # # inpcols_all = row["inputColumns"]
        # # inp_sel = row["inputSelected"]
        # inp = row[INP_FIELD]
        # try:
            # d = json.loads(expr)
        # except:
            # num_err += 1
            # num_ops_all.append(-1)
            # #num_unique_cols_all.append(-1)
            # num_filter_cols_all.append(-1)
            # num_unique_ops_all.append(-1)
            # num_cols_all.append(-1)
            # num_cols_sel.append(-1)
            # num_pcols.append(-1)
            # like_ops.append(-1)
            # discrete_ops.append(-1)
            # discrete_eqs.append(-1)
            # discrete_noneqs.append(-1)
            # cont_ops.append(-1)
            # cont_dates.append(-1)
            # cont_others.append(-1)
            # udf_ops.append(-1)
            # in_ops.append(-1)
            # regex_ops.append(-1)
            # equal_dates.append(-1)
            # complex_ops.append(-1)
            # complex_ops_num.append(-1)
            # #discrete_consts.append(-1)
            # discrete_types.append(-1)
            # discrete_types_all.append(-1)
            # num_discrete_consts.append(-1)
            # nullchecks.append(-1)
            # types_all.append(-1)
            # cont_types.append(-1)
            # cont_dtypes.append(-1)
            # cont_ranges.append(-1)
            # like_lens.append(-1)
            # cont_cols.append(-1)
            # like_dtype.append(-1)
            # like_kind.append(-1)
            # like_casting.append(-1)
            # like_const.append(-1)
            # num_like_cols.append(-1)
            # num_likes.append(-1)
            # continue

        # # parse d FOR cosntant values
        # filter_values = extract_values(d, "values")
        # ops = extract_values(d, "expOperator")
        # children = extract_values(d, "children")
        # col_names = extract_values(d, "name")

        # likeop = 0
        # likelen = 0
        # numlikes = 0
        # numlikecols = 0

        # udfop = 0
        # inop = 0
        # regexop = 0
        # discreteop = 0
        # discreteeq = 0
        # discretenoneq = 0
        # contop = 0
        # contdate = 0
        # contother = 0
        # equaldate = 0
        # num_discrete_const = 0

        # complexpred = 0
        # complexpredlen = 0
        # unknown = 0

        # nullcheck = 0

        # typeall = ""
        # discrete_type = ""
        # discrete_type_all = ""

        # cont_type = ""
        # cont_dtype = ""
        # contrange = 0.0
        # num_cont_cols = 0

        # likekind = ""
        # likedtype = ""
        # likecast = 0
        # likeconst = ""

        # curfilterdf = defaultdict(list)

        # for fi, fvs in enumerate(filter_values):
            # if ops[fi] is None:
                # continue
            # if ops[fi] == "Or":
                # # probably IN
                # if "IN" in inp_to_op_kind[inp]:
                    # inp_to_op_kind[inp]["IN"] += 1
                # else:
                    # inp_to_op_kind[inp]["IN"] = 1
                # inop = 1

                # # TODO: can be a mix of >= and =; handle case.
                # child_vals = extract_values(children[fi], "values")
                # child_cols = extract_values(children[fi], "name")

                # child_vals = [c[0] for c in child_vals if (len(c) > 0 \
                                        # and "System.DateTime" not in c[0])]
                # num_discrete_const = len(child_vals)
                # child_vals.sort()
                # child_vals = str(child_vals)
                # if child_vals in inp_to_in_consts[inp]:
                    # inp_to_in_consts[inp][child_vals] += 1
                # else:
                    # inp_to_in_consts[inp][child_vals] = 1
                # continue

            # # TODO: And + =,!= combination on same column? seems to be rare;
            # # and usual case is handled when we encounter them later.
            # if ops[fi] == "And":
                # continue

            # if "InOperator" in ops[fi] or \
                    # "string" in ops[fi].lower() and "in" in ops[fi].lower():

                # if "IN" in inp_to_op_kind[inp]:
                    # inp_to_op_kind[inp]["IN"] += 1
                # else:
                    # inp_to_op_kind[inp]["IN"] = 1
                # inop = 1
                # inexpr = ops[fi][ops[fi].find("InOperator"):]
                # incolumn = inexpr[inexpr.find("("):inexpr.find(",")]
                # invals = inexpr[inexpr.find(","):]
                # invals = invals[1:invals.find(")")]
                # child_vals = invals.split(",")
                # num_discrete_const = len(child_vals)

                # for cv in child_vals:
                    # curfilterdf["column"].append(incolumn)
                    # curfilterdf["op"].append("=")
                    # curfilterdf["constant"].append(cv)
                    # curfilterdf["opstring"].append(inexpr)

                # # child_vals.sort()
                # # childstr = str(child_vals)
                # # if childstr in inp_to_in_consts[inp]:
                    # # inp_to_in_consts[inp][childstr] += 1
                # # else:
                    # # inp_to_in_consts[inp][childstr] = 1

                # # no continue here because we could have been dealing with
                # # other operators as well connected by && etc.
            # if "regex" in ops[fi].lower():
                # regexop = 1
                # # again, no continue because it could havwe been linked
                # # together with && etc.

            # # TODO: complex predicates
            # # potential large classes:
            # ## regex-matches;
            # # like: contains, endswith etc. ---> just parse them out and
            # # analyze them too
            # ## separate by && and then do stuff with it?
            # ## string operations: invariant, lower(), etc.

            # if "&&" in ops[fi] or "||" in ops[fi]:

                # # complex predicates that are hard to parse
                # # TODO: can add threshold on length of these
                # # not handling further complex ops within complex ops scenario,
                # # so just deal with the simple case with regexes or likes
                # complexpred = 1
                # complexpredlen = len(ops[fi])
                # cdf = handle_complex_ops(ops[fi])
                # for k,v in cdf.items():
                    # # list of values for that key
                    # curfilterdf[k] += v
                    # if k == "op":
                        # for optype in v:
                            # if optype == "LIKE":
                                # likeop = 1

                # ## TODO: add like stuff
                # # typeall += "like,"
                # # likeop,likekind,likedtype,likecast,curop,likeconst = handle_likeop(curop)
                # # likelen = len(likeconst)
                # continue

            # if isalikeop(ops[fi]):
                # likeop = 1
                # typeall += "like,"
                # # likeop,likekind,likedtype,likecast,curop,likeconst = handle_likeop(ops[fi])
                # # likelen = len(likeconst)
                # cdf = handle_complex_ops(ops[fi])
                # for k,v in cdf.items():
                    # # list of values for that key
                    # curfilterdf[k] += v

            # if "??" in ops[fi]:
                # #print(ops[fi])
                # nullcheck = 1
                # #typeall += "??,"
                # continue

            # if "hasvalue" in ops[fi].lower():
                # #print(ops[fi])
                # #print(fvs)
                # nullcheck = 1
                # #typeall += "hasvalue,"
                # continue

            # if "null" in ops[fi].lower():
                # nullcheck = 1
                # continue

            # # if "(" in ops[fi] and ")" in ops[fi]:
                # # # FIXME: sth better.
                # # udfop = 1
                # # #print(ops[fi])
                # # continue

            # if ops[fi] == "=" or ops[fi] == "!=":
                # datetimeop = False
                # discrete_type = get_discrete_type(fvs)
                # discrete_type_all += discrete_type + ","
                # #typeall += "=" + discrete_type + ","
                # typeall += "=discrete,"

                # for const in fvs:
                    # if "System.DateTime" in const:
                        # datetimeop = True
                        # break

                # if not datetimeop:
                    # curfilterdict = inp_to_discrete_consts
                    # discreteop = 1
                    # if num_discrete_const == 0:
                        # num_discrete_const = 1
                        # child_vals = str(fvs)
                        # if child_vals in inp_to_in_consts[inp]:
                            # inp_to_in_consts[inp][child_vals] += 1
                        # else:
                            # inp_to_in_consts[inp][child_vals] = 1
                # else:
                    # equaldate = 1
                    # #assert ">" in ops[fi] or "<" in ops[fi]
                    # curfilterdict = inp_to_continuous_consts

                # if ops[fi] == "=":
                    # discreteeq = 1
                # elif ops[fi] == "!=":
                    # discretenoneq = 1

                # # assert len(fvs) == 1
                # # if len(fvs) != 1:
                    # # print(fvs)

                # for const in fvs:
                    # curfilterdf["column"].append(col_names[fi])
                    # curfilterdf["op"].append(ops[fi])
                    # curfilterdf["constant"].append(const)
                    # curfilterdf["opstring"].append(ops[fi])

            # elif ">" in ops[fi] or "<" in ops[fi]:
                # contop = 1
                # #assert ">" in ops[fi] or "<" in ops[fi]
                # curfilterdict = inp_to_continuous_consts

                # if len(fvs) != 0:
                    # if "System.Date" in fvs[0]:
                        # contdate = 1
                    # else:
                        # contother = 1
                # else:
                    # continue

                # for const in fvs:
                    # curfilterdf["column"].append(col_names[fi])
                    # curfilterdf["op"].append(ops[fi])
                    # curfilterdf["constant"].append(const)
                    # curfilterdf["opstring"].append(ops[fi])

            # else:
                # # print(ops[fi])
                # unknown = 1
                # continue

            # for const in fvs:
                # if const in curfilterdict[inp]:
                    # curfilterdict[inp][const] += 1
                # else:
                    # curfilterdict[inp][const] = 1

        # like_ops.append(likeop)
        # # like_lens.append(likelen)
        # like_dtype.append(likedtype)
        # like_kind.append(likekind)
        # like_casting.append(likecast)
        # like_const.append("")

        # discrete_ops.append(discreteop)
        # discrete_eqs.append(discreteeq)
        # discrete_noneqs.append(discretenoneq)
        # nullchecks.append(nullcheck)

        # cont_ops.append(contop)
        # cont_dates.append(contdate)
        # cont_others.append(contother)

        # udf_ops.append(udfop)
        # in_ops.append(inop)
        # regex_ops.append(regexop)
        # equal_dates.append(equaldate)
        # num_discrete_consts.append(num_discrete_const)
        # discrete_types.append(discrete_type)
        # discrete_types_all.append(discrete_type_all)

        # complex_ops.append(complexpred)
        # complex_ops_num.append(complexpredlen)

        # # col_names = extract_values(d, "name")
        # col_ops = extract_values(d, "expOperator")

        # for fi, fvs in enumerate(filter_values):
            # if ops[fi] is None:
                # continue
            # if ">" in ops[fi] or "<" in ops[fi]:
                # contdata = parse_cont_vals(ops, filter_values, col_names)
                # num_cont_cols = len(contdata)
                # for col,curcdata in contdata.items():
                    # cont_dtype = curcdata["dtype"]
                    # cont_type = curcdata["cont_type"]
                    # typeall += cont_type + ","
                    # if "range" in curcdata:
                        # contrange = curcdata["range"]
                # break

        # types_all.append(typeall)
        # cont_types.append(cont_type)
        # cont_dtypes.append(cont_dtype)
        # cont_ranges.append(contrange)
        # cont_cols.append(num_cont_cols)

        # seen_cols = []
        # seen_ops = []
        # num_unique_cols = 0
        # num_unique_ops = 0
        # num_operators = 0

        # # TODO: loop over col_ops and find the appropriate String.equals etc. kind of commands
        # assert len(col_ops) == len(col_names)

        # num_ops_all.append(len(curfilterdf))
        # num_filter_cols_all.append(len(set(curfilterdf["column"])))
        # num_unique_ops_all.append(len(set(curfilterdf["op"])))

        # curfilterdf = pd.DataFrame(curfilterdf)
        # likedf = curfilterdf[curfilterdf["op"] == "LIKE"]
        # num_like_cols.append(len(set(likedf["column"])))
        # num_likes.append(len(likedf))
        # if len(likedf) > 0:
            # # likedf["len"] = likedf.apply(lambda x: len(x["constant"]), axis=1)
            # lens = [len(c) for c in x["constant"].values]
            # like_lens.append(max(lens))
        # else:
            # like_lens.append(0)

    # print("final num decode errors: ", num_err)

    # df["num_ops"] = num_ops_all
    # df["num_unique_ops"] = num_unique_ops_all
    # df["unique_filter_cols"] = num_filter_cols_all

    # # df["num_cols_all"] = num_cols_all
    # # df["num_cols_sel"] = num_cols_sel
    # # df["num_pcols"] = num_pcols

    # df["like_ops"] = like_ops
    # df["like_lens"] = like_lens
    # df["like_dtype"] = like_dtype
    # df["like_kind"] = like_kind
    # df["like_casting"] = like_casting
    # df["like_const"] = like_const
    # df["num_like_cols"] = num_like_cols
    # df["num_likes"] = num_likes

    # df["discrete_ops"] = discrete_ops
    # df["discrete_eqs"] = discrete_eqs
    # df["discrete_noneqs"] = discrete_noneqs
    # df["discrete_type"] = discrete_types
    # df["discrete_types_all"] = discrete_types_all

    # df["null_checks"] = nullchecks

    # df["cont_ops"] = cont_ops
    # df["cont_dates"] = cont_dates
    # df["cont_others"] = cont_others
    # df["cont_type"] = cont_types
    # df["cont_dtype"] = cont_dtypes
    # df["cont_range"] = cont_ranges
    # df["cont_cols"] = cont_cols

    # df["types_all"] = types_all

    # df["complex_ops"] = complex_ops
    # df["complex_ops_num"] = complex_ops_num

    # df["udf_ops"] = udf_ops
    # df["in_ops"] = in_ops
    # df["regex_ops"] = regex_ops
    # df["equal_dates"] = equal_dates
    # df["num_discrete_consts"] = num_discrete_consts


def parse_filter_exprs2(df, INP_FIELD, FILTER_FIELD):
    '''
    '''
    augdf = defaultdict(list)
    allopvals = defaultdict(list)

    for idx, row in df.iterrows():
        # cur_row = row
        expr = row[FILTER_FIELD]
        inpname = row[INP_FIELD]
        try:
            d = json.loads(expr)
        except:
            continue

        exprhash = deterministic_hash(expr)
        # parse d FOR cosntant values
        filter_values = extract_values(d, "values")
        ops = extract_values(d, "expOperator")
        children = extract_values(d, "children")
        col_names = extract_values(d, "name")

        curfilterdf = defaultdict(list)
        for fi, fvs in enumerate(filter_values):
            if ops[fi] is None:
                continue
            if ops[fi] == "Or":
                continue
            if ops[fi] == "And":
                continue

            if "regex" in ops[fi].lower():
                regexop = 1
                # again, no continue because it could havwe been linked
                # together with && etc.

            if "&&" in ops[fi] or "||" in ops[fi]:
                # complex predicates that are hard to parse
                cdf = handle_complex_ops(ops[fi])
                for k,v in cdf.items():
                    curfilterdf[k] += v
                continue

            if isalikeop(ops[fi]):
                cdf = handle_complex_ops(ops[fi])
                for k,v in cdf.items():
                    curfilterdf[k] += v
                continue

            if "??" in ops[fi] or "hasvalue" in ops[fi].lower() \
                    or "null" in ops[fi].lower():
                # nullcheck = 1
                continue

            if ops[fi] == "=" or ops[fi] == "!=":
                # discrete_type = get_discrete_type(fvs)
                # discrete_type_all += discrete_type + ","
                dtype = get_discrete_type(fvs)
                if len(fvs) == 0:
                    continue
                is_id = check_is_id(col_names[fi], fvs[0])
                for const in fvs:
                    curfilterdf["column2"].append(col_names[fi])
                    curfilterdf["op"].append(ops[fi])
                    # curfilterdf["constant"].append(const)
                    if "system.date" in const.lower():
                        curfilterdf["constant"].append(datetime_to_secs(const))
                    else:
                        curfilterdf["constant"].append(const)

                    curfilterdf["opstring"].append(ops[fi])
                    curfilterdf["dtype"].append(dtype)
                    curfilterdf["optype"].append(ops[fi])
                    curfilterdf["is_id"].append(is_id)

            elif ">" in ops[fi] or "<" in ops[fi]:

                # FIXME: when does this happen?
                if len(fvs) == 0:
                    continue

                is_id = check_is_id(col_names[fi], fvs[0])
                for const in fvs:
                    curfilterdf["column2"].append(col_names[fi])

                    curfilterdf["op"].append(ops[fi])
                    if "system.date" in const.lower():
                        curfilterdf["constant"].append(datetime_to_secs(const))
                    else:
                        curfilterdf["constant"].append(const)

                    curfilterdf["opstring"].append(ops[fi])
                    curfilterdf["dtype"].append(get_cont_dtype(const)[0])
                    curfilterdf["optype"].append(ops[fi])
                    curfilterdf["is_id"].append(is_id)
            else:
                # print(ops[fi])
                unknown = 1
                continue

        if len(curfilterdf) == 0:
            continue


        curfilterdf["exprhash"] = [exprhash]*len(curfilterdf["op"])
        curfilterdf["input"] = [inpname]*len(curfilterdf["op"])
        curfilterdf["jobid"] = [row["jobid"]]*len(curfilterdf["op"])
        curfilterdf["InputCardinality"] = [row["InputCardinality"]]*len(curfilterdf["op"])
        curfilterdf["StageName"] = [row["StageName"]]*len(curfilterdf["op"])

        # old_columns = copy.deepcopy(curfilterdf["column"])
        # curfilterdf["column2"] = old_columns
        curfilterdf["column"] = [str(inpname) + remove_ints(c) for c in curfilterdf["column2"]]

        for k,v in curfilterdf.items():
            # print(k, len(v))
            allopvals[k] += v

        for k,v in row.items():
            augdf[k].append(v)

        # lets add a few more values
        newops = copy.deepcopy(curfilterdf["op"])

        # augdf["input"].append(inpname)
        # TODO: maybe add (a,b) values for cont_range queries to make it easier
        # to plot

        augdf["like_ops"].append(int("LIKE" in newops))
        augdf["discrete_ops"].append(int("=" in newops or "!=" in newops))
        augdf["discrete_eqs"].append(int("=" in newops))
        augdf["discrete_noneqs"].append(int("!=" in newops))
        augdf["is_id"].append(int(1 in curfilterdf["is_id"]))

        augdf["cont_ops"].append(int("<" in newops or ">" in newops or "<=" \
                in newops or ">=" in newops))
        augdf["regex_ops"].append(int("REGEX" in newops))

        augdf["num_ops"].append(len(curfilterdf["op"]))

        augdf["unique_filter_cols"].append(len(set(curfilterdf["column"])))
        augdf["unique_filter_cols2"].append(len(set(curfilterdf["column2"])))
        augdf["num_unique_ops"].append(len(set(curfilterdf["op"])))

        try:
            curfilterdf = pd.DataFrame(curfilterdf)
        except Exception as e:
            print(e)
            for k,v in curfilterdf.items():
                print(k, len(v))
            pdb.set_trace()

        allcolops = []
        for col in set(curfilterdf["column"]):
            tmp = curfilterdf[curfilterdf["column"] == col]
            colop = tmp["op"].values[0]
            if colop in ["=", "!="]:
                allcolops.append("=")
            elif ">" in colop or "<" in colop:
                allcolops.append("cont")
            elif colop == "LIKE":
                allcolops.append("LIKE")
        allcolops.sort()
        augdf["types_all"].append(",".join(allcolops))

        likedf = curfilterdf[curfilterdf["op"] == "LIKE"]
        augdf["num_like_cols"].append(len(set(likedf["column"])))
        augdf["num_likes"].append(len(likedf))

        if len(likedf) > 0:
            likedf["len"] = likedf.apply(lambda x: len(str(x["constant"])), axis=1)
            # likedf.loc[:,"len"] = likedf.apply(lambda x: len(str(x["constant"])), axis=1)

            likedf = likedf.sort_values(by="len", ascending=False)
            augdf["like_lens"].append(likedf["len"].values[0])
            augdf["like_dtype"].append(likedf["dtype"].values[0])
            augdf["like_kind"].append(likedf["optype"].values[0])
            if augdf["like_kind"][-1] == "=":
                print("bad like!!!")
                print(likedf["op"].values)
                print(likedf["dtype"].values)
                print(likedf["optype"].values)
                print(likedf["opstring"].values)
                print("*********************")
        else:
            augdf["like_lens"].append(0)
            augdf["like_dtype"].append("")
            augdf["like_kind"].append("")

        discdf = curfilterdf[curfilterdf["op"].isin(["=", "!="])]
        augdf["num_discrete_consts"].append(len(discdf))
        if len(discdf) > 0:
            augdf["discrete_type"].append(discdf["dtype"].values[0])

        else:
            augdf["discrete_type"].append("")

        # FIXME:
        augdf["in_ops"].append(0)

        cont_type = ""
        cont_dtype = ""
        contrange = -1
        cont_column = ""
        num_cont_cols = 0
        cont_lt = -1
        cont_gt = -1

        if augdf["cont_ops"][-1]:
            # lets find more details about the continuous operators
            for fi, fvs in enumerate(filter_values):
                if ops[fi] is None:
                    continue
                if ">" in ops[fi] or "<" in ops[fi]:
                    contdata = parse_cont_vals(ops, filter_values, col_names)
                    num_cont_cols = len(contdata)
                    for col,curcdata in contdata.items():
                        cont_dtype = curcdata["dtype"]
                        cont_type = curcdata["cont_type"]
                        if "range" in curcdata:
                            contrange = curcdata["range"]
                            cont_column = str(inpname) + remove_ints(col)
                            # crange = ret[col]["lt"] - ret[col]["gt"]
                            cont_lt = curcdata["lt"]
                            cont_gt = curcdata["gt"]

                    break

        augdf["cont_dtype"].append(cont_dtype)
        augdf["cont_type"].append(cont_type)
        augdf["cont_range"].append(contrange)
        augdf["cont_cols"].append(num_cont_cols)
        augdf["cont_column"].append(cont_column)
        augdf["cont_lt"].append(cont_lt)
        augdf["cont_gt"].append(cont_gt)

    opdf = pd.DataFrame(allopvals)
    opdf["discrete_ops"] = opdf.apply(lambda x: int(x["op"] in ["=", "!="]) ,
                    axis=1)
    opdf["cont_ops"] = opdf.apply(lambda x: int(">" in x["op"] or "<" in
            x["op"]) , axis=1)
    opdf["like_ops"] = opdf.apply(lambda x: int(x["op"] == "LIKE") , axis=1)

    return pd.DataFrame(augdf), opdf


def parse_filter_exprs_par(df, INP_FIELD, FILTER_FIELD, num_par=32):
    # divide df into N parts
    fsize = 50000
    frames = [df.iloc[i*fsize:min((i+1)*fsize,len(df))] for i in
            range(int(len(df)/fsize) + 1)]

    par_args = [(f, INP_FIELD, FILTER_FIELD) for f in frames]

    with Pool(processes = num_par) as pool:
        res = pool.starmap(parse_filter_exprs2, par_args)

    augdfs = [r[0] for r in res]
    opdfs = [r[1] for r in res]

    augdf = pd.concat(augdfs, ignore_index=True)
    opdf = pd.concat(opdfs, ignore_index=True)

    return augdf,opdf

# def merge_dbs(dbs, seeninps, inps):

def find_dbs_par(df, inp_field, num_par=32):
    fsize = 10000
    frames = [df.iloc[i*fsize:min((i+1)*fsize,len(df))] for i in
            range(int(len(df)/fsize) + 1)]

    par_args = [(f, str(fi), inp_field) for fi,f in enumerate(frames)]
    with Pool(processes = num_par) as pool:
        res = pool.starmap(find_dbs, par_args)

    # lets merge all the seeninps
    seeninps = defaultdict(set)
    dbs = defaultdict(set)

    for i in range(len(res)):
        for k,v in res[i][1].items():
            seeninps[k] = seeninps[k].union(v)
        for k,v in res[i][0].items():
            # assert k not in dbs
            if k in dbs:
                print("FUCK NO!")
                print(k)

            dbs[k] = v

    allinps = list(seeninps.keys())
    for inp in allinps:
        inpdbs = seeninps[inp]
        if len(inpdbs) <= 1:
            continue
        # needs merging
        inpdbs = list(inpdbs)
        chosendb = inpdbs[0]

        for di in range(1, len(inpdbs)):
            newdb = inpdbs[di]
            newinps = dbs[newdb]
            for newinp in newinps:
                seeninps[newinp].add(chosendb)
                seeninps[newinp].remove(newdb)

            # delete the db mapping since we won't use it again
            del dbs[newdb]
            if newdb in seeninps[inp]:
                # print("newdb still in seeninps")
                seeninps[inp].remove(newdb)

    # dbs and seeninps should be updated at this point
    return dbs, seeninps

def find_dbs(df, dbi, inp_field):

    dbi = dbi + str(random.randint(50,500))
    start = time.time()
    dbs = defaultdict(set)
    seeninps = defaultdict(set)
    numdbs = 0

    for jid in set(df["jobid"]):
        tmp = df[df.jobid == jid]
        inps = set(tmp[inp_field].values)
        seendb = False
        curseeninp = None
        for inp in inps:
            if inp in seeninps:
                curseeninp = inp
                seendb = True
                break

        chosendb = None
        if not seendb:
            numdbs += 1
            chosendb = dbi + str(numdbs)
            for inp in inps:
                dbs[chosendb].add(inp)
        else:
            # which db was any of the inp in?? And put others in that
            assert curseeninp is not None
            chosendb = list(seeninps[curseeninp])[0]

            # choose first db and add everything to that
            for inp in inps:
                dbs[chosendb].add(inp)

        for inp in inps:
            seeninps[inp].add(chosendb)

    # print("db grouping took: ", time.time()-start)
    # TODO: merge things here or after parallel?

    return dbs,seeninps


def get_operation_cols(optoken):

    def _get_cols(tokens):
        for token in tokens:
            if isinstance(token, sqlparse.sql.Parenthesis):
                _get_cols(token)
            elif isinstance(token, sqlparse.sql.Identifier):
                cols.append(token.value)
            elif isinstance(token, sqlparse.sql.Function):
                curc = get_function_cols(token)
                for c in curc:
                    cols.append(c)
            else:
                pass

    cols = []
    _get_cols(optoken)

    return cols

def get_function_cols(func_token):
    cols = []
    for param in func_token.get_parameters():
        if isinstance(param, sqlparse.sql.Identifier):
            cols.append(param.value)
    return cols

def find_prev_token(tokens, idx):
    # skips over whitespace tokens to get to previous non-whitespace token, up
    # to -20
    for i in range(1,100,1):
        ctoken = tokens[idx-i]
        if str(ctoken).strip() == "":
            continue
        return i,ctoken
    return -1,None

def find_next_token(tokens, idx):
    # skips over whitespace tokens to get to previous non-whitespace token, up
    # to -20
    for i in range(1,100,1):
        ctoken = tokens[idx+i]
        if str(ctoken).strip() == "":
            continue
        return i,ctoken
    return None

def parse_sql_preds(sql):
    def get_table_col(left):
        if "." in left:
            tablename = left[0:left.find(".")]
            col = left[left.find(".")+1:]
        else:
            tablename = "X"
            col = left
        return tablename, col

    def parse_where(wtokens):
        curwhere = {}
        for idx, token in enumerate(wtokens):
            if "where" in str(type(token)).lower():
                parse_where(token)
                continue

            if isinstance(token, sqlparse.sql.Parenthesis):
                assert hasattr(token, "tokens")
                #parse_recursive(token)
                parse_where(token)
                continue

            if "comparison" in str(type(token)).lower():
                assert hasattr(token, "tokens")
                left = None
                op = None
                right = None
                opstring = str(token)
                if "catery" in opstring:
                    opstring = opstring.replace("catery", "category")

                allv = [t for t in token if t.value.strip() != ""]
                if len(allv) > 3:
                    print("more than 3 tokens in a comparison op!")
                    print("alltokens: ", allv)
                    print("full token: ", token)
                    print("*****")

                left = allv[0]
                op = allv[1]
                right = allv[2]
                leftval = left.value

                if "catery" in leftval:
                    leftval = leftval.replace("catery", "category")

                if isinstance(left, sqlparse.sql.Identifier):
                    tablename, col = get_table_col(leftval)

                elif isinstance(left, sqlparse.sql.Function):
                    tablename = "function"
                    allcols = get_function_cols(left)
                    col = ",".join(allcols)
                elif isinstance(left, sqlparse.sql.Parenthesis):
                    assert hasattr(left, "tokens")
                    parse_where(left)
                    tablename = "function"
                    col = "function"
                elif isinstance(left, sqlparse.sql.Operation):
                    allcols = get_operation_cols(left)
                    # print(allcols)
                    # print("*******")
                    tablename = "function"
                    col = "function"
                else:
                    # FIXME: handle these better
                    if isinstance(right, sqlparse.sql.Identifier):
                        tablename, col = get_table_col(right.value)
                        tmp1 = right
                        right = left
                        left = tmp1
                        # FIXME: need to change other stuff, like operator type too
                    else:
                        assert False

                # TODO: do this outsider here
                if "catery" in col:
                    col = col.replace("catery", "category")

                if isinstance(right, sqlparse.sql.Identifier):
                    #print("Join?")
                    continue

                if isinstance(right, sqlparse.sql.Parenthesis):
                    assert hasattr(right, "tokens")
                    parse_recursive(right)

                # TODO: need a better way to find joins
                if not "token" in str(type(right)).lower():
                    if not "select" in right.value and \
                        op.value == "=" and \
                        "." in right.value:
                        # potential join
                        continue

                op = op.value.lower()
                right = right.value

                # TODO: parse optype etc as well
                is_id = check_is_id_sql(col, str(right))

                # print(tablename)
                # print(opstring)

                if "like" in op:
                    allpreddata["op"].append("LIKE")
                    likedtype,likekind = get_like_type(op, str(right))
                    allpreddata["dtype"].append(likedtype)
                    allpreddata["optype"].append(likekind)
                    allpreddata["constant"].append(str(right))

                    allpreddata["input"].append(tablename)
                    allpreddata["column"].append(col)
                    allpreddata["opstring"].append(opstring)
                    allpreddata["is_id"].append(is_id)

                elif op in ["=", "!="]:
                    dtype,dkind = get_discrete_type_sql(op, str(right))
                    allpreddata["op"].append(op)
                    allpreddata["dtype"].append(dtype)
                    allpreddata["optype"].append(op)
                    allpreddata["constant"].append(str(right))

                    allpreddata["input"].append(tablename)
                    allpreddata["column"].append(col)
                    allpreddata["opstring"].append(opstring)
                    allpreddata["is_id"].append(is_id)

                elif op in ["in", "not in"]:
                    if op == "in":
                        inop = "="
                    else:
                        inop = "!="
                    # is it a sql on the right?
                    if "select" in right:
                        dtype,dkind = get_discrete_type_sql(op, right)
                        assert dtype == "sql"
                        allpreddata["op"].append(inop)
                        allpreddata["dtype"].append(dtype)
                        allpreddata["optype"].append(op)
                        allpreddata["constant"].append("sql")
                        allpreddata["input"].append(tablename)
                        allpreddata["column"].append(col)
                        allpreddata["opstring"].append(opstring)
                        allpreddata["is_id"].append(is_id)

                    else:
                        allrights = right.split(",")
                        dtype,dkind = get_discrete_type_sql(op, str(allrights[0]))
                        for r in allrights:
                            allpreddata["op"].append(inop)
                            allpreddata["dtype"].append(dtype)
                            allpreddata["optype"].append(op)
                            r = r.replace("(", "")
                            r = r.replace(")", "")
                            allpreddata["constant"].append(r.strip())

                            allpreddata["input"].append(tablename)
                            allpreddata["column"].append(col)
                            allpreddata["opstring"].append(opstring)
                            allpreddata["is_id"].append(is_id)

                elif ">" in op or "<" in op:
                    allpreddata["op"].append(op)
                    allpreddata["dtype"].append("cont")
                    allpreddata["optype"].append("cont")
                    allpreddata["constant"].append(str(right))

                    allpreddata["input"].append(tablename)
                    allpreddata["column"].append(col)
                    allpreddata["opstring"].append(opstring)
                    allpreddata["is_id"].append(is_id)
                else:
                    # print("WOOOT?")
                    # print(op)
                    # print(col)
                    # print(right)
                    continue
                    # allpreddata["op"].append(op)
                    # allpreddata["dtype"].append("woot")
                    # allpreddata["optype"].append("woot")
                    # allpreddata["constant"].append(str(right))

                    # allpreddata["input"].append(tablename)
                    # allpreddata["column"].append(col)
                    # allpreddata["opstring"].append(op)
                    # allpreddata["is_id"].append(is_id)

            if str(token) == "between":
                # print(dir(token))
                # left = wtokens[idx-2]
                lefti, left = find_prev_token(wtokens, idx)
                if isinstance(left, sqlparse.sql.Identifier):
                    tablename, col = get_table_col(left.value)
                elif isinstance(left, sqlparse.sql.Parenthesis):
                    tablename = "between-parenthesis"
                    col = "between-parenthesis"
                else:
                    tablename = "between-unknown"
                    col = "between-unknown"

                righti,righta = find_next_token(wtokens, idx)
                righti2,rightand = find_next_token(wtokens, idx+righti)
                righti3,rightb = find_next_token(wtokens, idx+righti+righti2)
                endright = idx+righti+righti2+righti3

                if str(rightand).lower() != "and":
                    print("not and!")

                optokens = wtokens[idx-lefti:idx+righti3]
                optokens = [str(o) for o in wtokens[idx-lefti:endright+1]]
                opstring = " ".join(optokens)

                contdtype,rightval = get_cont_dtype_sql(str(rightb))
                contdtype,leftval = get_cont_dtype_sql(str(righta))

                ltval = rightb
                gtval = righta


                is_id = check_is_id_sql(col, ltval)

                allpreddata["input"].append(tablename)
                allpreddata["column"].append(col)
                allpreddata["op"].append(">")
                allpreddata["opstring"].append(opstring)
                allpreddata["optype"].append("between")
                allpreddata["dtype"].append("cont")
                allpreddata["constant"].append(str(righta))
                allpreddata["is_id"].append(is_id)

                allpreddata["input"].append(tablename)
                allpreddata["column"].append(col)
                allpreddata["op"].append("<")
                allpreddata["opstring"].append(opstring)
                allpreddata["optype"].append("between")
                allpreddata["dtype"].append("cont")
                allpreddata["constant"].append(str(rightb))
                allpreddata["is_id"].append(is_id)

    def parse_recursive(tokens):
        for idx, token in enumerate(tokens):
            if hasattr(token, "tokens"):
                parse_recursive(token)

            # original code
            if "where" in str(type(token)).lower():
                parse_where(token)

            if token.value.lower() in ["having", "group by"]:
                # TODO: parse
                index, token2 = tokens.token_next(idx)

    allpreddata = defaultdict(list)
    allpreds = []

    if sql.strip() == "":
        return pd.DataFrame(allpreddata)

    parsed = sqlparse.parse(sql)
    assert len(parsed) == 1
    parse_recursive(parsed[0])

    df = pd.DataFrame(allpreddata)
    return df

EXPR_TMP = "SELECT COUNT(*) FROM {TAB} WHERE {PREDS}"

def ops_to_expr_df(df):
    exprdicts = []
    for jobid in set(df["jobid"]):
        curdf = df[df.jobid == jobid]
        edf = ops_to_expr_df_singleq(curdf)
        exprdicts.append(edf)

    cdict = defaultdict(list)
    for edict in exprdicts:
        for k,v in edict.items():
            cdict[k] += v

    return pd.DataFrame(cdict)

def ops_to_expr_df_singleq(df):
    '''jobid', 'HashTagInput', 'OperatorName', 'EstCardinality',
       'InputCardinality', 'NormInputHashCode', 'RowCount', 'Selectivity',
       'ExclusiveTime', 'StageName', 'VertexCount', 'FilterExpr', 'QError',
       'like_ops', 'discrete_ops', 'discrete_eqs', 'discrete_noneqs',
       'cont_ops', 'regex_ops', 'num_ops', 'unique_filter_cols',
       'num_unique_ops', 'types_all', 'num_like_cols', 'num_likes',
       'like_lens', 'like_dtype', 'like_kind', 'num_discrete_consts',
       'discrete_type', 'in_ops', 'cont_dtype', 'cont_type', 'cont_range',
       'cont_cols'''

    assert len(set(df["jobid"])) == 1
    jobid = str(df["jobid"].values[0])
    exprs = defaultdict(list)
    aliases = set(df["alias"])

    for al in aliases:
        tmp = df[df["alias"] == al]

        # convert into a sql statement to get rowcount
        tab = tmp["input"].values[0]
        filterexprs = tmp["opstring"].drop_duplicates().values
        filterexpr = " AND ".join(filterexprs)
        if tab == "X" or tab == "function" or al == "function" or al == "X":
            filtersql = "X"

        elif "select" in filterexpr:
            filtersql = "X"
        else:
            # if "d2" in filterexpr:
                # print("!!!!!!!!!")
                # print(tab, al)
                # print(filterexpr)
                # print("!!!!!!!!!")

            # if "d1" in filterexpr and "d2" in filterexpr:
                # print("!!!!!!!!!")
                # print(tab, al)
                # print(filterexpr)
                # print("!!!!!!!!!")

            # if "." in filterexpr:
                # exal = filterexpr[0:filterexpr.find(".")]
                # if exal != al:
                    # print("!!!!!!!!!")
                    # print(exal)
                    # print(al)
                    # print(tab)
                    # print(filterexpr)
                    # print("!!!!!!!!!")
                # print(tab)
                # print(al)
                # print(filterexpr)

            filtersql = COUNT_TMP.format(TABLE=tab,
                                         ALIAS = al,
                                         FILTER = filterexpr)
            filtersql = filtersql.replace("\n", " ")
            filtersql = filtersql.replace("\t", " ")

        # expr
        exprs["filtersql"].append(filtersql)
        exprs["jobid"].append(jobid)
        curinp = tmp["input"].values[0]
        exprs["input"].append(curinp)
        exprs["alias"].append(al)

        exprs["exprhash"].append(tmp["exprhash"].values[0])

        exprs["unique_filter_cols"].append(len(set(tmp["column"])))
        exprs["discrete_ops"].append(int(1 in tmp["discrete_ops"].values))
        exprs["cont_ops"].append(int(1 in tmp["cont_ops"].values))
        exprs["like_ops"].append(int(1 in tmp["like_ops"].values))
        exprs["is_id"].append(int(1 in tmp["is_id"].values))

        exprs["num_unique_ops"].append(len(set(tmp["op"])))
        exprs["num_ops"].append(len(tmp))

        discdf = tmp[tmp["discrete_ops"] == 1]
        exprs["num_discrete_consts"].append(len(discdf["constant"]))

        likedf = tmp[tmp["like_ops"] == 1]
        exprs["num_likes"].append(len(likedf))
        if len(likedf) > 0:
            lens = [len(c) for c in likedf["constant"].values]
            exprs["like_lens"].append(max(lens))

        else:
            exprs["like_lens"].append(0)
        contdf = tmp[tmp["cont_ops"] == 1]

    return exprs

def parse_sqls_single(sqls, batchnum=0, cols_to_tables={}):
    start = time.time()
    fails = 0
    allinpdata = defaultdict(list)
    allpreds = []

    for si, sql in enumerate(sqls):
        if sql.strip() == "":
            continue
        # print(si)

        alldfdata = defaultdict(list)
        numwheres = sql.lower().count("where")

        try:
            preds = parse_sql_preds(sql)
        except Exception as e:
            # print(e)
            print("FAIL")
            fails += 1
            continue

        preds = preds.drop_duplicates()
        # print(preds)
        if len(preds) == 0:
            continue

        # this is only useful for job/ceb stuff
        # preds["alias"] = preds.apply(lambda x: x["input"], axis=1)
        # inputs = preds.apply(lambda x: remove_ints(x["input"]) ,axis=1)
        # preds["input"] = inputs
        preds["jobid"] = batchnum*si + si

        if len(preds) == 0:
            continue

        allpreds.append(preds)

    if len(allpreds) == 0:
        return None,None

    opdf = pd.concat(allpreds, ignore_index=True)

    opdf["discrete_ops"] = opdf.apply(lambda x: int(x["op"] in ["=", "!="]) ,
            axis=1)
    opdf["cont_ops"] = opdf.apply(lambda x: int(">" in x["op"] or "<" in
        x["op"]) , axis=1)
    opdf["like_ops"] = opdf.apply(lambda x: int(x["op"] == "LIKE") , axis=1)

    def update_inputs(row, cols_to_tables):
        # if it is in cols_to_tables, that supersedes everything
        if row["column"] in cols_to_tables:
            return cols_to_tables[row["column"]]
        else:
            return remove_ints(row["input"])

    def update_alias(row, cols_to_tables):
        if row["input"] == "X" and row["column"] in cols_to_tables:
            return cols_to_tables[row["column"]]
        else:
            return row["input"]

    # need to update both input / alias, since we use alias for finding
    # expressions etc.
    opdf["alias"] = opdf.apply(lambda x: update_alias(x, cols_to_tables), axis=1)
    opdf["input"] = opdf.apply(lambda x: update_inputs(x, cols_to_tables), axis=1)

    # print(set(opdf["alias"]))
    # print(set(opdf["input"]))

    opdf["exprhash"] = opdf.apply(lambda x: deterministic_hash(str(x["jobid"])+x["alias"]+x["input"]) ,axis=1)

    exprdf = ops_to_expr_df(opdf)

    print("finished parsing batch: ", batchnum, "in : ",
            int(time.time()-start), "fails: ", fails)
    return opdf,exprdf

def parse_sqls_par(sqls, cols_to_tables={}):
    # num_par = int(len(sqls) / 1000)
    # num_par = max(1, num_par)
    # num_par = min(num_par, 64)

    fsize = 1000
    sframes = [sqls[i*fsize:min((i+1)*fsize,len(sqls))] for i in
            range(int(len(sqls)/fsize) + 1)]
    print("number of sql groups: ", len(sframes))
    num_par = min(len(sframes), 64)
    par_args = [(s, si, cols_to_tables) for si,s in enumerate(sframes)]

    with Pool(processes = num_par) as pool:
        res = pool.starmap(parse_sqls_single, par_args)

    opdfs = [r[0] for r in res if r[0] is not None]
    edfs = [r[1] for r in res if r[1] is not None]

    return pd.concat(opdfs, ignore_index=True), pd.concat(edfs,
            ignore_index=True)

