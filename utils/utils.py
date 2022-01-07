import re
import dateutil.parser as dp
from collections import defaultdict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
import hashlib

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
    print(expr)
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

# binaryname.tolower().contains(".cat")
# binaryname.tolower().contains(".hbaked")
# binaryname.tolower().contains(".htm")
# binaryname.tolower().contains(".html")
# binaryname.tolower().contains(".js")
# binaryname.tolower().contains(".man")
# binaryname.tolower().contains(".manifest")
# binaryname.tolower().contains(".mui")
# binaryname.tolower().contains(".mum")
# binaryname.tolower().contains(".png")
# binaryname.tolower().contains(".txt")
# binaryname.tolower().contains(".xml")
# binaryname.tolower().contains(".xrm-ms")


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
    likekind = "unknown"
    likecast = 0
    likedtype = "unknown"

    if "upper" in curop or "lower" in curop or "invariant" in curop:
        likecast = 1

    if "contains" in curop:
        curop = curop[curop.find("contains"):]
        opval = curop[curop.rfind("(")+1:-2]
        likekind = "contains"

    elif "like" in curop:
        curop = curop[curop.find("like"):]
        opval = curop[curop.find("\"")+1:curop.rfind("\"")]
        if curop.count("%") == 0:
            likekind = "no%"
        elif curop.count("%") == 2:
            likekind = "contains"
        elif curop.count("%") == 1:
            if opval[0] == "%":
                likekind = "ends"
            elif opval[-1] == "%":
                likekind = "starts"
            else:
                likekind = "no%"
        elif curop.count("%") > 2:
            likekind = "multi%"
        else:
            assert False

    elif "starts" in curop:
        likekind = "starts"
        curop = curop[curop.find("starts"):]
        opval = curop[curop.find("\"")+1:curop.rfind("\"")]
    elif "ends" in curop:
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


    return likekind, likedtype, likecast, curop, opval

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

def get_cont_dtype(val):
    if val.lower()[-1] == "d" or val.lower()[-1] == "f":
        val = val[0:-1]

    if "system.date" in val.lower():
        t = val[val.find("/*")+2:val.find("*/")]
        #print(val)
        #print(t)
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
                if ret[col]["comb_op"] != "Or":
                    print("******************")
                    print(ops)
                    print(vals)
                    print(col)
                    print(ret[col])
                    print(crange)
                    print(ret[col]["lt"], ret[col]["gt"])
                    print("******************")
                # assert ret[col]["comb_op"] == "Or"
                ret[col]["range"] = crange
            else:
                ret[col]["range"] = crange

        ret[col]["cont_type"] = ctype

    return ret

def parse_filter_exprs(df, INP_FIELD, FILTER_FIELD):
    cur_row = None

    inp_to_filter_cols = defaultdict(set)
    inp_to_pcols = defaultdict(set)
    inp_to_all_cols = defaultdict(set)

    inp_to_ops = defaultdict(set)
    inp_to_num_cols = defaultdict(list)

    inp_to_discrete_consts = defaultdict(dict)
    inp_to_continuous_consts = defaultdict(dict)
    inp_to_op_kind = defaultdict(dict)
    inp_to_in_consts = defaultdict(dict)

    inp_to_like_consts = defaultdict(dict)

    num_ops_all = []
    num_filter_cols_all = []
    num_cols_all = []
    num_cols_sel = []
    num_unique_ops_all = []
    num_pcols = []

    like_ops = []
    like_lens = []
    like_dtype = []
    like_kind = []
    like_casting = []
    like_const = []

    discrete_ops = []
    discrete_eqs = []
    discrete_noneqs = []
    discrete_types = []
    discrete_types_all = []
    types_all = []

    nullchecks = []

    cont_ops = []
    cont_dates = []
    cont_others = []

    cont_types = []
    cont_dtypes = []
    cont_ranges = []
    cont_cols = []

    complex_ops = []
    complex_ops_num = []

    udf_ops = []
    in_ops = []
    equal_dates = []
    num_discrete_consts = []

    num_err = 0

    for idx, row in df.iterrows():
        cur_row = row
        expr = row[FILTER_FIELD]
        # inpcols_all = row["inputColumns"]
        # inp_sel = row["inputSelected"]
        inp = row[INP_FIELD]
        try:
            d = json.loads(expr)
        except:
            num_err += 1
            num_ops_all.append(-1)
            #num_unique_cols_all.append(-1)
            num_filter_cols_all.append(-1)
            num_unique_ops_all.append(-1)
            num_cols_all.append(-1)
            num_cols_sel.append(-1)
            num_pcols.append(-1)
            like_ops.append(-1)
            discrete_ops.append(-1)
            discrete_eqs.append(-1)
            discrete_noneqs.append(-1)
            cont_ops.append(-1)
            cont_dates.append(-1)
            cont_others.append(-1)
            udf_ops.append(-1)
            in_ops.append(-1)
            equal_dates.append(-1)
            complex_ops.append(-1)
            complex_ops_num.append(-1)
            #discrete_consts.append(-1)
            discrete_types.append(-1)
            discrete_types_all.append(-1)
            num_discrete_consts.append(-1)
            nullchecks.append(-1)
            types_all.append(-1)
            cont_types.append(-1)
            cont_dtypes.append(-1)
            cont_ranges.append(-1)
            like_lens.append(-1)
            cont_cols.append(-1)
            like_dtype.append(-1)
            like_kind.append(-1)
            like_casting.append(-1)
            like_const.append(-1)
            continue

        # parse d FOR cosntant values
        filter_values = extract_values(d, "values")
        ops = extract_values(d, "expOperator")
        children = extract_values(d, "children")

        likeop = 0
        likelen = 0

        udfop = 0
        inop = 0
        discreteop = 0
        discreteeq = 0
        discretenoneq = 0
        contop = 0
        contdate = 0
        contother = 0
        equaldate = 0
        num_discrete_const = 0

        complexpred = 0
        complexpredlen = 0
        unknown = 0

        nullcheck = 0

        typeall = ""
        discrete_type = ""
        discrete_type_all = ""

        cont_type = ""
        cont_dtype = ""
        contrange = 0.0
        num_cont_cols = 0

        likekind = ""
        likedtype = ""
        likecast = 0
        likeconst = ""

        for fi, fvs in enumerate(filter_values):
            if ops[fi] is None:
                continue
            if ops[fi] == "Or":
                # probably IN
                if "IN" in inp_to_op_kind[inp]:
                    inp_to_op_kind[inp]["IN"] += 1
                else:
                    inp_to_op_kind[inp]["IN"] = 1
                inop = 1

                # TODO: can be a mix of >= and =; handle case.
                child_vals = extract_values(children[fi], "values")
                child_cols = extract_values(children[fi], "name")

                # print(child_vals)
                # print(child_cols)

                child_vals = [c[0] for c in child_vals if (len(c) > 0 \
                                        and "System.DateTime" not in c[0])]
                num_discrete_const = len(child_vals)
                child_vals.sort()
                child_vals = str(child_vals)
                if child_vals in inp_to_in_consts[inp]:
                    inp_to_in_consts[inp][child_vals] += 1
                else:
                    inp_to_in_consts[inp][child_vals] = 1
                continue

            # TODO: And + =,!= combination on same column? seems to be rare;
            # and usual case is handled when we encounter them later.
            if ops[fi] == "And":
                # child_vals = extract_values(children[fi], "values")
                # child_cols = extract_values(children[fi], "name")
                # print(child_vals)
                # print(child_cols)
                continue

            # TODO: complex predicates
            # potential large classes:
            ## regex-matches;
            # like: contains, endswith etc. ---> just parse them out and
            # analyze them too
            ## separate by && and then do stuff with it?
            ## string operations: invariant, lower(), etc.
            if "&&" in ops[fi] or "##" in ops[fi] or "||" in ops[fi]:
                # complex predicates that are hard to parse
                # TODO: can add threshold on length of these
                complexpred = 1
                complexpredlen = len(ops[fi])
                # print(ops[fi])
                continue

            if "??" in ops[fi]:
                #print(ops[fi])
                nullcheck = 1
                #typeall += "??,"
                continue

            if "hasvalue" in ops[fi].lower():
                #print(ops[fi])
                #print(fvs)
                nullcheck = 1
                #typeall += "hasvalue,"
                continue

            if "null" in ops[fi].lower():
                #print(ops[fi])
                #print(fvs)
                nullcheck = 1
                #typeall += "null,"
                continue

            if "like" in ops[fi].lower() or "starts" in ops[fi].lower() \
                or "contains" in ops[fi].lower() or "endswith" in ops[fi].lower():
                if "LIKE" in inp_to_op_kind[inp]:
                    inp_to_op_kind[inp]["LIKE"] += 1
                else:
                    inp_to_op_kind[inp]["LIKE"] = 1
                likeop = 1
                assert len(fvs) == 0
                typeall += "like,"
                curop = ops[fi].lower()
                likekind, likedtype, likecast, curop, likeconst = get_like_kind(curop)

                if likeconst in inp_to_like_consts[inp]:
                    inp_to_like_consts[inp][likeconst] += 1
                else:
                    inp_to_like_consts[inp][likeconst] = 1

                likelen = len(likeconst)
                continue

            if "(" in ops[fi] and ")" in ops[fi]:
                # FIXME: sth better.
                udfop = 1
                #print(ops[fi])
                continue

            if ops[fi] == "=" or ops[fi] == "!=":
                datetimeop = False
                discrete_type = get_discrete_type(fvs)
                discrete_type_all += discrete_type + ","
                #typeall += "=" + discrete_type + ","
                typeall += "=discrete,"

                for const in fvs:
                    if "System.DateTime" in const:
                        datetimeop = True
                        break

                if not datetimeop:
                    curfilterdict = inp_to_discrete_consts
                    discreteop = 1
                    if num_discrete_const == 0:
                        num_discrete_const = 1
                        child_vals = str(fvs)
                        if child_vals in inp_to_in_consts[inp]:
                            inp_to_in_consts[inp][child_vals] += 1
                        else:
                            inp_to_in_consts[inp][child_vals] = 1
                else:
                    equaldate = 1
                    #assert ">" in ops[fi] or "<" in ops[fi]
                    curfilterdict = inp_to_continuous_consts

                if ops[fi] == "=":
                    discreteeq = 1
                elif ops[fi] == "!=":
                    discretenoneq = 1

            elif ">" in ops[fi] or "<" in ops[fi]:
                contop = 1
                #assert ">" in ops[fi] or "<" in ops[fi]
                curfilterdict = inp_to_continuous_consts

                #print(fvs)

                if len(fvs) != 0:
                    if "System.Date" in fvs[0]:
                        contdate = 1
                    else:
                        contother = 1
                else:
                    continue
            else:
                # print(ops[fi])
                unknown = 1
                continue

            for const in fvs:
                if const in curfilterdict[inp]:
                    curfilterdict[inp][const] += 1
                else:
                    curfilterdict[inp][const] = 1

        like_ops.append(likeop)
        like_lens.append(likelen)
        like_dtype.append(likedtype)
        like_kind.append(likekind)
        like_casting.append(likecast)
        like_const.append(likeconst)

        discrete_ops.append(discreteop)
        discrete_eqs.append(discreteeq)
        discrete_noneqs.append(discretenoneq)
        nullchecks.append(nullcheck)

        cont_ops.append(contop)
        cont_dates.append(contdate)
        cont_others.append(contother)

        udf_ops.append(udfop)
        in_ops.append(inop)
        equal_dates.append(equaldate)
        num_discrete_consts.append(num_discrete_const)
        discrete_types.append(discrete_type)
        discrete_types_all.append(discrete_type_all)

        complex_ops.append(complexpred)
        complex_ops_num.append(complexpredlen)

        col_names = extract_values(d, "name")
        col_ops = extract_values(d, "expOperator")

        for fi, fvs in enumerate(filter_values):
            if ops[fi] is None:
                continue
            if ">" in ops[fi] or "<" in ops[fi]:
                contdata = parse_cont_vals(ops, filter_values, col_names)
                num_cont_cols = len(contdata)
                for col,curcdata in contdata.items():
                    cont_dtype = curcdata["dtype"]
                    cont_type = curcdata["cont_type"]
                    typeall += cont_type + ","
                    if "range" in curcdata:
                        contrange = curcdata["range"]
                        #print(contrange)
                break

        types_all.append(typeall)
        cont_types.append(cont_type)
        cont_dtypes.append(cont_dtype)
        cont_ranges.append(contrange)
        cont_cols.append(num_cont_cols)

        # allcollist = inpcols_all.split("#HASH#")
        # for col in allcollist:
            # inp_to_all_cols[inp].add(col)
        # pcols = row["PartitioningColumn"]
        # if isinstance(pcols, float):
            # curpcols = 0
        # else:
            # pcols = pcols.split(",")
            # curpcols = 0
            # for pcol in pcols:
                # pcol = pcol.replace(":ASC", "")
                # pcol = pcol.replace(":DESC", "")
                # inp_to_pcols[inp].add(pcol)
                # curpcols += 1



        #num_cols = len(set(col_names))
        #print(num_cols)
        seen_cols = []
        seen_ops = []
        num_unique_cols = 0
        num_unique_ops = 0
        num_operators = 0

        # TODO: loop over col_ops and find the appropriate String.equals etc. kind of commands
        assert len(col_ops) == len(col_names)

        for j, col in enumerate(col_names):
            if col_ops[j] is None:
                continue

            if col == "":
                colop = col_ops[j]
                if colop.lower() == "and" or colop.lower() == "or":
                    continue
                # TODO: if is_like_op(colop): then parse like colname etc.
                # other cases: etc.
                parsedcol = col
            else:
                parsedcol = col[0:col.find(":")]

            num_operators += 1
            inp_to_filter_cols[inp].add(parsedcol)
            inp_to_ops[inp].add(col_names[j] + col_ops[j])

            if col not in seen_cols:
                num_unique_cols += 1
            if col_ops[j] not in seen_ops:
                num_unique_ops += 1

            seen_cols.append(col)
            seen_ops.append(col_ops[j])

        #inp_to_num_cols[col].append(inpcols_all.count("#"))
        # num_cols_all.append(inpcols_all.count("#") + 1)
        # num_cols_sel.append(inp_sel.count("#")+1)

        num_ops_all.append(num_operators)
        num_filter_cols_all.append(num_unique_cols)
        num_unique_ops_all.append(num_unique_ops)
        # num_pcols.append(curpcols)

    print("final num decode errors: ", num_err)

    df["num_ops"] = num_ops_all
    df["num_unique_ops"] = num_unique_ops_all
    df["unique_filter_cols"] = num_filter_cols_all
    # df["num_cols_all"] = num_cols_all
    # df["num_cols_sel"] = num_cols_sel
    # df["num_pcols"] = num_pcols

    df["like_ops"] = like_ops
    df["like_lens"] = like_lens
    df["like_dtype"] = like_dtype
    df["like_kind"] = like_kind
    df["like_casting"] = like_casting
    df["like_const"] = like_const

    df["discrete_ops"] = discrete_ops
    df["discrete_eqs"] = discrete_eqs
    df["discrete_noneqs"] = discrete_noneqs
    df["discrete_type"] = discrete_types
    df["discrete_types_all"] = discrete_types_all

    df["null_checks"] = nullchecks

    df["cont_ops"] = cont_ops
    df["cont_dates"] = cont_dates
    df["cont_others"] = cont_others
    df["cont_type"] = cont_types
    df["cont_dtype"] = cont_dtypes
    df["cont_range"] = cont_ranges
    df["cont_cols"] = cont_cols

    df["types_all"] = types_all

    df["complex_ops"] = complex_ops
    df["complex_ops_num"] = complex_ops_num

    df["udf_ops"] = udf_ops
    df["in_ops"] = in_ops
    df["equal_dates"] = equal_dates
    df["num_discrete_consts"] = num_discrete_consts

# def parse_filter_exprs(df, INP_FIELD, FILTER_FIELD):
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
    # equal_dates = []
    # num_discrete_consts = []

    # num_err = 0

    # for idx, row in df.iterrows():
        # cur_row = row
        # expr = row[FILTER_FIELD]
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
            # equal_dates.append(-1)
            # complex_ops.append(-1)
            # complex_ops_num.append(-1)
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
            # continue

        # # parse d FOR cosntant values
        # filter_values = extract_values(d, "values")
        # ops = extract_values(d, "expOperator")
        # children = extract_values(d, "children")

        # likeop = 0
        # likelen = 0

        # udfop = 0
        # inop = 0
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
                # #print(child_vals)
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

            # # TODO: And + =,!= combination on same column?
            # if ops[fi] == "And":
                # continue

            # # TODO: complex predicates
            # if "&&" in ops[fi] or "##" in ops[fi]:
                # # complex predicates that are hard to parse
                # # TODO: can add threshold on length of these
                # complexpred = 1
                # complexpredlen = len(ops[fi])
                # typeall += "complex,"
                # continue

            # if "??" in ops[fi]:
                # #print(ops[fi])
                # nullcheck = 1
                # typeall += "??,"
                # continue

            # if "hasvalue" in ops[fi].lower():
                # #print(ops[fi])
                # #print(fvs)
                # nullcheck = 1
                # typeall += "hasvalue,"
                # continue

            # if "null" in ops[fi].lower():
                # #print(ops[fi])
                # #print(fvs)
                # nullcheck = 1
                # typeall += "null,"
                # continue

            # if "like" in ops[fi].lower() or "starts" in ops[fi].lower() \
                # or "contains" in ops[fi].lower() or "endswith" in ops[fi].lower():
                # if "LIKE" in inp_to_op_kind[inp]:
                    # inp_to_op_kind[inp]["LIKE"] += 1
                # else:
                    # inp_to_op_kind[inp]["LIKE"] = 1
                # likeop = 1
                # assert len(fvs) == 0
                # typeall += "like,"

                # curop = ops[fi].lower()
                # if "contains" in curop:
                    # curop = curop[curop.find("contains"):]
                    # opval = curop[curop.rfind("(")+1:-2]
                # elif "like" in curop:
                    # curop = curop[curop.find("like"):]
                    # opval = curop[curop.find("\"")+1:curop.rfind("\"")]
                    # #print(opval)
                # elif "starts" in curop:
                    # #print("*******")
                    # #print(curop)
                    # curop = curop[curop.find("starts"):]
                    # opval = curop[curop.find("\"")+1:curop.rfind("\"")]
                # elif "ends" in curop:
                    # curop = curop[curop.find("ends"):curop.find("h")]
                    # opval = curop[curop.find("\"")+1:curop.rfind("\"")]
                # else:
                    # assert False

                # if opval in inp_to_like_consts[inp]:
                    # inp_to_like_consts[inp][opval] += 1
                # else:
                    # inp_to_like_consts[inp][opval] = 1

                # likelen = len(opval)
                # continue

            # if "(" in ops[fi] and ")" in ops[fi]:
                # # FIXME: sth better.
                # udfop = 1
                # #print(ops[fi])
                # continue

            # if ops[fi] == "=" or ops[fi] == "!=":
                # datetimeop = False
                # discrete_type = get_discrete_type(fvs)
                # discrete_type_all += discrete_type + ","
                # typeall += discrete_type + ","

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

            # elif ">" in ops[fi] or "<" in ops[fi]:
                # contop = 1
                # #assert ">" in ops[fi] or "<" in ops[fi]
                # curfilterdict = inp_to_continuous_consts

                # #print(fvs)

                # if len(fvs) != 0:
                    # if "System.Date" in fvs[0]:
                        # contdate = 1
                    # else:
                        # contother = 1
                # else:
                    # continue
            # else:
                # #print(ops[fi])
                # unknown = 1
                # continue

            # for const in fvs:
                # if const in curfilterdict[inp]:
                    # curfilterdict[inp][const] += 1
                # else:
                    # curfilterdict[inp][const] = 1

        # like_ops.append(likeop)
        # like_lens.append(likelen)

        # discrete_ops.append(discreteop)
        # discrete_eqs.append(discreteeq)
        # discrete_noneqs.append(discretenoneq)
        # nullchecks.append(nullcheck)

        # cont_ops.append(contop)
        # cont_dates.append(contdate)
        # cont_others.append(contother)

        # udf_ops.append(udfop)
        # in_ops.append(inop)
        # equal_dates.append(equaldate)
        # num_discrete_consts.append(num_discrete_const)
        # discrete_types.append(discrete_type)
        # discrete_types_all.append(discrete_type_all)

        # complex_ops.append(complexpred)
        # complex_ops_num.append(complexpredlen)

        # col_names = extract_values(d, "name")
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
                        # #print(contrange)
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

        # for j, col in enumerate(col_names):
            # if col_ops[j] is None:
                # continue

            # if col == "":
                # colop = col_ops[j]
                # if colop.lower() == "and" or colop.lower() == "or":
                    # continue
                # # TODO: if is_like_op(colop): then parse like colname etc.
                # # other cases: etc.
                # parsedcol = col
            # else:
                # parsedcol = col[0:col.find(":")]

            # num_operators += 1
            # inp_to_filter_cols[inp].add(parsedcol)
            # inp_to_ops[inp].add(col_names[j] + col_ops[j])

            # if col not in seen_cols:
                # num_unique_cols += 1
            # if col_ops[j] not in seen_ops:
                # num_unique_ops += 1

            # seen_cols.append(col)
            # seen_ops.append(col_ops[j])

        # #inp_to_num_cols[col].append(inpcols_all.count("#"))
        # #num_cols_all.append(inpcols_all.count("#") + 1)
        # #num_cols_sel.append(inp_sel.count("#")+1)

        # num_ops_all.append(num_operators)
        # num_filter_cols_all.append(num_unique_cols)
        # num_unique_ops_all.append(num_unique_ops)
        # #num_pcols.append(curpcols)

    # print("final num decode errors: ", num_err)

    # df["num_ops"] = num_ops_all
    # df["num_unique_ops"] = num_unique_ops_all
    # df["unique_filter_cols"] = num_filter_cols_all
    # #df["num_cols_all"] = num_cols_all
    # #df["num_cols_sel"] = num_cols_sel
    # #df["num_pcols"] = num_pcols

    # df["like_ops"] = like_ops
    # df["like_lens"] = like_lens

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
    # df["equal_dates"] = equal_dates
    # df["num_discrete_consts"] = num_discrete_consts
