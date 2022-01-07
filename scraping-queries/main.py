from multiprocessing.pool import ThreadPool
import requests
import sys
from bs4 import BeautifulSoup, SoupStrainer
import bs4
import httplib2
import time
import random
import os

LINK_TMP="""https://data.stackexchange.com/stackoverflow/queries?order_by=popular&page={PAGE}&pagesize=50"""

FORK_TMP="https://data.stackexchange.com/stackoverflow/query/fork/{QID}"
OUT_DIR = "./sqls/"
FN_TMP = OUT_DIR + "{pg}-{snum}-{qid}.sql"

start_page = int(sys.argv[1])
end_page = int(sys.argv[2])
print("From page: ", start_page, " to ", end_page)

def extract_sql(qid):
    time.sleep(random.randint(0,3))
    furl = FORK_TMP.format(QID=qid)
    page = requests.get(furl)
    soup = BeautifulSoup(page.content, "html.parser")
    info = soup.find("textarea", {"name": "sql"})
    return info.text

def is_int(num):
    try:
        int(num)
        return True
    except:
        return False

def download_all_sqls_in_page(pagenum, dummy):
    link = LINK_TMP.format(PAGE=pagenum)
    print(link)
    http = httplib2.Http()
    status, response = http.request(link)
    print("Status: ", status)
    sqlnum = 0
    for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
        print(type(link))
        if isinstance(link, bs4.element.Doctype):
            continue

        if link.has_attr('href'):
            clink = link["href"]
            if "query" in clink:
                #print(clink)
                qstart = clink.find("query/")
                clink = clink[qstart+6:]
                qend = clink.find("/")
                print(clink)
                qid = clink[0:qend]
                if is_int(qid):
                    print(int(qid))
                    sql = extract_sql(qid)
                    print("**********")
                    print(sql)
                    print("**********")
                    # TO SAVE
                    sqlnum += 1
                    fn = FN_TMP.format(pg=pagenum, snum = sqlnum, qid=qid)
                    with open(fn, "w") as f:
                        f.write(sql)
                else:
                    print("skipping query: ", link["href"])
            elif "revision" in clink:
                print("skipping revision: ", link["href"])


par_args = []
for i in range(start_page, end_page+1,1):
    par_args.append((i, "test"))

pool = ThreadPool(4)
pool.starmap(download_all_sqls_in_page, par_args)
