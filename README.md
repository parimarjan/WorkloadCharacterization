# WorkloadCharacterization

### PostgreSQL setup

A lot of the scripts rely on PostgreSQL;

* Follow instructions at: https://github.com/learnedsystems/ceb (under docker
setup, to setup PostgreSQL IMDb in a docker container)

Then in the following scripts, set the user,pwd,port appropriately to generate
the ground truth data.

* Downloading SQLs: for IMDb workloads, we can use:
  * https://github.com/learnedsystems/CEB/blob/main/scripts/download_all_sqls.sh
  * Put these SQLs in the same directory (set up ParsingSQLs in next step to
      find the correct directory with SQLs in the first cells)

### Creating workload files from sqls

Two steps, first parse sqls to extract expressions (similar to how they look
    like in SCOPE)

* Use ParsingSQLs.ipynb for general SQLs (tested on imdb,tpcds etc.)

* (OR ParsingSQLs-zdbs.ipynb which are hardcoded for ziniu's db instances)

* At this point, expr\_df.csv should have been generated. Then collect the
cardinality estimate for each expression using:

* ```bash python3 get_rowcounts.py``` w/ appropriate GLOBAL variables set in
the script (WK=ceb global variable)
* ```bash python3 get_const_rowcounts.py``` ---> produces literal\_df.csv,
  which adds additional cardinality data with single constants to expr\_df.csv

(for ziniu's db instances:)
* ```bash python3 get_rowcounts_zdbs.py```


### Running Evaluation for Generated Data


#### IMDb version

* have data files, `n.csv` etc.
* create table using a data file and evaluate on it.
  * ```bash
  python3 create_table.py --inp_fn data/gen_data/new_data3/n.csv --port 5432 --data_kind gen_shuffle
  python3 eval_data.py --inp_to_eval n --data_kind gen_shuffle --num_queries 100 --port 5432
  ```
  * --port 5432 is set on tebow to standard postgresql on docker; --port 5434 is set to 512mb limit version
  * --data_kind
    * --data_kind gen_shuffle ---> uses generated data file but just shuffles values
    * --data_kind gen_shuffle2 ---> uses generated data file + replaces NULLs w/ random values + shuffles

* create table using random values from the domain + evaluate

  * E.g. of evaluating data gene
  ```bash
  python3 create_table.py --inp_fn n.csv --port 5432 --data_kind random_domain2
  python3 eval_data.py --inp_to_eval n --data_kind random_domain2 --num_queries 100 --port 5432
  ```

#### SOSD version

### Brief Notes

* For Non-SCOPE workloads, ParsingSQLs.ipynb file should handle going from SQL
strings to op\_df.csv, expr\_df.csv files
* For SCOPE workloads, handled in filter\_constants.ipynb;
* TODO: need to clean up other SCOPE analysis files to be more consistent etc.

* For CEB, IMDb, we load in the cardinalities from the qrep objects; For tpcds
etc., we use the script get\_rowcounts.py with the appropriate WK setup to get
the cardinalities. Notice: this requires the creation of the same scale DB etc.
* TODO: tpch / tpcds parsing needs to handle edge cases better; seems to be
accidentally converting OR statements to AND? Sanity check: no single table cardinality should be 0 in expr\_df.csv

* expr\_df.csv ---> literal\_df.csv is done in get\_const\_rowcounts.py
