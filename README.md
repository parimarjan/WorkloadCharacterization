# WorkloadCharacterization

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
