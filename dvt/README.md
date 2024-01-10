# Data Validation Utility (DVU)

This data validation tool is used to compare two queries to detect differences. At first it goes and queries the "source" system from a defined query passed in either via config file or via command line arguments.  It then runs the target query that was passed in as well. To "snapshot" each environment. N_TILE is used in the database to split the query up into min and max for the keys that were passed in via the PARALLEL option in the query.  Resulting in something that looks like this:

Source:

    n mandt_mx mandt_mn            objek_mx            objek_mn    atinn_mx    atinn_mn atzhl_mx atzhl_mn mafid_mx mafid_mn klart_mx klart_mn adzhl_mx adzhl_mn       cnt
0   1      010      010  000000000001279599                      0000001438  0000000000      005      001        O        0      DEV      001     0000     0000  19238104
1   2      010      010  000000000002828066  000000000001279599  0000001438  0000000000      003      001        O        0      DEV      DEV     0000     0000  19238104
2   3      010      010  000000000003411064  000000000002828066  0000001438  0000000000      002      001        O        O      DEV      DEV     0000     0000  19238104
3   4      010      010  000000000004106765  000000000003411064  0000001438  0000000622      003      001        O        0      DEV      DEV     0000     0000  19238104
4   5      010      010  000000000004795909  000000000004106766  0000001438  0000000000      002      001        O        O      DEV      DEV     0000     0000  19238104
5   6      010      010  000000000005547515  000000000004795909  0000001449  0000000622      002      001        O        O      DEV      DEV     0000     0000  19238104
6   7      010      010     DEV000000342115  000000000005547515  0000001450  0000000000      004      000        O        0      DEV      001     0000     0000  19238104
7   8      010      010     DEV000001055633     DEV000000342115  0000001438  0000000000      004      001        O        0      DEV      002     0000     0000  19238104
8   9      010      010     DEV000001795102     DEV000001055633  0000001438  0000000000      006      001        O        0      DEV      002     0000     0000  19238104
9  10      010      010            XXX92075     DEV000001795102  0000001438  0000000000      870      001        O        0      DEV      002     0000     0000  19238104

Target:

    n mandt_mx mandt_mn            objek_mx            objek_mn    atinn_mx    atinn_mn atzhl_mx atzhl_mn mafid_mx mafid_mn klart_mx klart_mn adzhl_mx adzhl_mn       cnt
0   1      010      010  000000000001279599                      0000001438  0000000000      005      001        O        0      DEV      001     0000     0000  19238103
1   2      010      010  000000000002828066  000000000001279599  0000001438  0000000000      003      001        O        0      DEV      DEV     0000     0000  19238103
2   3      010      010  000000000003411064  000000000002828066  0000001438  0000000000      002      001        O        O      DEV      DEV     0000     0000  19238103
3   4      010      010  000000000004106765  000000000003411064  0000001438  0000000622      003      001        O        0      DEV      DEV     0000     0000  19238103
4   5      010      010  000000000004795909  000000000004106766  0000001438  0000000000      002      001        O        O      DEV      DEV     0000     0000  19238103
5   6      010      010  000000000005547516  000000000004795909  0000001449  0000000622      002      001        O        O      DEV      DEV     0000     0000  19238103
6   7      010      010     DEV000000342115  000000000005547516  0000001450  0000000000      004      000        O        0      DEV      001     0000     0000  19238103
7   8      010      010     DEV000001055633     DEV000000342115  0000001438  0000000000      004      001        O        0      DEV      002     0000     0000  19238103
8   9      010      010     DEV000001795102     DEV000001055633  0000001438  0000000000      006      001        O        0      DEV      002     0000     0000  19238103
9  10      010      010            XXX92075     DEV000001795102  0000001438  0000000000      870      001        O        0      DEV      002     0000     0000  19238103

These queries were N_TILE'd by 10 buckets.  The example of what was passed in for this to occur was:

- source = select MANDT,OBJEK,ATINN,ATZHL,MAFID,KLART,ADZHL from SAPR3.AUSP where MANDT='010' AND /* PARALLEL(BY=MANDT,OBJEK,ATINN,ATZHL,MAFID,KLART,ADZHL;) */
- target = select MANDT,OBJEK,ATINN,ATZHL,MAFID,KLART,ADZHL from DATA_LAKE.sapeast_AUSP where MANDT='010' AND /* PARALLEL(BY=MANDT,OBJEK,ATINN,ATZHL,MAFID,KLART,ADZHL;) */

MANDT,OBJEK,ATINN,ATZHL,MAFID,KLART,ADZHL are the primary keys of this table.  You should always parallel by the primary key of the table if possible. If it is passed in to do detailed analysis and if there are detected differences. Then the values above for each key will be used to query the database.  For example on the target this would run for row 2:

select MANDT,OBJEK,ATINN,ATZHL,MAFID,KLART,ADZHL from DATA_LAKE.sapeast_AUSP where MANDT='010' AND  coalesce(MANDT,'') BETWEEN '010' AND '010' AND  coalesce(OBJEK,'') BETWEEN '000000000002828066' AND '000000000003411064' AND  coalesce(ATINN,'') 
BETWEEN '0000000000' AND '0000001438' AND  coalesce(ATZHL,'') BETWEEN '001' AND '002' AND  coalesce(MAFID,'') BETWEEN 'O' AND 'O' AND  coalesce(KLART,'') BETWEEN 'DEV' AND 'DEV' AND  coalesce(ADZHL,'') BETWEEN '0000' AND '0000';

Each query will execute in it's own thread.  You have the option of defining how many threads via the command line interface. Default is 10.

To keep memory consumption at a minimum. Data is batched from the database and inserted into a duckdb file.  There is a compare_source.db and compare_target.db file generated for each "validation" that at the end of the load will contain all rows. Once all rows from target and source are ingested into duckdb. The compare_target.db is attached to compare_source.db where analysis is done to see what rows are different.

## output

Data is output to the validations folder. The name of the validation is used as the top level folder and then a time stamped folder contains the duckdb files as well as the csv's containing the comparison results.

- compare_source.db contains source detail data
- compare_target.db contains target detail data
- diff_summary_{name}.csv contains the summary stats for the initial compare
- diff_detail_{name}.csv contains keys missing in either target or source

This csv files are brought together at the end of the run and then merged with the current date file out in azure.

## python modules

- dbase.py acts as an interface between the db_oracle.py and db_ss.py modules. To extend to other databases simply follow pattern of db_oracle/db_ss and implement a sqlalchemy engine. 
- db_duck.py contains file location and the connection to the duckdb files
- dvase.py contains most of the logic to create the queries needed. And for the detail load contains the logic for multi-threading.
- validate.py contains the logic to compare target and source and outputs the csv's
- runner.py contains the logic to orchestrate and multi-thread the "validations" that are passed in during runtime
- dvu.py is the main cli. There are two options for running the utility. "run-config" and "run". run-config expects a json file containing "validations" to run. run will execute the single validation you pass in via command line arguments.  see: python dvu.py run --help or python dvu.py run-config --help to see arguments.


## TO DOS:

- feature: with the above summary result... we should be able to detect where things are different. Meaning right now if 10 rows are missing in target like the output suggests. Use the source where clauses to query the target in the same way you would source. So as to see of the 10 threads/buckets where counts are different. Then you should be able to narrow only in on those buckets instead of running all 10 queries.
- bug: currently saving data to azure could have issues with concurrency. If multiple runs of the app are running. As it saves and concatenates to a daily file. If another process happens to be saving at the same time data loss could occur.
- feature: secrets... right now the user and pw for each defined database in db.json is stored in clear text in file. Must either use env variables or download from a secrets engine.
- feature: more robust handling of coalescing in sql generation for the where clauses.  Right now only string and integers are handled.
- feature: implement on python server
- feature: schedule in autosys
- feature: deploy powerbi dashboard to azure
- bug: with long running pulls I've seen periodically a termination of a session to the database.  further investigation is needed.  https://stackoverflow.com/questions/71635261/python-long-idle-connection-in-cx-oracle-getting-dpi-1080-connection-was-close
- feature: take IDR config xml document and output a useable json config file for DVU.
- feature: implement other needed connectors (eg. azure db)
