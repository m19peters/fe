import sqlglot

sql1 = """select MANDT as blah,ANLAGE from SAPR3.EANL where MANDT='010' AND ANLAGE BETWEEN '0700733746' and '0700000000'"""

sql2 = """select MANDT,ANLAGE from DATA_LAKE.sapeast_EANL where MANDT='010' AND ANLAGE BETWEEN '0' and '0700000000'"""

sqlg1 = sqlglot.parse_one(sql1, sqlglot.Dialects.ORACLE)
sqlg2 = sqlglot.parse_one(sql2, sqlglot.Dialects.MYSQL)

# change sql2 to be count only and to change filter based on sql1... then run and compare count with sql1 count
sqlg2.args['where'] = sqlg1.args['where']

sql2 = sqlg2.sql()

sql2 = f"select count(*) cnt from ({sql2}) xxxx"

print(sql1)
print(sql2)