import duckdb
import datetime
import os



target_name = "sapwest_EVER"
source_name = "sapwest_EVER"
source_file = "validations\\sapwest_EVER\\20230420_150118\\compare_source.db"
target_file = "validations\\sapwest_EVER\\20230420_150118\\compare_target.db"
source_db_config = "sapwest"
target_db_config = "ss_prod"

source_duck_db = duckdb.connect(source_file)
target_duck_db = duckdb.connect(target_file)

cols_source = source_duck_db.sql(f"""select column_name
                                        from information_schema.columns
                                        where table_name='{source_name}'
                                        and column_name not in ('t')""").to_df()

cols_source_str = ','.join("coalesce(s." + cols_source["column_name"] + ",t." + cols_source["column_name"] + ")")
cols_target = target_duck_db.sql(f"""select column_name
                                        from information_schema.columns
                                        where table_name='{target_name}'
                                        and column_name not in ('t') """).to_df()
           
cols_target_str = ','.join(cols_target["column_name"])
# will do sql compare in source db
target_duck_db.close() # must close connection to attach to source db
source_duck_db.sql(f"ATTACH '{target_file}' as target")


select_coalesce_clause = ",".join("coalesce(s." + cols_source['column_name'] + ",t." + cols_source['column_name'] + ") " + cols_source['column_name']) 
join_clause = ' and '.join("s." + cols_source['column_name'] + " = t." + cols_source['column_name'])
where_clause = ""
case_clause = ""

for index in range(0,2):
        if index == 0: #do source
                case_clause = " when " + ' and '.join("s." + cols_source['column_name'] + " is null") + " then 'source'"
                where_clause = " (" + ' and '.join("s." + cols_source['column_name'] + " is null") + ") "
        elif index == 1: #do target
                case_clause += " when " +' and '.join("t." + cols_source['column_name'] + " is null") + " then 'target'"
                where_clause += "or (" + ' and '.join("t." + cols_source['column_name'] + " is null") + ") "

sql = f"""select {select_coalesce_clause}, 1 cnt
                                , case {case_clause} end missing_in
                                from {source_name} s
                                        full outer join target.main.{target_name} t 
                                                on {join_clause} 
                                where {where_clause}"""

diffs = source_duck_db.sql(sql).to_df()

source_duck_db.sql(f"DETACH target")
diffs["name"] = source_name
diffs["source_db"] = source_db_config
diffs["target_db"] = target_db_config
diffs["capture_time"] = datetime.datetime.now()
cols = diffs.loc[:, ~diffs.columns.isin(['name','capture_time', 'source_db', 'target_db', 'column0', 'missing_in', 'filename', 'cnt'])].columns.values.tolist()
cols = list(map(lambda x: f"concat('{x}=',{x})", cols))
cols_str = ",',',".join(cols)
tf_diffs = duckdb.sql(f"SELECT name, source_db, target_db, missing_in, cast(cnt as bigint) cnt, capture_time, concat({cols_str}) cols FROM diffs").to_df()
tf_diffs.to_csv(os.path.join(os.path.dirname(target_file),f"diff_detail_{target_name}.csv"))

