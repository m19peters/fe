import dvase
from pandas.testing import assert_frame_equal
import time
import os
import duckdb
import sqlglot

class validator:
        def __init__(self, source_validator: dvase.dv, target_validator: dvase.dv):
                # db_config_name: str, duck_file: str, source_query: str, name: str
                if (source_validator.summary_df.empty):
                        self.source_validator = dvase.dv(db_config_name=source_validator.source_db.db_config_name
                                                         , duck_file = source_validator.duck.file
                                                         , source_query = source_validator.source_db.query
                                                         , name = source_validator.name
                                                         , thread = source_validator.thread_cnt
                                                         , start_time = source_validator.start_time)
                else:
                        self.source_validator = source_validator
        
                if (target_validator.summary_df.empty):
                        self.target_validator = dvase.dv(db_config_name = target_validator.source_db.db_config_name
                                                         , duck_file= target_validator.duck.file
                                                         , source_query= target_validator.source_db.query
                                                         , name = target_validator.name
                                                         , thread = target_validator.thread_cnt
                                                         , start_time = target_validator.start_time
                                                         , comparison_summary_df = self.source_validator.summary_df) ## change this to have a flag in config file to do this type of comparison
                else:
                        self.target_validator = target_validator

               
                
        def validate_summary(self):
                target_df = self.target_validator.summary_df
                source_df = self.source_validator.summary_df
                matching = True
                error = ""
                diff_cnt = 0
                pct = 100
                diff_indexes = []

                if (len(target_df)==len(source_df)):
                        target_compare_cols = target_df.loc[:, ~target_df.columns.isin(['parallel_query','parallel_by', 'parallel_n', 'db', 'name', 'capture_time'])]
                        print(target_compare_cols)
                        source_compare_cols = source_df.loc[:, ~source_df.columns.isin(['parallel_query','parallel_by', 'parallel_n', 'db', 'name', 'capture_time'])]
                        print(source_compare_cols)

                        try:
                                assert_frame_equal(target_compare_cols, source_compare_cols, check_dtype=False)
                        except AssertionError as e:
                                error = e
                                matching = False
                                src_cnt = source_compare_cols["cnt"].sum()
                                trg_cnt = target_compare_cols["cnt"].sum()
                                diff_cnt = src_cnt - trg_cnt
                                if (src_cnt!=0):
                                        pct = trg_cnt / src_cnt
                                else:
                                        pct = 0

                                diff = source_compare_cols.compare(target_compare_cols)
                                diff_indexes = diff.index.tolist()

                duckdb.sql("""select s.name, s.db source, t.db target, s.source_row_count, t.target_row_count, s.capture_time
                              from
                                (select name, db, sum(cnt) source_row_count, max(capture_time) capture_time
                                from source_df 
                                group by name, db) s
                                        inner join 
                                (select name, db, sum(cnt) target_row_count, max(capture_time) capture_time 
                                from target_df 
                                group by name, db) t on s.name=t.name
                        """).to_csv(os.path.join(self.target_validator.output_dir, f"diff_summary_{self.target_validator.name}.csv"), sep=",",header=True)

                if error:
                        with open(os.path.join(self.target_validator.output_dir, f"diff_summary_{self.target_validator.name}_error.txt"), "w") as o:
                                o.write(str(error))

                return matching, error, diff_cnt, pct, diff_indexes

        def validate_detail(self):
                self.target_validator.run_load()
                self.source_validator.run_load()
                target_name = self.target_validator.name
                source_name = self.source_validator.name

                source_duck_db = self.source_validator.duck.database
                target_duck_db = self.target_validator.duck.database

                cols_source = source_duck_db.sql(f"""select column_name
                                        from information_schema.columns
                                        where table_name='{source_name}'
                                        and column_name not in ('t')""").to_df()

                # will do sql compare in source db
                target_duck_db.close() # must close connection to attach to source db
                source_duck_db.sql(f"ATTACH '{self.target_validator.duck.file}' as target")

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

                diffs["name"] = self.target_validator.name
                diffs["source_db"] = self.source_validator.source_db.db_config_name
                diffs["target_db"] = self.target_validator.source_db.db_config_name
                diffs["capture_time"] = self.target_validator.start_time


                cols = diffs.loc[:, ~diffs.columns.isin(['name','capture_time', 'source_db', 'target_db', 'column0', 'missing_in', 'filename', 'cnt'])].columns.values.tolist()
                cols = list(map(lambda x: f"concat('{x}=',{x})", cols))

                cols_str = ",',',".join(cols)
                tf_diffs = duckdb.sql(f"SELECT name, source_db, target_db, missing_in, cast(cnt as bigint) cnt, capture_time, concat({cols_str}) cols FROM diffs").to_df()

                tf_diffs.to_csv(os.path.join(self.target_validator.output_dir, f"diff_detail_{self.target_validator.name}.csv"))

                return len(diffs)
        


