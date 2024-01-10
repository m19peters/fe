from enums import DbType
import db_duck
import dbase
from concurrent.futures import ThreadPoolExecutor
import datetime
import time
import os
import pandas as pd


class dv:
    replace_me_phrase = "'<plogic>'='<plogic>'"

    def __init__(self, db_config_name: str, duck_file: str, query: str, name: str, threads: int, start_time : datetime, is_source: bool, comparison_summary_df: pd.DataFrame = pd.DataFrame()):
        self.start_time = start_time
        
        if not os.path.exists("validations"):
            os.mkdir("validations")
        
        if not  os.path.exists(os.path.join("validations",name)):
            os.mkdir(os.path.join("validations",name))

        if not  os.path.exists(os.path.join("validations",name, str(self.start_time.strftime('%Y%m%d_%H%M%S')))):
            os.mkdir(os.path.join("validations",name, str(self.start_time.strftime('%Y%m%d_%H%M%S'))))

        self.root_output_dir = os.path.join("validations", name)
        self.output_dir = os.path.join(self.root_output_dir, str(self.start_time.strftime('%Y%m%d_%H%M%S')))
        
        self.source_db = dbase.db(db_config_name, query, name)
        self.duck =  db_duck.db(os.path.join(self.output_dir, duck_file))
        self.name = name
        self.thread_cnt = threads
        self.summary_df, self.can_do_detail = self.get_summary_data(comparison_summary_df)
        self.is_source = is_source
               
    def run_load(self):

        start = time.time()
        print(f"Starting at - {datetime.datetime.now()}")  

        df = self.summary_df
        row_cnt = 0
        part_cnt = 0
        duck_dbs = []

        # start threading from query meta data
        with ThreadPoolExecutor(max_workers=6) as e:
            parts = len(df)
            threads = []
            for indx,row in df.iterrows():
                threads.append(e.submit(self.getData, row["parallel_query"], row["n"], self.source_db.db_config_name, self.source_db.name, self.duck.file))

            for thread in threads:  
                part_row_cnt, duck_file, duck_table = thread.result() 
                duck_dbs.append({ "file" : duck_file, "table" : duck_table, "cnt": part_row_cnt })          
                row_cnt += part_row_cnt
                part_cnt += 1
                print(f"Loaded all rows from source ({part_row_cnt} rows - partition {part_cnt} out of {parts}) in {time.time() - start}ms")

        print(f"Loaded all rows from source ({row_cnt} rows) in {time.time() - start} sec")
        
        view_lst = []
        # attach individual files
        for db in duck_dbs:
            self.duck.database.sql(f"ATTACH '{ db['file'] }' as { db['table'] }")
            view_lst.append(f"select * from {db['table']}.main.{db['table']}")

        # logically consolidate by creating view over loaded data
        self.duck.database.sql(f"""DROP TABLE IF EXISTS {self.name};
                                    CREATE TABLE {self.name} as 
                                    {" UNION ALL ".join(view_lst)}
                                    ;""")
        
        # delete database files 
        for db in duck_dbs:
            self.duck.database.sql(f"DETACH { db['table'] }")
            os.remove(db['file'])
            if os.path.exists(db['file']+".wal"):
                os.remove(db['file']+".wal")

        print(f"Finished physically consolidating to {self.name}")

    def getData(self, query:str, part:int, db_config_name:str, validation_name:str, duck_file:str):

        db = dbase.db(db_config_name, query, validation_name)
        duck_part_file_name = f"{duck_file}_{validation_name}_{part}"
        duck = db_duck.db(duck_part_file_name)
        create = True
        table_name = f"{db.name}_{part}"
        duck.database.sql(f"drop table if exists {table_name}")
        i:int=0

        print(f"Running sql {query}")
        
        for chunk in db.load():        
            print(f"Fetched {db.name} {len(chunk)} rows - {datetime.datetime.now()}")  

            if create:
                create=False
                duck.database.sql(f"create table {table_name} as select '{table_name}' t, * from chunk")
            else:
                duck.database.sql(f"insert into {table_name} select '{table_name}' as t, * from chunk")

            i=i+len(chunk)
            print(f"Inserted {db.name} into duckdb... {i} total rows - {datetime.datetime.now()}")       
        
        return i, duck_part_file_name, table_name
    
    # TO DO make this better and handle other types ... THIS PROBABLY NEEDS TO BE ON db object as well so can be defined there
    def coalesce_sql_col(self, col_name, dbtyp):
        good_val = col_name

        if (self.source_db.type == DbType.db_oracle):
            if dbtyp == int or dbtyp == float:
                good_val = f"coalesce({col_name},0)"
            else:
                good_val = f"coalesce({col_name},'')"
        elif (self.source_db.type == DbType.db_ss):
            if dbtyp == int or dbtyp == float:
                good_val = f"coalesce({col_name},0)"
            else:
                good_val = f"coalesce({col_name},'')"

        return good_val

    def get_summary_data(self, comparison_summary_df : pd.DataFrame = pd.DataFrame()):
        db = self.source_db
        parallel_by = ""
        parallel_query = db.query
        can_do_detail = True
        #latest_summary_path = os.path.join(self.root_output_dir, f"latest_summary_{self.name}.pkl")
                                           
        # example for partitioning loads 
        # "select MANDT, VERTRAG FROM SAPR3.EVER WHERE X=1 AND /* PARALLEL(BY=MANDT,VERTRAG;) */ AND SOMETHINGELSE=1"

        #if comparison_summary_df.empty:
        #   if os.path.isfile(latest_summary_path):
        #       # check if no older than 2 days old.. if not then use it
        #       if (os.path.getmtime(latest_summary_path) > time.time() - 2*86400):
        #           comparison_summary_df = pd.read_pickle(latest_summary_path)

        n_tile_query, parallel_by, parallel_query = self.get_ntile_parallel_query()

        if not comparison_summary_df.empty: # get row counts based on partition counts from other source            
            # recreate summary df from comparison for other source
            df = self.get_parallel_dataframe(n_tile_query,parallel_query,parallel_by,comparison_summary_df)     
            df = self.get_refresh_counts_for_summary(df)
        elif n_tile_query:
            df = self.get_parallel_dataframe(n_tile_query,parallel_query,parallel_by)

            # save if using comparison so next time through we can use as well... use pickle to ensure types stay
            #df.to_pickle(latest_summary_path) 
        else:
            print(f"running: {db.query}")
            df = db.read_sql(db.query)
            can_do_detail = False           
        
        return df, can_do_detail
    
    def get_ntile_parallel_query(self):
        db = self.source_db
        parallel_by = ""
        parallel_query = db.query

        # example for partitioning loads 
        # "select MANDT, VERTRAG FROM SAPR3.EVER WHERE X=1 AND /* PARALLEL(BY=MANDT,VERTRAG;) */ AND SOMETHINGELSE=1"

        p_loc_start = db.query.find("/* PARALLEL(")

        if p_loc_start>0:
            p = db.query[p_loc_start+12:] # get rid of previous string and parallel part
            p_loc_end = p.find("*/")
            
            if p_loc_end>0:
                p = p[:p_loc_end] # get rid of end to isolate the parallel options and get rid of any other things left over

                for parm in p.split(";"):
                    key = parm[:parm.find("=")]
                    val = parm[parm.find("="):]

                    if key.upper() == "BY":
                        parallel_by = str(val.replace("=",""))

        if len(parallel_by)>0:
            replace_me = parallel_query[p_loc_start:p_loc_end+p_loc_start+14]
            parallel_query = parallel_query.replace(replace_me, self.replace_me_phrase)

            # find select clause to then insert n_tile right after
            select_loc = parallel_query.lower().find("select")
            ntile_cmd = f"""NTILE({self.thread_cnt}) OVER (ORDER BY {parallel_by}) n,"""
            ntile_base_query = f"""{parallel_query[:select_loc+7] + ntile_cmd + parallel_query[select_loc+7:]}"""        
            
            # get max cmd for grouping of each partition
            bounds_cmd = ""
            for col in parallel_by.split(","):
                bounds_cmd += "MAX(" + col + ") " + col + "_mx, " + "MIN(" + col + ") " + col + "_mn,"
            
            # set query for n_tile
            n_tile_query = f"""SELECT n, {bounds_cmd} count(*) cnt
                            FROM ({ntile_base_query}) abc_xyz
                            GROUP BY n
                            ORDER BY n
                            """
        else:
            n_tile_query = ""

        return n_tile_query, parallel_by, parallel_query
         
    def get_parallel_dataframe(self, n_tile_query: str, parallel_query: str, parallel_by: str, summary_comparison_dataframe: pd.DataFrame = pd.DataFrame()):
        db = self.source_db

        if not summary_comparison_dataframe.empty:
             df = summary_comparison_dataframe
        else:
            print(f"n_tile_query running: {n_tile_query}")
            df = db.read_sql(n_tile_query)

        replace_me_loc = parallel_query.find(self.replace_me_phrase)

        df["parallel_query"] = parallel_query
        df["parallel_by"] = parallel_by
        df["parallel_n"] = self.thread_cnt

        #fix column headers and nulls
        df.columns = [x.lower() for x in df.columns]

        #fix nan to default vals
        for col in parallel_by.split(","):
            dt = df[col.lower()+"_mx"].dtype 
            if dt == int or dt == float:
                df[col.lower()+"_mx"].fillna(0, inplace=True)
                df[col.lower()+"_mn"].fillna(0, inplace=True)
            else:
                df[col.lower()+"_mx"].fillna("", inplace=True)
                df[col.lower()+"_mn"].fillna("", inplace=True)

        
        for idx, row in df.iterrows():     
            # only make parallel query for "non-outlier" rows 
            if idx < self.thread_cnt:   
                # create and sql for filters
                and_cmd = ""

                for col in parallel_by.split(","):
                    #fix sql col to correct isnull syntax
                    dt = df[col.lower()+"_mx"].dtype
                    and_cmd += f""" {self.coalesce_sql_col(col, dt)} BETWEEN '{row[col.lower()+"_mn"]}' AND '{row[col.lower()+"_mx"]}' AND """

                and_cmd += " 1=1 "

                df.at[idx, "parallel_query"] = parallel_query[:replace_me_loc] + and_cmd + parallel_query[replace_me_loc+len(self.replace_me_phrase):]

                    
        df["db"] = self.source_db.db_config_name
        df["name"] = self.name
        df["capture_time"] = self.start_time

        # add < min search and > max search to find outliers if the two new rows haven't been added
       
        # create list to interate through to generate 2 rows
        lt_gt = [{"indx":0,"sign":"<","set_col":"_mx","get_col":"_mn"},{"indx":1,"sign":">","set_col":"_mn","get_col":"_mx"}]
        new_rows = df.iloc[[0,len(df)-1]].copy(deep=True).reset_index(drop=True)
        new_rows["cnt"] = 0

        for sign in lt_gt:                   
            and_cmd = " ("

            for col in parallel_by.split(","):
                dt = df[col.lower()+"_mx"].dtype

                # set where clause for correct sign and value
                new_rows.loc[sign["indx"], col.lower()+sign["set_col"]] = new_rows.iloc[sign["indx"]][col.lower()+sign["get_col"]]
                and_cmd += f""" {self.coalesce_sql_col(col, dt)} {sign['sign']} '{new_rows.loc[sign["indx"]][col.lower()+sign["get_col"]]}' OR """
            
            # remove extra "OR"
            and_cmd = and_cmd[:-3] + ") "
            # set parallel query to correct value for particular row
            new_rows.loc[sign["indx"],"parallel_query"] = parallel_query[:replace_me_loc] + and_cmd + parallel_query[replace_me_loc+len(self.replace_me_phrase):] 

        # put the two new outer bound rows into dataframe... if don't exist the union together... otherwise replace
        if self.thread_cnt==len(df):
            df = pd.concat([df, new_rows],ignore_index=True)   
        else:
            df.loc[len(df)-2,df.columns] = new_rows.loc[0]
            df.loc[len(df)-1,df.columns] = new_rows.loc[1]

        df = df.reset_index(drop=True)
        df["n"] = df.index   

        return df

    def get_refresh_counts_for_summary(self, summary_dataframe: pd.DataFrame):
        part_cnt = 0
        df = summary_dataframe

        # start threading from query meta data
        with ThreadPoolExecutor(max_workers=6) as e:
            threads = []
            for indx,row in df.iterrows():
                threads.append(e.submit(self.get_parallel_query_count, row['parallel_query'], indx))

            for thread in threads:  
                summary_count_df = thread.result()  
                i = summary_count_df.iat[0,0]
                df.at[i, "cnt"] = summary_count_df.iat[0,1]
                part_cnt += 1

        return df
    
    def get_parallel_query_count(self, query:str, indx:int):
        sql = f"select {indx} indx, coalesce((select count(*) from ({query})),0) cnt" 

        if self.source_db.type is DbType.db_oracle:
            sql += " FROM DUAL"

        df = self.source_db.read_sql(sql)

        return df