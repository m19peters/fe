import datetime
import duckdb
import dvase
import time
import validate
import azure_client
import os
from concurrent.futures import ThreadPoolExecutor
import shutil
import hashlib
import pandas as pd
import random 

class validation_runner:
     
    def run_validations(self, validations):        
        with ThreadPoolExecutor(max_workers=3) as e:
            threads = []
            for i in validations:
                threads.append(e.submit(self.run_validation
                                            ,name=i["name"]
                                            ,source_db_name=i["source_db_name"]
                                            ,source_query=i["source_query"]
                                            ,target_db_name=i["target_db_name"]
                                            ,target_query=i["target_query"]
                                            ,duck_file=i["duck_file"]
                                            ,threads=i["threads"]
                                            ,run_detail=i["run_detail"])
                                )

            for thread in threads:  
                good = thread.result() 
            
    # compare_type(s): source, full, target
    def run_validation(self, name: str, source_db_name:str, source_query: str, target_db_name:str, target_query: str, duck_file: str, threads: int, run_detail:bool=False, compare_type:str="source"):
        try:
            good = True
            start = time.time()
            start_dt = datetime.datetime.now()

            comparison_summary_fname = ""
            compare_order = ["source", "target"]

            if compare_type=="source":
                comparison_summary_fname = f"smc_{name}_{hashlib.md5(bytes(source_query, 'utf-8')).hexdigest()}.parquet"
            elif compare_type=="target":
                comparison_summary_fname = f"smc_{name}_{hashlib.md5(bytes(target_query, 'utf-8')).hexdigest()}.parquet"
                compare_order = ["target", "source"]
            
            comparison_summary_expired = True 
            comparison_summary_df = self.download_comparison_summary(comparison_summary_fname)
            if not comparison_summary_df.empty:
                capture_time = comparison_summary_df.iloc[0]["capture_time"]
                delta = start_dt - capture_time
                if delta.days <= 7:
                    comparison_summary_expired = False

            first = True

            for compare in compare_order:                       
                if compare=="source":
                    source_validator = dvase.dv(db_config_name=source_db_name
                                , duck_file = f"{duck_file}_source.db"
                                , query = source_query
                                , name = name
                                , threads = threads
                                , is_source = True
                                , start_time = start_dt
                                , comparison_summary_df = comparison_summary_df)
                    
                    if first and compare_type in ["source","target"]:
                        comparison_summary_df = source_validator.summary_df.copy(deep=True)
                        first = False
                if compare=="target":
                    target_validator = dvase.dv(db_config_name=target_db_name
                                , duck_file = f"{duck_file}_target.db"
                                , query = target_query
                                , name = name
                                , threads = threads
                                , start_time = start_dt
                                , is_source = False
                                , comparison_summary_df = comparison_summary_df)
                    
                    if first and compare_type in ["source","target"]:
                        comparison_summary_df = target_validator.summary_df.copy(deep=True)
                        first = False

            v = validate.validator(source_validator=source_validator, target_validator=target_validator)
      
            matching, error, diff_cnt, pct, diff_indexes = v.validate_summary()
            if not comparison_summary_df.empty and comparison_summary_expired:
                self.upload_comparison_summary(comparison_summary_fname, comparison_summary_df)
            
            if (not matching):
                    print("Summary of source and target does not match!!")
                    print(error)
                    print(f"Rows difference (source - target) is {diff_cnt}")

                    if (run_detail and source_validator.can_do_detail and target_validator.can_do_detail):
                        print("Running detailed analysis to find which rows are missing...")

                        if (len(diff_indexes)>0):                         
                            print(f"Differences was: {diff_indexes}")
                            diff_indexes = random.choice(diff_indexes)
                            print(f"Differences now after random choice is: {diff_indexes}")
                            v.source_validator.summary_df = v.source_validator.summary_df.iloc[[diff_indexes]]
                            v.target_validator.summary_df = v.target_validator.summary_df.iloc[[diff_indexes]]

                        detail_diff_cnt = v.validate_detail()
                        print(f"Diff file written, there were {detail_diff_cnt} rows determined as being different... {pct} matching")
                    else:
                        print("Detail skipped as it's setting run_detail is false... run at another time")
            else:
                    print(f"Determined source and target are {pct} matching")
            
            print(f"Comparison took {time.time() - start} sec")
        except Exception as e:
            good = False
            print(f"Error: {e}")
            with open("error.txt", "a") as o:
                o.write(f"Error caught for {name} at {datetime.datetime.now()} ")
                o.write(str(e))
        try:
            if  source_validator.duck.database:
                source_validator.duck.database.close()
        except Exception as e:
            print("Warning: Error thrown when cleaning up source connections to duckdb... this may be ignored.")

        try:
             if  target_validator.duck.database:
                target_validator.duck.database.close()
        except Exception as e:
            print("Warning: Error thrown when cleaning up target connections to duckdb... this may be ignored.")
           
        return good
    
    def download_comparison_summary(self, comparison_summary_fname:str):
        az_path = f"data_lake/bronze/validator/summary_frame/{comparison_summary_fname}"
        local_tmp_path = os.path.join("temp_data",comparison_summary_fname)
        az = azure_client.AzureDataLake()
        compare_summary_df = pd.DataFrame()

        if (az.exists(az_path)):
            az.download(az_path, local_tmp_path)
            compare_summary_df = pd.read_parquet(local_tmp_path)
       
        return compare_summary_df

    def upload_comparison_summary(self, comparison_summary_fname:str, comparison_summary_df:pd.DataFrame):
        az = azure_client.AzureDataLake()
        local_tmp_path = os.path.join("temp_data",comparison_summary_fname)
        comparison_summary_df.to_parquet(local_tmp_path)
        az_path = f"data_lake/bronze/validator/summary_frame/{comparison_summary_fname}"
        az.upload_safe(local_tmp_path, az_path)

    def upload_validations(self):
        summary_files = duckdb.sql("SELECT * FROM glob('validations/*/*/diff_summary_*.csv');").to_df()

        if (len(summary_files)>0):
            # get all local results into a file
            local_summary_results_df = duckdb.sql("SELECT * FROM read_csv_auto('validations/*/*/diff_summary_*.csv');").to_df() 
            
            if (len(local_summary_results_df)>0):
                az_path = f"data_lake/bronze/validator/summary/{datetime.datetime.now().strftime('%Y%m%d')}_diff_summary.csv"
                
                az = azure_client.AzureDataLake()
                tmp_local_summary_path = os.path.join("temp_data",os.path.basename(az_path))
                local_summary_path = os.path.join("validations",os.path.basename(az_path))
                
                # combine with daily file up in azure if exists.. otherwise create and upload one
                if (az.exists(az_path)):
                    az.download(az_path, tmp_local_summary_path)
                    duckdb.sql(f"""SELECT * 
                                    FROM read_csv_auto('{ tmp_local_summary_path }')
                                    UNION 
                                    SELECT *
                                    FROM local_summary_results_df
                    """).to_csv(local_summary_path, header=True)
                else:
                    duckdb.sql(f"""SELECT *
                                    FROM local_summary_results_df
                    """).to_csv(local_summary_path, header=True)

                az.upload_safe(local_summary_path, az_path)
                os.remove(local_summary_path)

        #summary_compare_files = duckdb.sql("SELECT * FROM glob('temp/smc_*_*.parquet');").to_df()
        #if (len(summary_compare_files)>0):   
        #    for idx, row in summary_compare_files.iterrows(): 
        #        self.upload_comparison_summary(pd.read_parquet(os.path.join("temp",row["File"])))

        detail_files = duckdb.sql("SELECT * FROM glob('validations/*/*/diff_detail_*.csv');").to_df()

        if (len(detail_files)>0):   
            # do same for detail... 
            tf_local_detail_result_df = duckdb.sql("SELECT * FROM read_csv_auto('validations/*/*/diff_detail_*.csv', filename=True);").to_df()

            az_path = f"data_lake/bronze/validator/detail/{datetime.datetime.now().strftime('%Y%m%d')}_diff_detail.csv"

            # combine with daily file up in azure if exists.. otherwise create and upload one
            if len(tf_local_detail_result_df)>0:
                az = azure_client.AzureDataLake()
                tmp_local_detail_path = os.path.join("temp_data",os.path.basename(az_path))
                local_detail_path = os.path.join("validations",os.path.basename(az_path))
                
                if (az.exists(az_path)):
                    az.download(az_path, tmp_local_detail_path)
                    duckdb.sql(f"""SELECT * 
                                        FROM read_csv_auto('{ tmp_local_detail_path }')
                                        UNION 
                                        SELECT *
                                        FROM tf_local_detail_result_df
                        """).to_csv(local_detail_path, header=True)
                else:
                    duckdb.sql(f"""SELECT *
                                        FROM tf_local_detail_result_df
                        """).to_csv(local_detail_path, header=True)

                az.upload_safe(local_detail_path, az_path)
                os.remove(local_detail_path)
        
        if len(summary_files)>0:
            # dispose any connections made
            duckdb.close()
            self.clean_validations()


    def clean_validations(self, days:int=0):
        threshold = time.time() - days*86400
        staging_dirs = ["validations/","temp_data"]
        for sd in staging_dirs:
            for root, dirs, files in os.walk(sd):
                for f in files:
                    creation_time = os.stat(os.path.join(root, f)).st_ctime

                    if creation_time<threshold or days==0:
                        os.unlink(os.path.join(root, f))
                for d in dirs:
                    mod_time = os.stat(os.path.join(root, d)).st_mtime

                    if mod_time<threshold or days==0:
                        shutil.rmtree(os.path.join(root, d))