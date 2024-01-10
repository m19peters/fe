import duckdb
import azure_client
import datetime
import os

summary_files = duckdb.sql("SELECT * FROM glob('validations/*/*/diff_summary_*.csv');").to_df()

for i, row in summary_files.iterrows():
    os.remove(row["file"])

if (len(summary_files)>0):
       
    local_summary_results_df = duckdb.sql("SELECT * FROM read_csv_auto('validations/*/*/diff_summary_*.csv');").to_df() 
            
    if (len(local_summary_results_df)>0):
                az_path = f"data_lake/dstep=1_bronze/dstore=validator/{datetime.datetime.now().strftime('%Y%m%d')}_diff_summary.csv"
                
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

                for i, row in summary_files.iterrows():
                    os.remove(row["file"])


detail_files = duckdb.sql("SELECT * FROM glob('validations/*/*/diff_detail_*.csv');").to_df()

if (len(detail_files)>0):
    local_detail_result_df = duckdb.sql("SELECT * FROM read_csv_auto('validations/*/*/diff_detail_*.csv', filename=True);").to_df()



                # cols = local_detail_result_df.loc[:, ~local_detail_result_df.columns.isin(['name','capture_time', 'source_db', 'target_db', 'column0', 'missing_in', 'filename', 'cnt'])].columns.values.tolist()
                # cols = list(map(lambda x: f"concat('{x}=',{x})", cols))

                # cols_str = ",',',".join(cols)
    tf_local_detail_result_df = duckdb.sql(f"SELECT name, source_db, target_db, missing_in, cast(cnt as bigint) cnt, capture_time, cols FROM local_detail_result_df").to_df()

    az_path = f"data_lake/dstep=1_bronze/dstore=validator/{datetime.datetime.now().strftime('%Y%m%d')}_diff_detail.csv"

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

                    for i, row in detail_files.iterrows():
                        os.remove(row["file"])