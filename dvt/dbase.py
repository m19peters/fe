from enums import DbType
import pandas as pd
import json

# import profile_test

class db:
    def __init__(self, db_config_name: str, query: str, name: str):
        cnn_str: str = ""
        typ :DbType = None

        with open("db.json","r") as f:
            js = json.load(f)

            for i in js["config"]:
                if db_config_name.upper() == str(i["name"]).upper():
                    cnn_str = str(i["cnnstr"])
                    typ = DbType[str(i["dbtype"]).lower()]

        dynamo = __import__(typ.name)
        self.database = dynamo.Database(cnn_str)
        self.query = query
        self.name = name
        self.type = typ
        self.db_config_name = db_config_name

    def load(self, chunksize: int=1000000):    
        #with profile_test.profiled():   
        with self.database.engine.connect().execution_options(stream_results=True, max_row_buffer=chunksize) as conn:
            for chunk in pd.read_sql(self.query, conn, chunksize=chunksize):
                yield chunk
        
  
    def read_sql(self, query: str):
        with self.database.engine.connect() as conn:
            print(f"Running {query}...")
            df = pd.read_sql(query, conn)

        return df
    