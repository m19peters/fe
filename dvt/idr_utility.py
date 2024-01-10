import sys
import xmltodict
import json
import duckdb

file_loc =  "C:\\Users\\60099\\Downloads\\MEMSQL_DATA_LAKE_FROM_ORACLE_SAPEAST.xml"

with open(file_loc, "r") as f:
    xml = f.read()

    python_dict=xmltodict.parse(xml)
    js=json.dumps(python_dict)

with open("temp_data\idr.json", "w") as outfile:
    parsed = json.loads(js)
    json.dump(js, outfile, indent=4)

duckdb.sql("""INSTALL 'json'; LOAD 'json';""")

duckdb.sql("select * from read_json('temp_data\\idr.json')")



