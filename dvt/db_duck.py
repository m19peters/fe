import duckdb

class db:
    def __init__(self, file: str):
        self.database = duckdb.connect(file)
        self.file = file
