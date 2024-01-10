import sqlalchemy
from enums import DbType

class Database:
        def __init__(self, cnn_str : str):
                self.engine = sqlalchemy.create_engine(cnn_str)
                self.db_type = DbType.db_ss

