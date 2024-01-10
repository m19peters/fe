import sqlalchemy
from enums import DbType

class Database:
        def __init__(self, cnn_str:str):
                self.engine = sqlalchemy.create_engine(cnn_str, max_identifier_length=128)
                self.db_type = DbType.db_oracle