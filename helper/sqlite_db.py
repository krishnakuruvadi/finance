import sqlite3
import os
from sqlite3 import Error
 
class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

 
class SQLiteDb(metaclass=Singleton):
    _conn = None

    def __init__(self):
        self.create_db(self.get_file_path())

    def get_file_path(self):
        code_path = os.path.dirname(os.path.realpath(__file__))
        db_path = code_path[:code_path.rfind('/')]+'/data/data.db'
        print("code_path:",code_path)
        print("db_path:", db_path)
        return db_path

    def create_db(self, db_file):
        """ create a database connection to a SQLite database """
        self._conn = None
        try:
            self._conn = sqlite3.connect(db_file)
            print(sqlite3.version)
        except Error as e:
            print(e)

    def close(self):
        if self._conn:
            self._conn.close()

    def create_table(self, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = self._conn.cursor()
            c.execute(create_table_sql)
            self._conn.commit()
        except Error as e:
            print(e)

    def insert_data(self, insert_data_sql):
        try:
            c = self._conn.cursor()
            c.execute(insert_data_sql)
            self._conn.commit()
        except Error as e:
            print(e)
 
def get_db_conn():
    return SQLiteDb.__call__()

if __name__ == '__main__':
    conn = get_db_conn()
    create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    begin_date text,
                                    end_date text
                                ); """
    conn.create_table(create_projects_table)
