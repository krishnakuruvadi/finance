import requests
import json
from bs4 import BeautifulSoup
import datetime
from datetime import date, timedelta
from sqlite_db import get_db_conn

class MfParser:
    def __init__(self):
        self._session = requests.session()
        # URL list
        self._get_quote_url = 'https://www.amfiindia.com/spages/NAVAll.txt'
        self._get_scheme_url = 'https://api.mfapi.in/mf/'

    def get_schemes(self, as_json=False):
        """
        returns a dictionary with key as scheme code and value as scheme name.
        cache handled internally
        :return: dict / json
        """
        scheme_info = {}
        url = self._get_quote_url
        response = self._session.get(url)
        data = response.text.split("\n")
        header = None
        for scheme_data in data:
            if header is None:
                header = scheme_data
            elif ";" in scheme_data:
               scheme = scheme_data.split(";")
               scheme_info[scheme[0]] = {"fund_house":fund_house, "fund_type":fund_type, "fund_name": scheme[3]}
            elif "Mutual Fund" in scheme_data:
                fund_house = scheme_data.rstrip()
            elif scheme_data.strip() != "":
                fund_type = scheme_data.rstrip()
                
        return scheme_info

    def create_table(self):
        create_mf_table = """ CREATE TABLE IF NOT EXISTS MF (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    fund_house text NOT NULL,
                                    type text NOT NULL
                                ); """
        conn = get_db_conn()
        conn.create_table(create_mf_table)

    def populate_mf_table(self):
        self.create_table()
        conn = get_db_conn()
        for k,v in self.get_schemes().items():
            insert_stmnt = '''INSERT INTO MF(id, name, fund_house, type) VALUES ("'''
            insert_stmnt = insert_stmnt + str(k) + '''","''' + v["fund_name"] +'''","''' + v["fund_house"] + '''","''' + v["fund_type"] + '''")'''
            conn.insert_data(insert_stmnt)

if __name__ == '__main__':
    mf_parse = MfParser()
    #print(mf_parse.get_schemes())
    mf_parse.populate_mf_table()
