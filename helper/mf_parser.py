import requests
import os
import csv
import json
from bs4 import BeautifulSoup
import datetime
from datetime import date, timedelta
from sqlite_db import get_db_conn
from os import listdir
from os.path import isfile, join

class MfParser:
    def __init__(self):
        self._session = requests.session()
        # URL list
        self._get_quote_url = 'https://www.amfiindia.com/spages/NAVAll.txt'
        self._get_scheme_url = 'https://api.mfapi.in/mf/'

    def get_data_path(self):
        code_path = os.path.dirname(os.path.realpath(__file__))
        db_path = code_path[:code_path.rfind('/')]+'/data/'
        return db_path

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

    def create_mf_mapping_table(self):
        create_mf_table = """ CREATE TABLE IF NOT EXISTS MF_MAPPING (
                                    id integer PRIMARY KEY,
                                    kuvera text NOT NULL
                                ); """
        conn = get_db_conn()
        conn.create_table(create_mf_table)

    def create_transactions_table(self):
        create_trans_table = """ CREATE TABLE IF NOT EXISTS MF_TRANSACTIONS (
                                    id integer NOT NULL,
                                    trans_date text NOT NULL,
                                    folio text NOT NULL,
                                    type text NOT NULL,
                                    units FLOAT NOT NULL,
                                    nav FLOAT NOT NULL,
                                    amount FLOAT NOT NULL,
                                    user text NOT NULL,
                                    goal text,
                                    PRIMARY KEY (id, trans_date, folio, type)
                                ); """
        conn = get_db_conn()
        conn.create_table(create_trans_table)

    def populate_mf_mapping_table(self):
        self.create_mf_mapping_table()
        conn = get_db_conn()
        mapping_file = self.get_data_path() + "mapping.csv"
        
        with open(mapping_file, mode='r', encoding='utf-8-sig') as csv_file:
            print("opened file:", mapping_file)
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    print(f'Column names are {", ".join(row)}')
                else:
                    print(row)
                    if row["scheme_code"] == "":
                        continue
                    insert_stmnt = '''INSERT INTO MF_MAPPING(id, kuvera) VALUES ("'''
                    insert_stmnt = insert_stmnt + row["scheme_code"] + '''","''' + row["kuvera"] +'''")'''
                    conn.insert_data(insert_stmnt)
                line_count += 1

    def populate_mf_table(self):
        self.create_table()
        conn = get_db_conn()
        for k,v in self.get_schemes().items():
            insert_stmnt = '''INSERT INTO MF(id, name, fund_house, type) VALUES ("'''
            insert_stmnt = insert_stmnt + str(k) + '''","''' + v["fund_name"] +'''","''' + v["fund_house"] + '''","''' + v["fund_type"] + '''")'''
            conn.insert_data(insert_stmnt)

    def populate_transactions_table(self, user, source="kuvera"):
        self.create_transactions_table()
        if source == "kuvera":
            self.populate_trans_kuvera(user)

    def populate_trans_kuvera(self, user):
        conn = get_db_conn()
        trans_dir = self.get_data_path() + user + "/kuvera/"
        for f in listdir(trans_dir):
            item = join(trans_dir, f)
            if isfile(item) and item.endswith(".csv"):
                with open(item, mode='r', encoding="ascii", errors="surrogateescape") as trans_file:
                    print("opened file:", item)
                    #csv_reader = csv.DictReader(trans_file)
                    csv_reader = csv.DictReader((line.replace('\0','') for line in trans_file))
                    line_count = 0
                    name_id_map = dict()
                    for row in csv_reader:
                        if line_count == 0:
                            print(f'Column names are {", ".join(row)}')
                        else:
                            print(row)
                            fund_name = row[" Name of the Fund"]
                            if fund_name in name_id_map:
                                id = name_id_map[fund_name]
                            else:
                                select_stmnt = '''SELECT id from MF_MAPPING WHERE kuvera="'''+fund_name + '''"'''
                                res = conn.get_one(select_stmnt)
                                print(res)
                                print(type(res))
                                if res:
                                    name_id_map[fund_name] = res[0]
                                else:
                                    name_id_map[fund_name] = None
                                id = name_id_map[fund_name]
                            if id is not None:
                                insert_stmnt = '''INSERT INTO MF_TRANSACTIONS(id, trans_date, folio, type, units, nav, amount, user, goal) '''
                                insert_stmnt += '''VALUES ("'''+str(id) + '''","''' + row["Date"].strip()
                                insert_stmnt += '''","'''+row[" Folio Number"] + '''","'''+row[" Order"]
                                insert_stmnt += '''","'''+row[" Units"]+'''","'''+row[" NAV"]
                                insert_stmnt += '''","'''+row[" Amount (INR)"]+'''","'''+user
                                insert_stmnt += '''",Null)'''
                                print("Executing:", insert_stmnt)
                                conn.insert_data(insert_stmnt)
                                
                        line_count += 1
                    

if __name__ == '__main__':
    mf_parse = MfParser()
    #print(mf_parse.get_schemes())
    mf_parse.populate_mf_table()
    mf_parse.populate_mf_mapping_table()
    mf_parse.populate_transactions_table(user='krishna')
