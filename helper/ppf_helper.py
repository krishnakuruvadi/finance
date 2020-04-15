from sqlite_db import get_db_conn
from dateutil.relativedelta import relativedelta
from datetime import datetime
import os
import csv


class PpfHelper:
    def __init__(self):
        pass

    def create_ppf_rates_table(self):
        rate_table = """CREATE TABLE IF NOT EXISTS PPF_RATES (
                                     id integer PRIMARY KEY,
                                     from_date text NOT NULL,
                                     to_date text NOT NULL,
                                     roi FLOAT NOT NULL
                                 ); """
        conn = get_db_conn()
        conn.create_table(rate_table)

    def get_data_path(self):
        code_path = os.path.dirname(os.path.realpath(__file__))
        data_path = code_path[:code_path.rfind('/')]+'/data/ppf_rates.csv'
        return data_path

    def load_ppf_rates(self):
        data_path = self.get_data_path()
        conn = get_db_conn()

        with open(data_path, mode='r', encoding='utf-8-sig') as csv_file:
            print("opened file:", data_path)
            csv_reader = csv.DictReader(csv_file)
            line_count = 1
            for row in csv_reader:
                print(row)
                if row["From"] == "" or row["To"] == "" or row["ROI"] == "":
                    print("Invalid row for ppf rates entry")
                    continue
                insert_stmnt = '''REPLACE INTO PPF_RATES(id, from_date, to_date, roi) VALUES ('''
                insert_stmnt += str(line_count) + ''',"''' + row["From"] + '''","''' + \
                    row["To"] + '''",''' + row["ROI"] + ''')'''
                conn.insert_data(insert_stmnt)
                line_count += 1

    def get_last_five_years_avg_rate(self):
        conn = get_db_conn()
        select_stmnt = '''SELECT from_date, to_date, roi from PPF_RATES'''
        rows = conn.get_data(select_stmnt)
        rate = 0
        total_months = 0
        if rows:
            for row in rows:
                print(row)
                start_date = datetime.strptime(
                    row[0], '%d/%m/%Y').date()
                end_date = datetime.now()
                if relativedelta(end_date,  start_date).years <= 5:

                    months = relativedelta(datetime.strptime(row[1], '%d/%m/%Y').date(
                    ), datetime.strptime(row[0], '%d/%m/%Y').date()).months + 1
                    print("For ", str(months), " months between ",
                          row[0], " and ", row[1], " roi is ", row[2])
                    rate += float(row[2])*months
                    total_months += months
        if rate > 0:
            return rate/total_months
        else:
            return 0

    def create_ppf_entry_table(self):
        entry_table = """CREATE TABLE IF NOT EXISTS PPF_ENTRY (
                                     id integer,
                                     trans_date text NOT NULL,
                                     notes text,
                                     reference text,
                                     type text NOT NULL,
                                     amount FLOAT NOT NULL,
                                     interest_component BOOLEAN,
                                     PRIMARY KEY (id, trans_date, type)
                                 ); """
        conn = get_db_conn()
        conn.create_table(entry_table)

    def create_ppf_table(self):
        ppf_table = """CREATE TABLE IF NOT EXISTS PPF (
                                     id integer PRIMARY KEY,
                                     number text NOT NULL,
                                     start_date text NOT NULL,
                                     user text NOT NULL,
                                     goal integer
                                 ); """
        conn = get_db_conn()
        conn.create_table(ppf_table)

    def create_ppf_summary_table(self):
        summary_table = """CREATE TABLE IF NOT EXISTS PPF_SUMMARY (
                                     id integer  PRIMARY KEY,
                                     number text NOT NULL,
                                     start_date text NOT NULL,
                                     user text NOT NULL,
                                     goal integer,
                                     curr_amount FLOAT NOT NULL,
                                     principal FLOAT NOT NULL,
                                     interest FLOAT NOT NULL,
                                     withdrawal FLOAT NOT NULL,
                                     avg_investment FLOAT NOT NULL,
                                     roi_five FLOAT NOT NULL
                                 ); """
        conn = get_db_conn()
        conn.create_table(summary_table)

    def insert_ppf_trans_entry(self, ppf_number, date, trans_type, amount, notes, reference, interest_component):
        self.create_ppf_entry_table()
        insert_stmnt = ''' REPLACE INTO PPF_ENTRY (id, trans_date, notes, reference, type, amount, interest_component) '''
        insert_stmnt += '''SELECT A.id, "''' + date + \
            '''", "''' + notes + '''", "''' + reference + '''","'''
        insert_stmnt += trans_type + '''",''' + str(amount) + ''','''
        if interest_component:
            insert_stmnt += str(1)
        else:
            insert_stmnt += str(0)
        insert_stmnt += ''' from PPF A where A.number="''' + ppf_number + '''"'''
        conn = get_db_conn()
        conn.insert_data(insert_stmnt)

    def insert_ppf(self, number, start_date, user, goal):
        self.create_ppf_table()
        conn = get_db_conn()
        select_stmnt = '''SELECT IFNULL(MAX(A.id),0)+1, A.number FROM PPF A WHERE A.number= "''' + \
            number + '''"'''
        res = conn.get_one(select_stmnt)
        print(res)
        print(type(res))
        if res and res[1] is None:
            insert_stmnt = ''' INSERT INTO PPF(id, number, start_date, user, goal) VALUES('''
            insert_stmnt += str(res[0]) + ''', "''' + \
                number + '''","''' + start_date + '''","'''
            insert_stmnt += user + '''",''' + str(goal) + ''')'''
            print(insert_stmnt)
            conn.insert_data(insert_stmnt)
        else:
            print("PPF account exists:", number)

    def refresh_summary(self):
        self.create_ppf_summary_table()
        conn = get_db_conn()
        select_stmnt = '''SELECT A.id, sum(A.amount) from PPF_ENTRY A WHERE A.type="credit" AND A.interest_component = 0 GROUP BY A.id'''
        rows = conn.get_data(select_stmnt)
        summary = {}
        if rows:
            for row in rows:
                print(row)
                summary[row[0]] = {"principal": row[1]}

        select_stmnt = '''SELECT A.id, sum(A.amount) from PPF_ENTRY A WHERE A.type="debit" GROUP BY A.id'''
        rows = conn.get_data(select_stmnt)
        if rows:
            for row in rows:
                if row[0] in summary.keys():
                    summary[row[0]]["debit"] = row[1]
                else:
                    summary[row[0]] = {"debit": row[1]}

        select_stmnt = '''SELECT A.id, sum(A.amount) from PPF_ENTRY A WHERE A.type="credit" AND A.interest_component = 1 GROUP BY A.id'''
        rows = conn.get_data(select_stmnt)
        if rows:
            for row in rows:
                if row[0] in summary.keys():
                    summary[row[0]]["interest"] = row[1]
                else:
                    summary[row[0]] = {"interest": row[1]}

        for k, v in summary.items():
            summary[k]["curr_amount"] = v.get(
                "principal", 0) + v.get("interest", 0) - v.get("debit", 0)
        print(summary)
        select_stmnt = '''SELECT A.id, A.number, A.start_date, A.user, A.goal from PPF A'''
        rows = conn.get_data(select_stmnt)
        for row in rows:
            insert_ppf_summary = ''' REPLACE INTO PPF_SUMMARY (id, number, start_date, user, goal, '''
            insert_ppf_summary += '''curr_amount, principal, interest, withdrawal, avg_investment, roi_five) VALUES ('''
            insert_ppf_summary += str(row[0]) + ''',"''' + row[1] + '''","''' + \
                row[2] + '''","''' + row[3] + '''",''' + str(row[4]) + ''','''
            if row[0] in summary.keys():
                temp = summary[row[0]]
                insert_ppf_summary += str(temp.get("curr_amount", 0)) + ''','''
                insert_ppf_summary += str(temp.get("principal", 0)) + ''','''
                insert_ppf_summary += str(temp.get("interest", 0)) + ''','''
                insert_ppf_summary += str(temp.get("debit", 0)) + ''','''
                insert_ppf_summary += str(temp.get("principal", 0) /
                                          self.get_diff_in_years(row[2]))
            else:
                insert_ppf_summary += '''0, 0, 0, 0, 0'''
            insert_ppf_summary += ''', ''' + \
                str(self.get_last_five_years_avg_rate()) + ''')'''
            conn.insert_data(insert_ppf_summary)

    def get_diff_in_years(self, start_date):
        start_date = datetime.strptime('1/1/2015', '%m/%d/%Y').date()
        end_date = datetime.now()
        difference_in_years = relativedelta(end_date, start_date).years
        return difference_in_years + 1


if __name__ == '__main__':
    ppf_h = PpfHelper()
    ppf_h.create_ppf_table()
    ppf_h.create_ppf_entry_table()
    ppf_h.create_ppf_rates_table()
    ppf_h.load_ppf_rates()
    ppf_h.insert_ppf("0000045678765", "12/1/2015", "krishna", 0)
    ppf_h.insert_ppf_trans_entry(
        "0000045678765", "12/1/2015", "credit", 7000, "REF00987", "Credit to account", False)
    ppf_h.insert_ppf_trans_entry(
        "0000045678765", "12/3/2016", "credit", 7000, "", "Interest capitalized", True)
    ppf_h.refresh_summary()
    print(ppf_h.get_last_five_years_avg_rate())
