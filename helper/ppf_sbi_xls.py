from os import listdir
from os.path import isfile, join
import xlrd
import os
import csv
from datetime import datetime


class PpfSbiHelper:
    def __init__(self):
        pass

    def get_data_path(self):
        code_path = os.path.dirname(os.path.realpath(__file__))
        db_path = code_path[:code_path.rfind('/')]+'/data/'
        return db_path

    def get_transactions(self, user):
        trans_dir = self.get_data_path() + user + "/ppf/sbi/"
        for f in listdir(trans_dir):
            item = join(trans_dir, f)
            if isfile(item) and item.endswith(".xls"):
                print("working with ", item)
                '''
                # open a workbook
                workbook = xlrd.open_workbook(item)
                # open sheet by index
                worksheet = workbook.sheet_by_index(0)
                # iterate cell value until we find the right data
                i = 0
                found_header = False
                while True:
                    # read cell value
                    if not found_header and worksheet.cell(i, 0).value == 'Txn Date':
                        found_header = True
                    if found_header:
                        val = {"trans_date": worksheet.cell(i, 1).value}
                        val["notes"] = worksheet.cell(i, 2).value
                        val["reference"] = worksheet.cell(i, 3).value
                        print(val)
                        print(worksheet.cell(i, 4).value)
                        print(worksheet.cell(i, 5).value)
                        print(worksheet.cell(i, 6).value)
                        print(worksheet.cell(i, 7).value)
                        # type text NOT NULL,
                        # amount FLOAT NOT NULL,
                        # interest_component BOOLEAN,
                        # yield
                    i += 1
                '''

                read_file = open(item, "r")
                temp_file = item.replace('.xls', '.csv')
                write_file = open(temp_file, "w")
                found_header = False
                acc_num = None
                for line in read_file:
                    # print(line)
                    if not found_header:
                        if line.startswith('Txn Date'):
                            found_header = True
                            write_file.write(line)
                        if line.startswith("Account Number"):
                            acc_num = line[line.find(
                                ":")+1:].strip().replace('_', '')
                            print(acc_num)
                    else:
                        if not line.strip():
                            continue
                        if not "There is no financial transaction" in line and not "This is a computer generated" in line:
                            write_file.write(line)

                read_file.close()
                write_file.close()
                with open(temp_file, mode='r', encoding='utf-8-sig') as csv_file:
                    print("opened file as csv:", temp_file)
                    csv_reader = csv.DictReader(csv_file, dialect="excel-tab")
                    line_count = 0
                    for row in csv_reader:
                        if line_count == 0:
                            print(f'Column names are {", ".join(row)}')
                        else:
                            print(row)
                            val = {"ppf_number": acc_num}
                            for k, v in row.items():
                                # Txn Date, Value Date, Description, Ref No./Cheque No.,         Debit, Credit, Balance
                                if 'Value Date' in k:
                                    val["trans_date"] = datetime.strptime(
                                        v, '%d %b %Y').date().strftime('%m/%d/%Y')
                                elif 'Description' in k:
                                    val["notes"] = v
                                    val["interest_component"] = "interest" in v.lower()
                                elif 'Ref No' in k:
                                    val["reference"] = v
                                elif 'Debit' in k:
                                    if v.strip():
                                        val["type"] = "debit"
                                        val["amount"] = int(
                                            v.strip().replace(',', ''))
                                elif 'Credit' in k:
                                    if v.strip():
                                        val["type"] = "credit"
                                        val["amount"] = float(
                                            v.strip().replace(',', ''))
                            # print(val)
                            yield val
                        line_count += 1


if __name__ == "__main__":
    ppf_sbi_helper = PpfSbiHelper()
    for trans in ppf_sbi_helper.get_transactions("krishna"):
        print(trans)
