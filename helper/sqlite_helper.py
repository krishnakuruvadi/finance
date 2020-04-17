class SQLiteSelectHelper:
    def __init__(self, table_name):
        self.table_name = table_name
        self.fields = list()
        self.clauses = list()

    def add_get_field(self, *argv):
        for arg in argv:
            self.fields.append(arg)

    def add_where_clause(self, clause):
        self.clauses.append(clause)

    def get_statement(self):
        sel_stmnt = '''SELECT '''
        if self.fields is None or len(self.fields) == 0:
            sel_stmnt += '''*'''
        else:
            i = 1
            for field in self.fields:
                sel_stmnt += field
                if i < len(self.fields):
                    sel_stmnt += ''', '''
                i += 1
        sel_stmnt += ''' FROM ''' + self.table_name
        if self.clauses:
            sel_stmnt += ''' WHERE '''
            j = len(self.clauses)
            k = 1
            for clause in self.clauses:
                sel_stmnt += clause
                if k < j:
                    sel_stmnt += ''' AND '''
                k += 1
        return sel_stmnt


class SQLiteReplaceHelper:
    def __init__(self, table_name, is_insert=False):
        self.table_name = table_name
        self.fields = dict()
        self.is_insert = is_insert

    def add_field(self, key, val):
        self.fields[key] = val

    def get_statement(self):
        repl_stmnt = '''INSERT''' if self.is_insert else '''REPLACE'''
        repl_stmnt += ''' INTO ''' + self.table_name
        if not self.fields or len(self.fields) == 0:
            return ""
        field_stmnt = '''('''
        val_stmnt = ''' VALUES ('''
        i = 1
        for k, v in self.fields.items():
            field_stmnt += k
            print(v, "is of type ", type(v))
            if type(v) == str:
                val_stmnt += '''"''' + v + '''"'''
            elif type(v) == bool:
                val_stmnt += str(1) if v else str(0)
            else:
                val_stmnt += str(v)
            if i < len(self.fields):
                field_stmnt += ''', '''
                val_stmnt += ''', '''
            i += 1
        field_stmnt += ''')'''
        val_stmnt += ''')'''
        repl_stmnt += field_stmnt + val_stmnt
        return repl_stmnt


if __name__ == "__main__":
    sel_helper = SQLiteSelectHelper('PPF')
    # sel_helper.add_get_field('id', 'number', 'start_date', 'user', 'goal')
    sel_helper.add_where_clause('id = 2')
    sel_helper.add_where_clause('''user = "krishna"''')
    print(sel_helper.get_statement())

    repl_helper = SQLiteReplaceHelper('PPF')
    repl_helper.add_field("id", 2)
    repl_helper.add_field("number", "000000004567")
    repl_helper.add_field("start_date", "1/1/2000")
    repl_helper.add_field("user", "krishna")
    repl_helper.add_field("goal", 0)
    print(repl_helper.get_statement())
