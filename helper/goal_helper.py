from sqlite_db import get_db_conn

class GoalHelper:
    def __init__(self):
        pass

    def create_goal_table(self):
        create_goal_table = """CREATE TABLE IF NOT EXISTS GOALS (
                                     id integer  PRIMARY KEY,
                                     name text NOT NULL,
                                     curr_val FLOAT NOT NULL,
                                     time_period integer NOT NULL,
                                     inflation FLOAT NOT NULL,
                                     final_val FLOAT NOT NULL,
                                     user text NOT NULL,
                                     one_time_pay BOOLEAN NOT NULL,
                                     expense_period integer,
                                     post_returns FLOAT
                                 ); """
        conn = get_db_conn()
        conn.create_table(create_goal_table)

    def add_goal(self, name, curr_val, time_period, inflation, user, one_time, expense_period, post_returns):
        self.create_goal_table()
        conn = get_db_conn()
        select_stmnt = '''SELECT max(id) from GOALS'''
        res = conn.get_one(select_stmnt)
        print(res)
        print(type(res))
        if res and res[0] is not None:
            max_id = res[0] + 1
        else:
            max_id = 1
        if one_time:
            final_val = self.one_time_pay_final_val(curr_val, inflation, time_period)
        else:
            final_val = self.recur_revenue_final_val(curr_val, inflation, time_period/12, expense_period/12, post_returns)
        sql_stmnt = '''INSERT INTO GOALS (id, name, curr_val, time_period, inflation, final_val, user, one_time_pay, expense_period, post_returns) VALUES ('''
        sql_stmnt += str(max_id) + ''',"'''+name+'''",''' + str(curr_val) + ''','''
        sql_stmnt += str(time_period) +''',''' + str(inflation) + ''', ROUND(''' + str(final_val) + ''',2),"'''
        sql_stmnt += user + '''", '''
        if one_time:
            sql_stmnt += str(1) + ''',null,null'''
        else:
            sql_stmnt += str(0) + ''',''' + str(expense_period) + ''',''' + str(post_returns)
        sql_stmnt += ''')'''
        print("running ", sql_stmnt)
        conn.insert_data(sql_stmnt)


    def one_time_pay_final_val(self, curr_val, inflation, time_period):
        final_val = curr_val*(pow(1+inflation/100, time_period/12)-1)
        return final_val

    def recur_revenue_final_val(self, curr_val, inflation, accum_period, expense_period, post_returns):
        inflated_exp = curr_val*(pow(1+inflation/100, accum_period))
        if inflation == post_returns:
            post_returns = post_returns - 0.0000001
        real_ret =(((1+post_returns/100)/(1+inflation/100))-1)*100
        corpus = inflated_exp*((1-pow((1+real_ret/100), -1*expense_period))/(real_ret/100))
        print("corpus:",corpus)
        return corpus

if __name__ == '__main__':
    goal_h = GoalHelper()
    goal_h.add_goal("Child education", 600000, 16*12, 7, "krishna", True, None, None)
    goal_h.add_goal("Retirement", 720000, 30*12, 7, "krishna", False, 25*12, 7)
