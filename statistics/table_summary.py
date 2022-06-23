from base_summary import base_summary

class table_summary(base_summary):
    def __init__(self, conn, query, table_name):
        self.table_name = table_name
        self.query = query
        super().__init__(conn)

    def execute(self):
        curr = self.conn.cursor()

        drop_query = f"DROP TABLE IF EXISTS {self.table_name};"
        ctas_query = f'''
            CREATE TABLE {self.table_name} AS 
            {query}
            ;
        '''
        try :
            curr.execute(drop_query)
            curr.execute(ctas_query)
        except Exception as e:
            print(e)
            return False 

        return True

if __name__ == "__main__":
    import sqlite3
    conn = sqlite3.connect("test/trade_info.db")
    query = '''
                SELECT distinct created_date FROM TRADEINFO
            '''
    a = table_summary(conn, query, 'hello')
    print(a.execute())