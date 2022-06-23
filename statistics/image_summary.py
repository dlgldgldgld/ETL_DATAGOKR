from base_summary import base_summary
from datetime import datetime

class image_summary(base_summary):
    def __init__(self, conn, query, img_path):
        self.img_path = img_path
        super().__init__(conn, query)

    def __get_rows(self):
        res = []
        curr = self.conn.cursor()
        for row in curr.execute(self.query):
            res.append(row)
        return res

    def __save_image(self, res : list()):
        pass

    def execute(self):
        # get_data with query.
        # x_axis : city 
        # y_axis : 이전월 price, 현재월 price
        res = self.__get_rows()
        self.__save_image(res)
        

if __name__ == "__main__":
    import sqlite3
    conn = sqlite3.connect("test/trade_info.db")
    query = """
       SELECT a.법정동시군구코드, b.시도명, b.시군구명,
          SUM(CASE WHEN a.created_date = strftime('%Y%m%d', date('now', 'start of month', '-1 month', '-1 day')) THEN a.거래금액 END) as 이전달거래액,
          SUM(CASE WHEN a.created_date = strftime('%Y%m%d', date('now', 'start of month', '-1 day')) THEN a.거래금액 END) as 이번달거래액
        FROM TRADEINFO AS a, lawd_cd AS b
        WHERE a.법정동시군구코드 = b.법정동코드
        GROUP BY a.법정동시군구코드
    """
    a = image_summary(conn, query, 'test/hello.png')
    a.execute()
