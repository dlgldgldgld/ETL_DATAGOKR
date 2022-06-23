from statistics.base_summary import base_summary
from matplotlib import rc, font_manager
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os

rc('font', family='NanumGothicCoding', size=8)

@ticker.FuncFormatter
def million_formatter(x, pos):
    return "%.2f억" % (x/1E6)

class image_summary(base_summary):
    def __init__( 
        self, conn, col, 
        date1:datetime, date2:datetime, img_path ) :
        
        super().__init__(conn)
        self.img_path = img_path
        self.col = col
        self.date1 = date1.strftime('%Y%m')
        self.date2 = date2.strftime('%Y%m')

        if self.col == "거래금액":
            self.query = f"""
            SELECT a.법정동시군구코드, b.시도명, b.시군구명,
                SUM(CASE WHEN a.created_date like '{self.date1}%' THEN a.{self.col} END) as lastmonth_col,
                SUM(CASE WHEN a.created_date like '{self.date2}%' THEN a.{self.col} END) as thismonth_col
                FROM TRADEINFO AS a, lawd_cd AS b
                WHERE a.법정동시군구코드 = b.법정동코드
                GROUP BY a.법정동시군구코드
            """
        else:
            self.query = f"""
            SELECT a.법정동시군구코드, b.시도명, b.시군구명,
                COUNT(CASE WHEN a.created_date like '{self.date1}%' THEN 1 ELSE NULL END) as lastmonth_col,
                COUNT(CASE WHEN a.created_date like '{self.date2}%' THEN 1 ELSE NULL END) as thismonth_col
                FROM TRADEINFO AS a, lawd_cd AS b
                WHERE a.법정동시군구코드 = b.법정동코드
                GROUP BY a.법정동시군구코드
            """
        
        print(self.query)

    def __get_rows(self):
        res = []
        curr = self.conn.cursor()
        for row in curr.execute(self.query):
            res.append(row)

        res = np.array(res).T
        np.place(res[3], res[3] == None, 0)
        np.place(res[4], res[4] == None, 0)
        return res

    def __save_image(self, data:np.array):
        citys = np.unique(data[1])
        fig, ax = plt.subplots(nrows=len(citys), ncols=1)
        fig.set_size_inches(18.5,10.5)
        width = 0.25

        for idx, city in enumerate(citys):
            mask = (data[1] == city)
            labels = data[2][mask]
            last_stat = data[3].astype(np.int32)[mask]
            curr_stat = data[4].astype(np.int32)[mask]
            
            x = np.arange(len(labels))
            rects1 = ax[idx].bar(x - width/2, last_stat, width, label=self.date1)
            rects2 = ax[idx].bar(x + width/2, curr_stat, width, label=self.date2)
            
            ax[idx].set_ylabel(self.col)
            ax[idx].set_title(city)
            ax[idx].set_xticks(x, labels)
            ax[idx].legend()

            if self.col == "거래금액":
                ax[idx].yaxis.set_major_formatter(million_formatter)
                ax[idx].bar_label(rects1, labels = ['%.2f억' % (rec/1e6) for rec in rects1.datavalues], padding=3)
                ax[idx].bar_label(rects2, labels = ['%.2f억' % (rec/1e6) for rec in rects2.datavalues], padding=3)
            else :
                ax[idx].bar_label(rects1, padding=3)
                ax[idx].bar_label(rects2, padding=3)
        
        fig.tight_layout()
        plt.savefig(self.img_path)

    def execute(self):
        # get_data with query.
        # x_axis : city 
        # y_axis : 이전월 price, 현재월 price
        res = self.__get_rows()
        self.__save_image(res)

if __name__ == "__main__":
    import sqlite3
    conn = sqlite3.connect("test/trade_info.db")
    a = image_summary(conn, '거래금액', datetime(2022,4,1), datetime(2022,5,1), 'test/hello1.png')
    a.execute()
    a = image_summary(conn, '거래횟수', datetime(2022,4,1), datetime(2022,5,1), 'test/hello2.png')
    a.execute()
