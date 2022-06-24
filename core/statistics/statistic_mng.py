from core.statistics.base_summary import base_summary, summary_exception
import sqlite3

class statistic_mng():
    def __init__(self, db_path:str):
        self.db_path = db_path
        self.conn = None
        self.task_que : list[base_summary] = []
        self.__connectdb()
        pass

    def __connectdb(self):
        try :
            self.conn = sqlite3.connect(self.db_path)
        except Exception as e :
            raise e
        return
        
    def exec(self):
        for task in self.task_que :
            if not task.execute() :
                print(f'{task.__name__} check please.')
                raise RuntimeError()
        