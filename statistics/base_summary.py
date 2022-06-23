from abc import ABCMeta, abstractmethod

class base_summary(metaclass=ABCMeta):
    """_summary_
    :param connection : The connecttion of main DB. it must be opened before using summary table.
    :type connection : db_connect
    """
    def __init__(self, connection, query):
        self.conn  = connection
        self.query = query

    @abstractmethod
    def execute(self) -> list():
        pass
    
class summary_exception(Exception):
    def __str__(self):
        return "error elert during execute()."