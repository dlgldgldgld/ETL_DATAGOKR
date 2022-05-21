from dags import APT_Trade_KOR
from datetime import datetime

context = {
    'execute_date' : datetime(2017, 5, 31, 12, 5, 10),
    'servicekey' : 'EvZj5v2s4TOzNd2Bez7DuOkPc9LRqhB11HV79D6PU9Uh9TmLzoH6LPb7p6FWv77VDKP5IqkAm/dG9GaAeVkesw==',
    'sido' : '부산광역시'
}

APT_Trade_KOR.__main__(**context)