from pandas import read_csv
import os
import logging

def getlawdlist(sido):
    cwd = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(cwd, 'lawd_cd.csv')

    try :
        city_csv = read_csv(path, encoding='utf-8')
    except UnicodeDecodeError as e:
        logging.error('Decode Error.', e)
    
    return city_csv[city_csv['시도명'] == sido].values.tolist()
    