from pandas import read_csv
import os
import logging

cwd = os.path.dirname(os.path.realpath(__file__))
path = os.path.join(cwd, 'city_code.csv')
r_path = os.path.join(cwd, 'lawd_cd.csv')

try :
    city_csv = read_csv(path, encoding='cp949')
except UnicodeDecodeError as e:
    logging.error('Decode Error.', e)

city_csv = city_csv.filter(['법정동코드', '시도명', '시군구명'])
city_csv['법정동코드'] = city_csv['법정동코드'].transform(func = lambda x : x // 100)
city_csv = city_csv.drop_duplicates()

city_csv.to_csv(r_path, index=False)
