# from airflow import DAG
# from airflow.models import variable
# from airflow.operators.python import PythonOperator

from core.data_gokr import DataGoKR
from thirdparty.lawd_cd import get_lawd

from datetime import datetime, timedelta

import requests
import logging

def __main__(**context):
    stdrYear   = str(context['execute_date'].year)
    servicekey = context['servicekey']
    lawd_list  = get_lawd.getlawdlist(context['sido'])

    for lawd, sido, sigun in lawd_list:
        res = DataGoKR.getIndvdHousingPriceAttr(
                servicekey=servicekey, 
                pnu=lawd, stdrYear=stdrYear, format='json')
        print(sido, sigun, str(len(res)))
    
    
# with DAG( 
#     dag_id = 'APT_Trade_KOR', 
#     default_args={
#         'email' : ['shin12272014@gmail.com'],
#         'email_on_failure' : True,
#         'email_on_retry'   : False,
#         'retries' : 1,
#         'retry_delay' : timedelta(minutes=5)
#     },
#     start_date=datetime(2021, 5, 1, 6, 0, 0),
#     schedule_interval='0 6 L * *',
#     catchup = False,
#     tags = ['incremental update', 'life'],
#     doc_md = '''
#     ## APT_Trade_KOR.py
#     - Purpose : 월간 아파트 거래 정보를 추출하는 dag.
#     - input  : DATA.go.kr - 국토교통부_아파트매매 실거래 상세 자료
#     - output : Redshift Table
#     '''
# ) as dag:
#     pass