import email
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from core.data_gokr import DataGoKR
from thirdparty.lawd_cd import get_lawd
from datetime import datetime, timedelta

import logging

def extract(**context):
    stdrYear   = str(context['execution_date'].year) 
    stdrMonth  = str(context['execution_date'].month)
    stdrMonth  = stdrMonth.rjust(2, '0')

    servicekey = context['params']['servicekey']
    input_sido = context['params']['sido']
    lawd_list  = get_lawd.getlawdlist(input_sido)

    logging.info(stdrYear + stdrMonth)
    for lawd, sido, sigun in lawd_list:
        res = DataGoKR.getRTMSDataSvcAptTradeDev(
                servicekey=servicekey, 
                lawd=lawd, deal_ymd=stdrYear + stdrMonth)
        log = f'{stdrYear + stdrMonth}, {sido}, {sigun}, len = {str(len(res))}'
        logging.info(log)
        break

with DAG( 
    dag_id = 'APT_Trade_KOR', 
    default_args={
        'email' : ['shin12272014@gmail.com'],
        'email_on_failure' : True,
        'email_on_retry'   : False,
        'retries' : 1,
        'retry_delay' : timedelta(minutes=5)
    },
    start_date=datetime(2021, 5, 1, 6, 0, 0),
    schedule_interval='0 6 L * *',
    catchup = False,
    tags = ['incremental update', 'life'],
    doc_md = '''
    ## APT_Trade_KOR.py
    - Purpose : 월간 아파트 거래 정보를 추출하는 dag.
    - input  : DATA.go.kr - 국토교통부_아파트매매 실거래 상세 자료
    - output : Redshift Table
    '''
) as dag:
    
    t1 = PythonOperator(
        task_id='extract',
        python_callable = extract,
        params={
                  'servicekey' : Variable.get("datagokr_token"),
                  'sido' : Variable.get("datagokr_sido"),
                },
        provide_context=True)
        
