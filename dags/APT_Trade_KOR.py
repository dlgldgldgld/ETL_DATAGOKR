from pandas import read_csv
import sqlite3

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from core.data_gokr import DataGoKR
from thirdparty.lawd_cd import get_lawd
from datetime import datetime, timedelta

import os
import csv
import logging

def extract(**context):
    stdrYear   = str(context['execution_date'].year) 
    stdrMonth  = str(context['execution_date'].month)
    stdrMonth  = stdrMonth.rjust(2, '0')

    servicekey = context['params']['servicekey']
    input_sido = context['params']['sido']
    csv_temp_path = context['params']['csv_path']
    csv_temp_path = os.path.join(csv_temp_path, stdrYear + stdrMonth + '_' + input_sido + '.csv')
    
    logging.info(stdrYear + stdrMonth)
    logging.info('Extract - getRTMSDataSvcAptTradeDev')
    
    rows = []
    lawd_list  = get_lawd.getlawdlist(input_sido)
    for lawd, sido, sigun in lawd_list:
        row = DataGoKR.getRTMSDataSvcAptTradeDev(
                servicekey=servicekey, 
                lawd=lawd, deal_ymd=stdrYear + stdrMonth)
        rows.extend(row)
        log = f'{stdrYear + stdrMonth}, {sido}, {sigun}, len = {str(len(row))}'
        logging.info(log)
        break

    logging.info(csv_temp_path)
    with open(csv_temp_path, 'w', newline='') as w:
        writer = csv.writer(w, delimiter='\t')
        writer.writerow(rows[0].keys())
        for row in rows:
            writer.writerow(row.values())

    logging.info('temporary csv extract end.')
    return csv_temp_path

def transform_load(**context):
    stdrYear   = str(context['execution_date'].year) 
    stdrMonth  = str(context['execution_date'].month)
    stdrMonth  = stdrMonth.rjust(2, '0')
    path = os.path.join(context['params']['result_path'] , 'trade_info.db')
    # db init
    conn = sqlite3.connect(path)
    
    # transform csv file
    logging.info('transform csv file')
    csv_temp_path = context['ti'].xcom_pull(key='return_value', task_ids='extract')
    pd = read_csv(csv_temp_path, delimiter='\t')
    pd["거래금액"] = pd['거래금액'].transform(lambda x : int(x.replace(',' , '')))
    pd = pd.drop_duplicates()
    
    # load into db
    logging.info('load into db')
    pd.to_sql(stdrYear+stdrMonth + '_TRADE', conn, index=False, if_exists="replace")
    return 

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
                  'csv_path' : Variable.get("datagokr_csv_path"),
                },
        provide_context=True)
        
    t2 = PythonOperator(
        task_id='transform_load',
        python_callable = transform_load,
        params={
                  'result_path' : Variable.get("datagokr_output_path"),
                },
        provide_context=True)
