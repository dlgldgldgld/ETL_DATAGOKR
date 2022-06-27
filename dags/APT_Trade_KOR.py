from pandas import read_csv
import sqlite3

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.email  import EmailOperator
from core.data_gokr import DataGoKR

from thirdparty.lawd_cd import get_lawd
from datetime import datetime, timedelta, timezone

import os
import csv
import logging

def extract(**context):
    stdrYear   = str(context['execution_date'].year) 
    stdrMonth  = str(context['execution_date'].month)
    stdrMonth  = stdrMonth.rjust(2, '0')

    servicekey = context['params']['servicekey']
    input_sido = context['params']['sido']

    logging.info(stdrYear + stdrMonth)
    logging.info('Extract - getRTMSDataSvcAptTradeDev')

    # result variable is needed for transform_load(**context) function.
    result = [ ]

    for city, is_proc in input_sido.items() :
        if not is_proc :
            continue 

        csv_temp_path = context['params']['csv_path']
        csv_temp_path = os.path.join(csv_temp_path, stdrYear + stdrMonth + '_' + city + '.csv')
        rows = []
        lawd_list  = get_lawd.getlawdlist(city)
        for lawd, sido, sigun in lawd_list:
            row = DataGoKR.getRTMSDataSvcAptTradeDev(
                    servicekey=servicekey, 
                    lawd=lawd, deal_ymd=stdrYear + stdrMonth)
            rows.extend(row)
            log = f'{stdrYear + stdrMonth}, {sido}, {sigun}, len = {str(len(row))}'
            logging.info(log)

        logging.info(csv_temp_path)
        result.append(csv_temp_path)
        with open(csv_temp_path, 'w', newline='') as w:
            writer = csv.writer(w, delimiter='\t')
            writer.writerow(rows[0].keys())
            for row in rows:
                writer.writerow(row.values())
        logging.info('temporary csv extract end.')
    
    return result

def transform_load(**context):
    created_date = context['execution_date'].in_tz('Asia/Seoul').strftime('%Y%m%d')
    path = os.path.join(context['params']['result_path'] , 'trade_info.db')
    # db init
    conn = sqlite3.connect(path)
    
    # record drop
    curr = conn.cursor()
    curr.execute(f'''
        DELETE FROM TRADEINFO WHERE created_date = "{created_date}"
    ''')
    conn.commit()

    # transform csv file
    logging.info('transform csv file')
    temp_paths = context['ti'].xcom_pull(key='return_value', task_ids='extract')
    for csv_temp_path in temp_paths:
        pd = read_csv(csv_temp_path, delimiter='\t')
        pd["거래금액"] = pd['거래금액'].transform(lambda x : int(x.replace(',' , '')))
        pd.insert(0, 'created_date', created_date)
        pd = pd.drop_duplicates()

        # load into db
        logging.info('load into db')
        pd.to_sql('TRADEINFO', conn, index=False, if_exists="append")

    conn.close()
    return path

def make_image(**context):
    from core.statistics.image_summary import image_summary

    sqlite_path = context['ti'].xcom_pull(key='return_value', task_ids='transform_load')

    conn = sqlite3.connect(sqlite_path)

    from dateutil.relativedelta import relativedelta
    execute_date = context['execution_date']
    date_2 = datetime(year=execute_date.year, month=execute_date.month, day=1, hour=6, minute=0,second=0)
    date_1 = date_2 + relativedelta(months=-1)

    temp_path = os.path.join(context['params']['temp_path'], context['ds'])
    try:
        if not os.path.exists(temp_path):
            os.makedirs(temp_path)
    except OSError:
        print('Error Creating direcotry', temp_path)

    img1_path = os.path.join(temp_path, '부동산_거래금액.png')
    img2_path = os.path.join(temp_path, '부동산_거래수량.png')

    img1 = image_summary(conn, "거래금액", date_1, date_2, img1_path)
    img2 = image_summary(conn, "거래수량", date_1, date_2, img2_path)

    img1.execute()
    img2.execute()

    conn.close()
    return [img1_path, img2_path]


with DAG( 
    dag_id = 'APT_Trade_KOR', 
    default_args={
        'email' : ['hsshin.airflow@gmail.com'],
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
                  'sido' : Variable.get(key="datagokr_sido", deserialize_json=True),
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


    execution_date = "{{ds}}"
    img_file_path = Variable.get("datagokr_output_path")

    t3 = PythonOperator(
        task_id='make_image',
        python_callable = make_image,
        params={ 
            'temp_path' : img_file_path,
        },
        provide_context=True)

    def __get_all_filepath(path):
        filepaths = []
        for dirpath, dirname, filelists in os.walk(path):
            for filename in filelists:
                filepaths.append(os.path.join(dirpath, filename))
        return filepaths

    email = EmailOperator(
        task_id='send_email',
        to=['shin12272014@gmail.com'],
        subject='부산광역시, 서울시 부동산 거래 정보_' + execution_date,
        html_content="""
        <h3>부동산 거래 정보 메일 알람</h3>
        본 메일은 매월 말일에 전송되는 메일입니다. <br>
        이미지를 통해 바로 이전달과 이번달 부동산 거래 수량 및 평균 가격을 확인하세요. <br>
        <br>
        세부 거래 내용은 db 파일을 참조하세요.
        <br>
        """,
        files=__get_all_filepath(img_file_path + '/' + execution_date)
    )

    t1 >> t2
    t2 >> t3
    t3 >> email


    