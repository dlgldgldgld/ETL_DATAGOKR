from math import ceil
import requests
import os
import json
from const import varient
import xmltodict

def _getRTMSDataSvcAptTradeDev(
        servicekey : str, lawd : str, deal_ymd : str, 
        numOfRows : str, pageNo : str 
        ) :
        url = varient.APT_TRADE_DEV_URL
        params ={'serviceKey' : servicekey, 'pageNo' : pageNo, 'numOfRows' : numOfRows, 'LAWD_CD' : lawd, 'DEAL_YMD' : deal_ymd }
        response = requests.get(url, params=params)
        content = xmltodict.parse(response.content)
        content = json.dumps(content)
        content = json.loads(content)
        return content
    
def _getIndvdHousingPriceAttr(
    servicekey : str, pnu : str, stdrYear : str, 
    format : str, numOfRows : str, pageNo : str 
    ) :
    url = varient.HOUSING_PRICEATTR_URL
    params ={'serviceKey' : servicekey, 'pnu' : pnu, 'stdrYear' : stdrYear, 'format' : format, 'numOfRows' : numOfRows, 'pageNo' : pageNo }
    response = requests.get(url, params=params)
    if format == 'json':
        content = json.loads(response.content)
    elif format == 'xml':
        content = xmltodict.parse(response.content)
        content = json.dumps(content)
        content = json.loads(content)
        content['response']['field'] = content['response'].pop('fields')['field']
    return content

class DataGoKR :
    def __init__(self):
        self.lawd_cd = os.path.join(os.getcwd(), 'thirdparty', 'lawd_cd', 'lawd_cd.csv')
        self.lawd_cd = open(self.lawd_cd, 'r')

    @classmethod
    def getIndvdHousingPriceAttr(
        cls, servicekey : str, pnu : str, 
        stdrYear : str, format : str
        ) -> list :
        res = []
        main_key = "indvdHousingPrices"
        if format == 'xml':
            main_key = "response"

        t_cnt = _getIndvdHousingPriceAttr(servicekey, pnu, stdrYear, format, str(1), str(1))[main_key]['totalCount']
        iter_cnt = ceil(int(t_cnt) / varient.REQUEST_RECORD )
        for pageNo in range(1, iter_cnt + 1):
            content = _getIndvdHousingPriceAttr(servicekey, pnu, stdrYear, format, str(varient.REQUEST_RECORD), str(pageNo))[main_key]["field"]
            res.extend(content)
        return res

    

