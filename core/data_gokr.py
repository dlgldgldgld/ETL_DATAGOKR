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
        try :
            response = requests.get(url, params=params)
        except requests.exceptions as e:
            raise e

        content = xmltodict.parse(response.content)
        content = json.dumps(content)
        content = json.loads(content)
        if "body" not in content["response"].keys():
            raise "Check api_token or Http-request url"

        return content["response"]["body"]
    
def _getIndvdHousingPriceAttr(
    servicekey : str, pnu : str, stdrYear : str, 
    format : str, numOfRows : str, pageNo : str 
    ) :
    url = varient.HOUSING_PRICEATTR_URL
    params ={'serviceKey' : servicekey, 'pnu' : pnu, 'stdrYear' : stdrYear, 'format' : format, 'numOfRows' : numOfRows, 'pageNo' : pageNo }
    try :
        response = requests.get(url, params=params)
    except requests.exceptions as e:
        raise e

    if format == 'json':
        content = json.loads(response.content)
    elif format == 'xml':
        content = xmltodict.parse(response.content)
        content = json.dumps(content)
        content = json.loads(content)
        content['response']['field'] = content['response'].pop('fields')['field']

    if "field" not in content["response"].keys():
            raise "Check api_token or Http-request url"
    return content

def _ValidationAptTradeDev( content : list ) :
    rows = []
    if isinstance(content, dict):
        content = [content]

    for row in content :
        new_row = dict()
        for col in varient.APT_TRADE_DEV:
            try:
                val = row.setdefault(col, None)
            except AttributeError:
                print(content)
            
            new_row[col] = val
        rows.append(new_row)
    
    return rows


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

    
    @classmethod
    def getRTMSDataSvcAptTradeDev(
        cls, servicekey : str, lawd : str, 
        deal_ymd : str
        ) :
        """????????? ???????????? - ????????? ??????

        Args:
            servicekey (str): data gokr service key
            lawd (str): ????????? ?????? 5??????
            deal_ymd (str): YYYYMM

        Returns:
            _type_: https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15057511??? response element (start at 6?????? "????????????" column) list

        Example:
            ::

              lawd_list = get_lawd.getlawdlist('???????????????') 
              rows = [] 
              for lawd in lawd_list: 
                rows.extend(DataGoKR.getRTMSDataSvcAptTradeDev(servicekey=context['servicekey'], lawd=lawd, deal_ymd='202204')) 
        """
        res = []
        t_cnt = _getRTMSDataSvcAptTradeDev(servicekey, lawd, deal_ymd, str(1), str(1))["totalCount"]
        iter_cnt = ceil(int(t_cnt) / varient.REQUEST_RECORD )
        for pageNo in range(1, iter_cnt + 1):
            content = _getRTMSDataSvcAptTradeDev(servicekey, lawd, deal_ymd, str(varient.REQUEST_RECORD), str(pageNo))['items']['item']
            content = _ValidationAptTradeDev(content)
            res.extend(content)
        return res

    

