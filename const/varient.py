
# RECORD COUNT
REQUEST_RECORD = 100

# URLS
APT_TRADE_DEV_URL = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
HOUSING_PRICEATTR_URL = 'http://apis.data.go.kr/1611000/nsdi/IndvdHousingPriceService/attr/getIndvdHousingPriceAttr'

# SCHEMAS
APT_TRADE_DEV = [
    '거래금액', '거래유형', '건축년도', '년',
    '도로명', '도로명건물본번호코드', '도로명건물부번호코드' ,'도로명시군구코드',
    '도로명일련번호코드', '도로명지상지하코드', '도로명코드' , '법정동',
    '법정동본번코드', '법정동부번코드', '법정동시군구코드', '법정동읍면동코드',
    '법정동지번코드', '아파트', '월', '일',
    '일련번호', '전용면적', '중개사소재지', '지번',
    '지역코드', '층', '해제사유발생일', '해제여부'
]