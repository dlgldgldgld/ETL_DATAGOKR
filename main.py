from core.data_gokr import DataGoKR
from thirdparty.lawd_cd import get_lawd
from datetime import datetime
import csv

context = {
    'execute_date' : datetime(2017, 5, 31, 12, 5, 10),
    'servicekey' : 'GtpsY2G4SR9diD2DApxAvraB+a/2eFWmFm+2pp89GtUYxOHaMJ/wt5f6Z2qNmuFv0mlDOwpneBobkVkH0C4lKQ==',
    'sido' : '부산광역시'
}

lawd_list = get_lawd.getlawdlist('부산광역시')
rows = []
for lawd in lawd_list:
    rows.extend(DataGoKR.getRTMSDataSvcAptTradeDev(servicekey=context['servicekey'], lawd=lawd, deal_ymd='202204'))
    break

with open('output.csv','w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(rows[0].keys())
    for row in rows:
        writer.writerow(row.values())