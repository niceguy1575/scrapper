# Python 샘플 코드 #
import pandas as pd
import numpy as np
import re
import os
import requests
from datetime import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
import json

def getDownload(url, headers, param=None, retries=3):
    resp = None

    try:
        resp = requests.get(url, params=param, headers = headers)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if 500 <= resp.status_code < 600 and retries > 0:
            print('Retries : {0}'.format(retries))
            return getDownload(url, param, retries - 1)
        else:
            print(resp.status_code)
            print(resp.reason)
            print(resp.request.headers)
    return resp

def concat_param(func_url, year, month, key, numOfRows, json):
    param_list = [str(year),
                  str(month),
                  str(key),
                  str(numOfRows),
                  str(json)]
    param_processing = [a_ + '=' + b_ for a_, b_ in zip(params, param_list)]
    param_concat = func_url + '?' + '&'.join(param_processing)
    
    return(param_concat)

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'}
base_url = 'http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService'

func_list = ['getHoliDeInfo', 'getRestDeInfo', 'getAnniversaryInfo']
func_url = [base_url + '/' + fun for fun in func_list]
params = ['solYear', 'solMonth', 'ServiceKey', 'numOfRows', 'type']

key = 'JRCMDHY9VoRXIriENhzZfsXxfAS5IAmtb1lfYH0OSsVzlSs6Iyc%2FM2YOZsIbk4BOJm6zZmQ6%2Bi8n7QsMXlthYA%3D%3D'

# generate date params

base = datetime.today()
numdays = 365 * 20 # 기준일로부터 gap
before_date_list = [base - timedelta(days=x) for x in range(numdays)]
after_date_list = [base + timedelta(days=x) for x in range(numdays)]

date_list = before_date_list.copy()
date_list.extend(after_date_list)

# mk dataframe
year_list = [day.year for day in date_list]
month_list = [str(day.month).zfill(2) for day in date_list]

ym_df = pd.DataFrame({'year': year_list, 'month': month_list})
param_df = ym_df.loc[~ym_df.duplicated()].copy().reset_index()[['year','month']]
param_df['key'] = key
param_df['numOfRows'] = 10
param_df['_type'] = 'json'

holy_url = param_df.apply(lambda row: concat_param(func_url[0], row['year'], row['month'], row['key'], row['numOfRows'], row['_type']) , axis = 1)
rest_url = param_df.apply(lambda row: concat_param(func_url[1], row['year'], row['month'], row['key'], row['numOfRows'], row['_type']) , axis = 1)
anniv_url = param_df.apply(lambda row: concat_param(func_url[2], row['year'], row['month'], row['key'], row['numOfRows'], row['_type']) , axis = 1)

holy_list = []
rest_list = []
anniv_list = []
for idx in range(0, len(holy_url)):
    #print(idx)
    
    hu = holy_url[idx]
    ru = rest_url[idx]
    au = anniv_url[idx]
    
    hr = getDownload(hu, headers = headers)
    rr = getDownload(ru, headers = headers)
    ar = getDownload(au, headers = headers)
    
    holy_list.append(hr)
    rest_list.append(rr)
    anniv_list.append(ar)
	
holy_df = pd.DataFrame({'list': holy_list, 'gubun': '국경일'})
rest_df = pd.DataFrame({'list': rest_list, 'gubun': '공휴일'})
anniv_df = pd.DataFrame({'list': anniv_list, 'gubun': '기념일'})

response_bind = pd.concat([holy_df, rest_df, anniv_df], axis = 0).reset_index()
response_bind = response_bind[['list', 'gubun']]

# dom to dateframe
dom_bind = pd.DataFrame()

for idx in range(0, response_bind.shape[0]):
    response_tmp = response_bind.loc[response_bind.index == idx]
    response_tmp_contents = response_tmp['list'].to_list()[0]
    response_tmp_gubun = response_tmp['gubun'].to_list()[0]

    dom = BeautifulSoup(response_tmp_contents.text, 'lxml')
    items = dom.find_all('item')

    if len(items) > 0:
        for item in items:
            datekind = item.find_all('datekind')[0].text.strip()
            datename = item.find_all('datename')[0].text.strip()
            isholiday = item.find_all('isholiday')[0].text.strip()
            locdate = item.find_all('locdate')[0].text.strip()

            res = pd.DataFrame({'gubun': [response_tmp_gubun],
                                'datekind': [datekind], 'datename': [datename],
                                'isholiday': [isholiday], 'locdate': [locdate]})
            dom_bind = pd.concat([dom_bind, res], axis = 0)
			
# 주말 5, 6
dom_yn_list = dom_bind['locdate'].apply(lambda x: datetime.strptime(x, '%Y%m%d'). weekday()).to_list() 
dom_yn = [dy in [5,6] for dy in dom_yn_list]
dom_bind['isweek'] = np.where(dom_yn, "Y", "N")

holiday_bind = dom_bind.reset_index()[['gubun', 'datekind', 'datename', 'locdate', 'isholiday', 'isweek']]
holiday_bind.columns = ['type', 'dateKind', 'dateName', 'locDate','isHoliday', 'isWeek']

holiday_bind = holiday_bind.sort_values(by = 'locDate').reset_index().drop('index', axis = 1)
holiday_bind.to_csv("../data/holiday_bind.txt", sep = "|", index = False)