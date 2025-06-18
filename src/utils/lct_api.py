import requests
import json
import os
import pandas as pd
from datetime import datetime
from datetime import timedelta
import time


def api_log(shipto):
    path = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling\\LB_Forecasting'
    filename = os.path.join(path, 'calling_log.txt')
    with open(filename, "a") as file:
        use_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        home = str(os.path.expanduser("~")).split('\\')
        if len(home) > 2:
            home_name = home[2]
        else:
            home_name = 'unknow person'
        file.write("{} -- {} -- {} -- LCT.\n".format(use_time, home_name, shipto))


def get_last_time(conn, shipto):
    '''获取 上次预测 历史液位的最后一个,以及 作为 API 查询的起始液位'''
    sql = '''select max(ReadingDate) ReadingDate from historyReading WHERE LocNum={};'''.format(
        shipto)
    df_history = pd.read_sql(sql, conn)
    if len(df_history) > 0 and df_history.ReadingDate[0] is not None:
        time_ago = (pd.to_datetime(
            df_history.ReadingDate.values[0]) - timedelta(days=1)).strftime('%Y-%m-%d')
        last_time = pd.to_datetime(df_history.ReadingDate.values[0])
    else:
        time_ago = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
        last_time = pd.to_datetime(time_ago) - timedelta(hours=1)
    endDate = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    return last_time, time_ago, endDate


def uom_transfer(row):
    if row['units'] == 'inH2O':
        return row['value'] * 2.54
    elif row['units'] == 'Inch':
        return row['value'] * 2.54
    elif row['units'] == 'Pct':
        return row['value']
    elif row['units'] == 'cmH2O':
        return row['value']
    elif row['units'] == 'MM':
        return row['value'] / 10


def job(shipto, time_ago, endDate):
    '''function for AWS API'''
    # 1. connection params
    url = "https://api.airproducts.com/iot/readings/v1/readings"
    headers = {'X-APIKEY': '6db46d89-c426-40c1-a79b-3da13da6075c',
               'Authorization': 'Basic dGVsZW1BUEk6WnhjdmJubTE='}
    session = requests.Session()
    cols = ['integration_shipto', 'integration_Active', 'units']
    tags = ','.join(cols)
    # 制作 shipto
    params = {'useCase': 'cniot',
            'startTime': time_ago,
            'endTime': endDate,
            'integration_shipto': str(shipto),
            'tags': tags,
            'fields': 'value, scaledValue'
            }
    resp = session.get(url, headers=headers, params=params)
    if len(resp.text) > 0:
        data = json.loads(resp.text)
        print(shipto)
        if 'series' not in data['results'][0].keys():
            df_data = pd.DataFrame()
        else:
            i = 0
            api_cols = data['results'][0]['series'][i]['columns']
            # 注意： AWS 取到数据后，由于channel 等原因，需要做一些 dataframe 的转换才能真正使用
            df_data = pd.DataFrame(data['results'][0]['series'][i]['values'], columns=api_cols)
            cols = ['time', 'integration_shipto', 'value', 'units']
            # 处理 integration_Active 为 True 的 表明 绑定 lbshell
            df_data = df_data.loc[df_data.integration_Active == 'True', cols].reset_index(drop=True)
    else:
        df_data = pd.DataFrame()
    return df_data



def updateLCT(shipto, conn):
    '''获取 DOL API 的数据,然后更新 historyReading'''
    # 1. 获取最后一次液位时间
    last_time, time_ago, endDate = get_last_time(conn, shipto)
    # 2. DOL API 请求
    df_data = job(shipto, time_ago, endDate)
    # 2023-11-01 新增 data 是 空 的情况处理: 直接退出。
    if len(df_data) == 0:
        # api_log(shipto)
        return
    # 3.请求后的数据处理
    # 3.1 注意需要转化标准时间为中国时间
    df_data.time = pd.to_datetime(df_data.time).dt.tz_localize(None)
    tz = pd.to_timedelta('8 hours')
    df_data.time = df_data.time + tz
    # 3.2 这一步的目的是要把 秒数 设置为 0， 这样 API 的时间 和 ODBC 是 一致的了。
    df_data.time = df_data.time.dt.floor('Min')
    # 3.3 格式等处理一下， 2023-11-03 把 assetShipto 改为 integration_shipto
    # 以 integration_shipto 为主 LocNum
    df_data = df_data.rename(columns={'integration_shipto': 'LocNum', 'time': 'ReadingDate'})
    # 3.4 LocNum，assetShipto，shipto 全部转化为 int， int 是标准格式。
    df_data.LocNum = df_data.LocNum.astype(int)
    # 3.5 单位转化
    df_data['ReadingLevel'] = df_data.apply(uom_transfer, axis=1)
    # 查询 GalsPerInch
    sql = '''select LocNum, GalsPerInch from odbc_master WHERE LocNum={};'''.format(shipto)
    GalsPerInch = pd.read_sql(sql, conn).set_index('LocNum').loc[shipto, 'GalsPerInch']
    df_data['Reading_Gals'] = (df_data.ReadingLevel * GalsPerInch).round(0)
    cols = ['LocNum', 'ReadingDate', 'Reading_Gals']
    df_data = df_data.loc[df_data.ReadingDate >last_time, cols].sort_values('ReadingDate').reset_index(drop=True)
    if len(df_data) == 0:
        print('already latest')
    else:
        print('new: ', df_data.ReadingDate[len(df_data)-1])
    # 4. 对 historyReading 进行更新
    table_name = 'historyReading'
    df_data.to_sql(table_name, con=conn, if_exists='append', index=False)
    # 5. log
    # api_log(shipto)
    # # for i in range(10):
    # #     print(i)
    # #     time.sleep(1)
