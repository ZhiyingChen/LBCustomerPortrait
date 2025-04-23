import requests
import json
import os
import pandas as pd
from datetime import datetime
from datetime import timedelta
import time


def api_log(shipto):
    '''2024-09-03 新增 log 查询 类型： LCT or DOL;'''
    path = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling\\LB_Forecasting'
    filename = os.path.join(path, 'calling_log.txt')
    with open(filename, "a") as file:
        use_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        home = str(os.path.expanduser("~")).split('\\')
        if len(home) > 2:
            home_name = home[2]
        else:
            home_name = 'unknow person'
        file.write("{} -- {} -- {} -- DOL.\n".format(use_time, home_name, shipto))


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
    return last_time, time_ago


def uom_transfer(row):
    if row['uom'] == 'inH2O':
        return row['readingValue'] * 2.54
    elif row['uom'] == 'Inch':
        return row['readingValue'] * 2.54
    elif row['uom'] == 'Pct':
        return row['readingValue']
    elif row['uom'] == 'cmH2O':
        return row['readingValue']
    elif row['uom'] == 'MM':
        return row['readingValue'] / 10
    elif row['uom'] == 'mmH2O':
        return row['readingValue'] / 10


def job(shipto, time_ago):
    '''function for DOL API'''
    param = {"ShipTo": shipto, 'BeginTime': time_ago}
    session = requests.Session()
    url = "https://dolv3.dataonline.com/ShipTo/Readings"
    username = "ShipToAPI@AirProducts.AS"
    password = "!h4PZ6r4fA"
    resp = session.post(url, auth=(username, password), json=param, timeout=8)
    if len(resp.text) > 0:
        data = json.loads(resp.text)
        print(shipto)
        total_info = []
        for j in range(len(data['aiChannels'])):
            # 为防错,dongliang 2021-12-02 新增
            # 注意 j 是 channel name, 后面也会用到
            f1 = data['aiChannels'][j]['uom'] != 'Volt'
            f2 = data['aiChannels'][j]['uom'] != 'Volts'
            f3 = data['aiChannels'][j]['uom'] != 'volt'
            f4 = data['aiChannels'][j]['uom'] != 'volts'
            f5 = data['aiChannels'][j]['uom'] is not None
            if f1 and f2 and f3 and f4 and f5:
                info = data['aiChannels'][j]['readings']
                for i in info:
                    i['LocNum'] = shipto
                    i['uom'] = data['aiChannels'][j]['uom']
                    i['channel'] = j
                total_info += info
    else:
        total_info = []
    return total_info


def combine_tank(df):
    '''如果是多个储罐,那么需要把储罐的液位相加；
       如果是单个储罐,这个函数也能处理。'''
    # 1. total tanks
    tank_idx = df.channel.unique()
    # 2. decide main tanks and others
    main_idx = df.channel.value_counts().idxmax()
    other_idx = tank_idx[tank_idx != main_idx]
    # 3. generate main tank data
    df_main = df[df.channel == main_idx].reset_index(drop=True)
    new_col = 'readingValue{}'.format(main_idx)
    df_main = df_main.rename(columns={'readingValue': new_col})
    df_main['readingValue'] = df_main[new_col]
    # 4. combine other tanks if exist
    for i in other_idx:
        print(i)
        df_temp = df[df.channel == i].reset_index(drop=True)
        # 需要改名,否则 列名会有问题
        new_col = 'readingValue{}'.format(i)
        df_temp = df_temp.rename(columns={'readingValue': new_col})
        # 合并
        df_main = pd.merge_asof(df_main, df_temp.loc[:, ['readingTimestamp', new_col]],
                                on='readingTimestamp', direction='nearest', tolerance=pd.Timedelta('1h'))
        df_main = df_main[df_main[new_col].notna()]
        df_main.readingValue = df_main.readingValue + df_main[new_col]
    # 5. choose columns
    cols = ['LocNum', 'readingTimestamp', 'uom', 'readingValue']
    df_dol_combine = df_main[cols]
    return df_dol_combine


def updateDOL(shipto, conn):
    '''获取 DOL API 的数据,然后更新 historyReading'''
    # 1. 获取最后一次液位时间
    last_time, time_ago = get_last_time(conn, shipto)
    # 2. DOL API 请求
    data = job(shipto, time_ago)
    # 2023-11-01 新增 data 是 空 的情况处理: 直接退出。
    if len(data) == 0:
        api_log(shipto)
        return
    # 3.请求后的数据处理
    df_dol = pd.DataFrame(data)
    tz = pd.to_timedelta('8 hours')
    df_dol.readingTimestamp = pd.to_datetime(df_dol.readingTimestamp) + tz
    df_dol = df_dol.sort_values(by='readingTimestamp').reset_index(drop=True)
    # 4. combine tanks
    df_dol_combine = combine_tank(df_dol)
    df_dol_combine['readingValue_1'] = df_dol_combine.apply(uom_transfer, axis=1)
    # 查询 GalsPerInch
    sql = '''select LocNum, GalsPerInch from odbc_master WHERE LocNum={};'''.format(shipto)
    GalsPerInch = pd.read_sql(sql, conn).set_index('LocNum').loc[shipto, 'GalsPerInch']
    df_dol_combine['Reading_Gals'] = (df_dol_combine.readingValue_1 * GalsPerInch).round(0)
    df_dol_combine = df_dol_combine.rename(columns={'readingTimestamp': 'ReadingDate'})
    cols = ['LocNum', 'ReadingDate', 'Reading_Gals']
    df_dol_combine = df_dol_combine.loc[df_dol_combine.ReadingDate >
                                        last_time, cols].reset_index(drop=True)
    if len(df_dol_combine) == 0:
        print('already latest')
    else:
        print('new: ', df_dol_combine.ReadingDate[len(df_dol_combine)-1])
    # 4. 对 historyReading 进行更新
    table_name = 'historyReading'
    df_dol_combine.to_sql(table_name, con=conn, if_exists='append', index=False)
    # 5. log
    api_log(shipto)
    # for i in range(10):
    #     print(i)
    #     time.sleep(1)
