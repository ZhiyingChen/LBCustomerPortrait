from .Email_forecast import send_email
from matplotlib.lines import Line2D
from .odbc_master import check_refresh_deliveryWindow
from ..utils import decorator
from datetime import datetime
from datetime import timedelta
import matplotlib.pylab as pylab
import tkinter as tk
from tkinter import ttk
import pandas as pd
import numpy as np
import sqlite3
import os
from matplotlib.backends.backend_tkagg import (
    FigureCanvasTkAgg, NavigationToolbar2Tk)
# Implement the default Matplotlib key bindings.
from matplotlib.backend_bases import key_press_handler
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import WeekdayLocator, YearLocator, DayLocator
from tkinter import messagebox
# from dateEntry import DateEntry
import matplotlib
import time
import threading
from tkinter import scrolledtext
from .dol_api import updateDOL
from .lct_api import updateLCT

# 设置使用的字体（需要显示中文的时候使用）
font = {'family': 'SimHei'}
# 设置显示中文,与字体配合使用
matplotlib.rc('font', **font)
matplotlib.rcParams['axes.unicode_minus'] = False
params = {'legend.fontsize': 'x-large',
          'axes.labelsize': 'x-large',
          'axes.titlesize': 'x-large',
          'xtick.labelsize': 'x-large',
          'ytick.labelsize': 'x-large'}
pylab.rcParams.update(params)


# annot = None
# canvas = None
def logConnection(filename, action):
    f = open(filename, "a")
    use_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    home = str(os.path.expanduser("~")).split('\\')
    if len(home) > 2:
        home_name = home[2]
    else:
        home_name = 'unknow person'
    f.write("{} -- {} -- {}.\n".format(use_time, home_name, action))
    f.close()


def connect_sqlite(db_name):
    '''连接 SQLITE'''
    conn = sqlite3.connect(db_name, check_same_thread=False)
    print('sqlite connected')
    return conn


def get_filename(path1, purpose='autoScheduling'):
    '''get filename and backup filename, modifid at 20220520'''
    file_dict = {}
    if purpose == 'autoScheduling':
        # 仅供 成都 Terminal
        name = 'chengdu'
        forecast_file = os.path.join(path1, 'Sample forecasted reading.csv')
        history_file = os.path.join(path1, 'Sample history reading.csv')
        drop_file = os.path.join(path1, 'Sample forecasted reading_drop.csv')
        file_dict[name] = [forecast_file, history_file, drop_file]
    else:
        # For Forecast Project
        regions = ['LB_LCT', 'CNS', 'CNCE', 'CNNW' ]
        path2 = os.path.join(path1, 'ForecastingInputOutput')
        for region in regions:
            file_dict[region] = []
            # 主文件名夹
            path3 = os.path.join(path2, region)
            # 备份文件名夹
            path_backup = os.path.join(path3, 'Backup')
            # 三个只要文件名：预测,历史,drop信息
            # 2024-08-30 新增
            if region == 'LB_LCT':
                files = ['Sample_forecasted_reading.csv',
                     'Sample_history_reading.csv', 'Sample_forecasted_reading_drop.csv']
            else:
                files = ['Sample forecasted reading.csv',
                     'Sample history reading.csv', 'Sample forecasted reading_drop.csv']
            for file in files:
                if os.path.exists(os.path.join(path3, file)):
                    filename = os.path.join(path3, file)
                else:
                    filename = None
                # 主文件名加入列表
                file_dict[region].append(filename)
                if os.path.exists(os.path.join(path_backup, file)):
                    filename_back = os.path.join(path_backup, file)
                else:
                    filename_back = None
                # 备份文件名加入列表
                file_dict[region].append(filename_back)
    return file_dict


def get_historyReading(shipto, fromTime, toTime, conn):
    '''获取历史液位数据'''
    sql = '''select LocNum, ReadingDate, Reading_Gals
             FROM historyReading
             where ReadingDate >= '{}'
             AND ReadingDate <= '{}'
             AND LocNum = {};'''.format(fromTime, toTime, shipto)
    df_history = pd.read_sql(sql, conn)
    df_history.ReadingDate = pd.to_datetime(df_history.ReadingDate)
    return df_history


def get_forecastReading(shipto, fromTime, toTime, conn):
    '''获取预测液位数据, 注意 返回的 df_forecast 长度始终大于 0；'''
    # 第一步 首先判断 该 shipto 是不是一个异常 shipto
    sql = '''select * FROM forecastReading
         where (Forecasted_Reading=999999
                OR Forecasted_Reading=888888
                OR Forecasted_Reading=777777)
                AND LocNum = {};'''.format(shipto)
    df_forecast = pd.read_sql(sql, conn)
    # 第二步 获取正常取值范围
    if len(df_forecast) > 0:
        return df_forecast
    sql = '''select * FROM forecastReading
         where Next_hr >= '{}'
         AND Next_hr <= '{}'
         AND LocNum = {};'''.format(fromTime, toTime, shipto)
    df_forecast = pd.read_sql(sql, conn)
    # 第三步 如果 df_forecast 是空集,需要一些必要元素的补充。
    if len(df_forecast) == 0:
        sql = '''select DISTINCT TargetRefillDate, TargetRiskDate,
                                 TargetRunoutDate, RiskGals
                 FROM forecastReading
                 where LocNum = {};'''.format(shipto)
        df_forecast = pd.read_sql(sql, conn)
        df_forecast['Next_hr'] = None
        df_forecast['Forecasted_Reading'] = None
        df_forecast['Hourly_Usage_Rate'] = None
    # 对上述两种情况一起处理的部分
    df_forecast.Next_hr = pd.to_datetime(df_forecast.Next_hr)
    df_forecast.TargetRefillDate = pd.to_datetime(df_forecast.TargetRefillDate)
    df_forecast.TargetRiskDate = pd.to_datetime(df_forecast.TargetRiskDate)
    df_forecast.TargetRunoutDate = pd.to_datetime(df_forecast.TargetRunoutDate)
    return df_forecast


def get_forecastBeforeTrip(shipto, fromTime, toTime, conn):
    '''获取当前到送货前预测液位数据'''
    sql = '''select LocNum, Next_hr, Forecasted_Reading
         FROM forecastBeforeTrip
         where Next_hr >= '{}'
         AND Next_hr <= '{}'
         AND LocNum = {};'''.format(fromTime, toTime, shipto)
    df_forecast = pd.read_sql(sql, conn)
    df_forecast.Next_hr = pd.to_datetime(df_forecast.Next_hr)
    return df_forecast


def get_beforeReading(conn, shipto):
    '''获取司机录入液位'''
    sql = '''select ReadingDate, beforeKG from beforeReading
             WHERE LocNum={};'''.format(shipto)
    df = pd.read_sql(sql, conn)
    df = df.sort_values('ReadingDate')
    return df.beforeKG.values

def get_max_payload_by_ship2(
        conn,
        ship2: str,
):

    sql_statement = \
        ("SELECT CorporateIdn, LicenseFill "
         "FROM odbc_MaxPayloadByShip2 "
         "WHERE ToLocNum = '{}' ").format(ship2)
    result_df = pd.read_sql(sql_statement, conn)
    return result_df

def get_manualForecast(shipto, fromTime, toTime, conn):
    '''get manually calculated data'''
    sql = '''select *
             FROM manual_forecast
             where Next_hr >= '{}'
             AND Next_hr <= '{}'
             AND LocNum = {};'''.format(fromTime, toTime, shipto)
    df_manual = pd.read_sql(sql, conn)
    df_manual.Next_hr = pd.to_datetime(df_manual.Next_hr)
    return df_manual


def get_customerInfo(shipto, conn):
    '''获取customer数据'''
    sql = '''select *
         FROM odbc_master
         where LocNum = {};'''.format(shipto)
    df_info = pd.read_sql(sql, conn)
    return df_info


def get_recent_reading(shipto, conn):
    '''从 historyReading 里获取最近液位读数'''
    galsPerInch = df_info.GalsPerInch.values[0]
    sql = '''select ReadingDate, Reading_Gals
                 FROM historyReading
                 where LocNum = {};'''.format(shipto)
    df1 = pd.read_sql(sql, conn).tail(24)
    df1.ReadingDate = pd.to_datetime(df1.ReadingDate)
    df1['Reading_CM'] = (df1.Reading_Gals/galsPerInch).round().astype(int)
    df1.Reading_Gals = df1.Reading_Gals.astype(int)
    df1 = df1.sort_values('ReadingDate', ascending=False).reset_index(drop=True)
    df1['cm_diff'] = df1.Reading_CM.diff(-1)
    df1['time_diff'] = df1.ReadingDate.diff(-1)/pd.Timedelta('1 hour')
    df1['Hour_CM'] = (df1.cm_diff/df1.time_diff).round(1)

    def clean_use(x):
        # 对小时用量进行清理
        if pd.isnull(x):
            return x
        if x <= 0:
            return -int(x)
        else:
            return None
    df1.Hour_CM = df1.Hour_CM.apply(clean_use)
    df1['No'] = range(1, len(df1)+1)
    cols = df1.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    # 去掉两个过度列
    cols.remove('cm_diff')
    cols.remove('time_diff')
    df1 = df1[cols]
    df1 = df1.rename(columns={'Reading_Gals': 'Read_KG', 'Reading_CM': 'Read_CM'})
    return df1


def get_deliveryWindow(shipto, conn):
    '''从 odbc_DeliveryWindow 里获取 送货窗口数据'''
    sql = '''select * from odbc_DeliveryWindow where LocNum={}'''.format(shipto)
    df = pd.read_sql(sql, conn)
    # df1 = df.loc[:, df.columns[1:]].applymap(lambda x: pd.to_datetime(x).strftime('%H:%M'))

    try:
        # 新版本
        df1 = df.loc[:, df.columns[1:]].map(lambda x: pd.to_datetime(x).strftime('%H:%M'))
    except Exception:
        # 老版本
        df1 = df.loc[:, df.columns[1:]].applymap(lambda x: pd.to_datetime(x).strftime('%H:%M'))
    data = {}
    weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    for i in weekdays:
        primary_start = 'Dlvry' + i + 'From'
        primary_end = 'Dlvry' + i + 'To'
        addtional_start = 'Dlvry' + i + 'From1'
        addtional_end = 'Dlvry' + i + 'To1'
        forms = [primary_start, primary_end, addtional_start, addtional_end]
        data[i] = [df1[j].values[0] for j in forms]
    # 再次转成dataframe
    df2 = pd.DataFrame(data)
    df2['title'] = ['PrimaryStart', 'PrimaryEnd', 'AdditionStart', 'AdditionEnd']
    cols = df2.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df2 = df2[cols]
    return df2


def get_forecastError(shipto, conn):
    '''获取 forecastError'''
    table_name = 'forecastError '
    sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
    df = pd.read_sql(sql, conn)
    if len(df) == 0:
        fe = 'NotFound'
    else:
        fe = str(round(df.AverageError.values[0]*100)) + '%'
    return fe

def get_t4_t6_value(shipto, conn):

    table_name = "t4_t6_data"
    sql = '''SELECT * FROM {} WHERE LocNum = {}'''.format(table_name, shipto)
    df = pd.read_sql(sql, conn)
    t4_t6_val = "unknown"

    for i, row in df.iterrows():
        t4_t6_val = round(row['beforeToRoHours_rolling_mean'],1)
    return t4_t6_val



def weight_length_factor(uom):
    '''因为 LBSHELL 与 odbc 导出 单位转化原因，需要设置一个 factor 进行还原'''
    if uom == 'Inch':
        return 2.54
    elif uom == 'M':
        return 10
    elif uom == 'MM':
        return 1/10
    else:
        return 1

def clean_detailed_info():
    '''before fill in the info, we need to clean the previous text'''
    lable_list = [lb2, lb4, lb6, lb8, lb10, lb12, lb14, lb16, lb17, lb18, lb20, lb22, t4_t6_value_label]
    for lb_Temp in lable_list:
        lb_Temp.config(text='')


def show_info(custName, TR_time, Risk_time, RO_time, full, TR,
              Risk, RO, ts_forecast_usage, galsperinch, uom, fe,
              primary_dt, max_payload,t4_t6_value
              ):
    '''显示客户的充装的详细信息'''
    # 20220624 we need to clean the previous info first
    clean_detailed_info()
    # unitOfLength_dict = {1: 'CM', 2: 'Inch', 3: 'M', 4: 'MM', 5: 'Percent', 6: 'Liters'}
    factor = weight_length_factor(uom)
    # 2023-10-31 修改
    if Risk_time is None:
        # 只挑选部分内容显示
        lb2.config(text=custName)
        full_cm = int(full/galsperinch/factor)
        lb10.config(text='{} KG / {} {}'.format(full, full_cm, uom))
        TR_cm = int(TR/galsperinch/factor)
        lb12.config(text='{} KG / {} {}'.format(TR, TR_cm, uom))
        RO_cm = int(RO/galsperinch/factor)
        lb16.config(text='{} KG / {} {}'.format(RO, RO_cm, uom))
    else:
        # 首先要进行字符串的转换
        tr = TR_time.strftime("%Y-%m-%d %H:%M")
        risk = Risk_time.strftime("%Y-%m-%d %H:%M")
        ro = RO_time.strftime("%Y-%m-%d %H:%M")
        lb2.config(text=custName)
        lb4.config(text=tr)
        lb6.config(text=risk)
        lb8.config(text=ro)
        full_cm = int(full/galsperinch/factor)
        lb10.config(text='{} KG / {} {}'.format(full, full_cm, uom))
        TR_cm = int(TR/galsperinch/factor)
        lb12.config(text='{} KG / {} {}'.format(TR, TR_cm, uom))
        Risk_cm = int(Risk/galsperinch/factor)
        lb14.config(text='{} KG / {} {}'.format(Risk, Risk_cm, uom))
        RO_cm = int(RO/galsperinch/factor)
        lb16.config(text='{} KG / {} {}'.format(RO, RO_cm, uom))
        if len(ts_forecast) >= 2:
            s_time = ts_forecast.index[0].strftime("%m-%d %H:%M")
            # modify 20220624
            e_time = ts_forecast.index[min(7, len(ts_forecast)-1)].strftime("%m-%d %H:%M")
            hourly_usage = round(ts_forecast_usage[:8].mean().values[0], 1)
            hourly_usage_cm = (hourly_usage/galsperinch/factor).round(1)
            # print(ts_forecast_usage[:8])
            # print(len(ts_forecast_usage[:8]))
            lb17.config(text='{}~{}\n 预测小时用量'.format(s_time, e_time))
            lb18.config(text='{} KG / {} {}'.format(hourly_usage, hourly_usage_cm, uom))
        else:
            lb17.config(text='')
            lb18.config(text='')

    lb20.config(text=fe)
    lb21.config(text='{} MaxPayload'.format(primary_dt))
    payload = int(max_payload) if isinstance(max_payload, float) else max_payload
    lb22.config(text=payload)
    t4_t6_value_label.config(text=t4_t6_value)


def time_validate_check(conn, shipto):
    ''''检查box的内容是否正确'''
    validate_flag = (True, True)
    try:
        fromTime = pd.to_datetime(from_box.get())
    except ValueError:
        validate_flag = (False, 'From Time Wrong!')
        return validate_flag
    try:
        toTime = pd.to_datetime(to_box.get())
    except ValueError:
        validate_flag = (False, 'To Time Wrong!')
        return validate_flag
    df = get_forecastReading(shipto, fromTime, toTime, conn)
    # 为了防止 df 是空的：
    if len(df) == 0:
        # 这表明没有预测数据， 但是也要显示历史数据
        return (True, True)
    checkValue = df.Forecasted_Reading.values[0]
    if checkValue == 777777:
        validate_flag = (False, '此shipto无法抓取读数数据!')
    elif checkValue == 888888:
        validate_flag = (False, '读数少于一个月,不足以提供预测!')
    elif checkValue == 999999:
        validate_flag = (False, '近2日的读数缺失!')
    # print(checkValue, validate_flag)
    return validate_flag


def plot_vertical_lines(fromTime, toTime, TR_time, Risk_time, RO_time, full):
    '''以下画垂直线, 一共有 10 种情况'''
    alpha = 0.4
    if fromTime < toTime < TR_time < Risk_time < RO_time:
        # 其实这表明查询的是历史记录
        pass
    if fromTime <= TR_time <= toTime <= Risk_time <= RO_time:
        ax.axvline(x=TR_time, color='green', linewidth=1)
        ax.fill_between(x=[TR_time, toTime], y1=full, facecolor='green', alpha=alpha)
    if fromTime <= TR_time <= Risk_time <= toTime <= RO_time:
        ax.axvline(x=TR_time, color='green', linewidth=1)
        ax.axvline(x=Risk_time, color='yellow', linewidth=1)
        ax.fill_between(x=[TR_time, Risk_time], y1=full, facecolor='green', alpha=alpha)
        ax.fill_between(x=[Risk_time, toTime], y1=full, facecolor='red', alpha=alpha)
    if fromTime <= TR_time <= Risk_time <= RO_time <= toTime:
        # 这个是最完整形态
        ax.axvline(x=TR_time, color='green', linewidth=1)
        ax.axvline(x=Risk_time, color='yellow', linewidth=1,)
        ax.axvline(x=RO_time, color='red', linewidth=1)
        ax.fill_between(x=[TR_time, Risk_time], y1=full, facecolor='green', alpha=alpha)
        ax.fill_between(x=[Risk_time, RO_time], y1=full, facecolor='red', alpha=alpha)
    if TR_time <= fromTime <= toTime <= Risk_time <= RO_time:
        ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='green', alpha=alpha)
    if TR_time <= fromTime <= Risk_time <= toTime <= RO_time:
        ax.axvline(x=Risk_time, color='yellow', linewidth=1)
        ax.fill_between(x=[fromTime, Risk_time], y1=full, facecolor='green', alpha=alpha)
        ax.fill_between(x=[Risk_time, toTime], y1=full, facecolor='red', alpha=alpha)
    if TR_time <= fromTime <= Risk_time <= RO_time <= toTime:
        ax.axvline(x=Risk_time, color='green', linewidth=1)
        ax.axvline(x=RO_time, color='red', linewidth=1)
        ax.fill_between(x=[fromTime, Risk_time], y1=full, facecolor='green', alpha=alpha)
        ax.fill_between(x=[Risk_time, RO_time], y1=full, facecolor='red', alpha=alpha)
    if TR_time <= Risk_time <= fromTime <= toTime <= RO_time:
        ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='red', alpha=alpha)
    if TR_time <= Risk_time <= fromTime <= RO_time <= toTime:
        ax.axvline(x=RO_time, color='red', linewidth=1)
        ax.fill_between(x=[fromTime, RO_time], y1=full, facecolor='red', alpha=alpha)
    if TR_time <= Risk_time <= RO_time <= fromTime <= toTime:
        ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='red', alpha=alpha)


def get_plot_basic(framename):
    '''获取作图框架'''
    fig = Figure(figsize=(5, 4), dpi=80)
    gs = fig.add_gridspec(1, 2,  width_ratios=(4, 1),
                      left=0.1, right=0.9, bottom=0.1, top=0.9,
                      wspace=0.1, hspace=0.05)
    ax = fig.add_subplot(gs[0, 0])
    ax_histy = fig.add_subplot(gs[0, 1], sharey=ax)
    # ax = fig.add_subplot(111)
    canvas = FigureCanvasTkAgg(fig, master=framename)  # A tk.DrawingArea.
    canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
    # canvas.get_tk_widget().grid(row=0, column=1)
    toolbar = NavigationToolbar2Tk(canvas, framename)
    return fig, ax, ax_histy, canvas, toolbar


def update_annot(pos, text):
    '''填写注释内容'''
    # pos = sc.get_offsets()[ind["ind"][0]]
    global annot
    annot.xy = pos
    # print(annot.xy)
    annot.set_text(text)
    # print(annot.get_text())
    # annot.get_bbox_patch().set_facecolor(cmap(norm(c[ind["ind"][0]])))
    # annot.get_bbox_patch().set_alpha(0.8)


def hover(event):
    '''悬浮'''
    global annot
    global canvas
    if annot is None:
        return
    vis = annot.get_visible()
    for curve in ax.get_lines():
        # print(curve)
        # Searching which data member corresponds to current mouse position
        if curve.contains(event)[0]:
            graph_id = curve.get_gid()
            # print('global ', 'ts_manual' in globals())
            # print('locals ', 'ts_manual' in locals())
            if 'ts_manual' in globals():
                graph_dict = {'point_history': ts_history,
                              'point_forecast': ts_forecast,
                              'point_forecastBeforeTrip': ts_forecastBeforeTrip,
                              'point_manual': ts_manual}
            else:
                graph_dict = {'point_history': ts_history,
                              'point_forecast': ts_forecast,
                              'point_forecastBeforeTrip': ts_forecastBeforeTrip}
            if graph_id in graph_dict.keys():
                if vis:
                    # 说明已经有了一个 annot, 就不再显示第二个了。
                    return
                df_data = graph_dict[graph_id]
                full = df_info.FullTrycockGals.values[0]
                ind = curve.contains(event)[1]['ind'][0]
                # pos = (event.x, event.y)
                pos = (event.xdata, event.ydata)
                show_time = df_data.index[ind].strftime("%Y-%m-%d %H:%M")
                show_level = int(df_data.values.flatten()[ind])
                # 转成长度单位
                galsperinch = df_info.GalsPerInch.values[0]
                unitOfLength = df_info.UnitOfLength.values[0]
                uom = unitOfLength_dict[unitOfLength]
                factor = weight_length_factor(uom)
                show_level_cm = int(round(show_level/(galsperinch*factor), 1))
                # 可卸货量
                loadAMT = int(full - show_level)
                loadAMT_cm = int(round(loadAMT/(galsperinch*factor), 1))
                text = '''{}\nLevel: {} KG / {} {}\n可卸货量: {} KG / {} {}'''.format(
                    show_time, show_level, show_level_cm, uom, loadAMT, loadAMT_cm, uom)
                update_annot(pos, text)
                annot.set_visible(True)
                canvas.draw_idle()
                # print('000')
            else:
                # pass
                if vis:
                    # print(999)
                    annot.set_visible(False)
                    canvas.draw_idle()
        else:
            # pass
            if vis:
                # print(777)
                annot.set_visible(False)
                canvas.draw_idle()


def hover_disappear(event):
    '''取消悬浮'''
    global annot
    global canvas
    if mutex.acquire(2):
        if annot is None:
            mutex.release()
            return
        vis = annot.get_visible()
        if vis:
            # curves = [i.get_gid() for i in ax.get_lines()]
            # print(curves)
            for curve in ax.get_lines():
                # print('curve:', curve)
                if curve.contains(event)[0]:
                    graph_id = curve.get_gid()
                    print('vis test:', vis, id(annot), graph_id)
                    # if graph_id is None:
                    #     mutex.release()
                    #     return
                    # hover_curves = ['point_history', 'point_forecast',
                    #                 'point_forecastBeforeTrip', 'point_manual']
                    hover_curves = ['point_history', 'line_history', 'point_forecast',
                                    'line_forecast', 'point_forecastBeforeTrip',
                                    'line_forecastBeforeTrip', 'line_join']
                    if graph_id not in hover_curves:
                        time.sleep(2)
                        annot.set_visible(False)
                        canvas.draw_idle()
                        # print('after:', annot.get_visible(), id(annot), graph_id)
                        mutex.release()
                        return
                else:
                    # print(333)
                    time.sleep(2)
                    annot.set_visible(False)
                    canvas.draw_idle()
                    print('no touch:', annot.get_visible(), id(annot))
                #     time.sleep(1)
                #     annot.set_visible(False)
                #     canvas.draw_idle()
        mutex.release()


def main_plot(root, conn, lock):
    '''作图主函数'''
    custName = listbox_customer.get(listbox_customer.curselection()[0])
    print('Customer: {}'.format(custName))
    # 检查 From time 和 to time 是否正确
    if custName not in df_name_forecast.index:
        messagebox.showinfo(parent=root, title='Warning', message='No Data To Show!')
        if lock.locked():
            lock.release()
    else:
        shipto = int(df_name_forecast.loc[custName].values[0])
        TELE_type = df_name_forecast.loc[custName, 'Subscriber']
        validate_flag = time_validate_check(conn, shipto)
        # print(custName, type(custName))
        # 如果查询得到 shipto,则显示 shipto,否则 将 shipto 设为 1
        if (not validate_flag[0]) and var_TELE.get() == 0:
            # 2023-10-31 新增逻辑
            # 如果 var_TELE 为 1,说明正在使用 api, 
            error_msg = validate_flag[1]
            if 'Time Wrong' in error_msg:
                # 说明时间填错
                messagebox.showinfo(parent=root, title='Warning', message=error_msg)
                if lock.locked():
                    lock.release()
            else:
                # 说明时间没有填错, 遇到了 无法预测的情况
                # 提醒采用 dol api 的选项
                error_msg = error_msg + ' -> 请使用 api 试试'
                messagebox.showinfo(parent=root, title='Warning', message=error_msg)
                if lock.locked():
                    lock.release()
        else:
            fromTime = pd.to_datetime(from_box.get())
            toTime = pd.to_datetime(to_box.get())
            # 2023-09-04 更新 DOL API 数据
            # print(var_TELE.get())
            # 如果 shipto 是龙口的,不需要更新,不是龙口的,需要 api 查询后更新
            # 2024-09-03 更新： 只有是 DOL 或 LCT 才需要更新；
            if var_TELE.get() == 1:
                if TELE_type == 3:
                    updateDOL(shipto, conn)
                elif TELE_type == 7:
                    updateLCT(shipto, conn)
                else:
                    pass
            # 获取数据
            # 首先根据客户简称,获取 shipto
            global df_info
            df_info = get_customerInfo(shipto, conn)
            # print('ass')
            # print(df_info)
            # shipto = df_info.LocNum.values[0]
            df_history = get_historyReading(shipto, fromTime, toTime, conn)
            # print('history len:', len(df_history))
            df_forecastBeforeTrip = get_forecastBeforeTrip(shipto, fromTime, toTime, conn)
            # print(df_history.head())
            df_forecast = get_forecastReading(shipto, fromTime, toTime, conn)
            df_max_payload = get_max_payload_by_ship2(
                conn=conn,
                ship2=str(shipto),
            )
            # 2023-10-31 需要做一步判断：如果 df_forecast 的 Forecasted_Reading 异常,那么就需要清空。
            if len(df_forecast) > 0:
                if df_forecast.Forecasted_Reading.values[0] in [777777, 888888, 999999]:
                    df_forecast.Forecasted_Reading = None

            current_primary_dt = '__'
            current_max_payload = 'unknown'
            for i, row in df_max_payload.iterrows():
                if not pd.isna( row['LicenseFill'] ) and row['LicenseFill'] > 0:
                    current_max_payload = row['LicenseFill']
                current_primary_dt = row['CorporateIdn']

            # print(df_forecast.head())
            # print(df_info.columns)
            # 作图数据处理
            global ts_history, ts_forecast, ts_forecastBeforeTrip
            ts_history = df_history[['ReadingDate', 'Reading_Gals']].set_index('ReadingDate')
            ts_forecast = df_forecast[['Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
            ts_forecastBeforeTrip = df_forecastBeforeTrip[[
                'Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
            ts_forecast_usage = df_forecast[['Next_hr',
                                            'Hourly_Usage_Rate']].set_index('Next_hr')
            # 记录四个液位值
            full = df_info.FullTrycockGals.values[0]
            TR = df_info.TargetGalsUser.values[0]
            RO = df_info.RunoutGals.values[0]
            Risk = (RO + TR)/2
            # 防止 Risk 是 None 而 无法 int
            Risk = Risk if Risk is None else int(Risk) 
            galsperinch = df_info.GalsPerInch.values[0]
            unitOfLength = df_info.UnitOfLength.values[0]
            uom = unitOfLength_dict[unitOfLength]
            # 记录三个液位时间
            if len(df_forecast) > 0:
                TR_time = df_forecast.iloc[0].TargetRefillDate
                Risk_time = df_forecast.iloc[0].TargetRiskDate
                RO_time = df_forecast.iloc[0].TargetRunoutDate
            else:
                TR_time = None
                Risk_time = None
                RO_time = None
            # 开始作图
            # 没想到这句话还这么重要(在hover的时候造成了极大的困扰)
            ax.clear()
            # 新增注释
            global annot
            # annot = ax.annotate("", xy=(0, 0), xytext=(20, 20), textcoords="offset points",
            #                     bbox=dict(boxstyle="round", fc="lightblue",
            #                               ec="steelblue", alpha=1),
            #                     arrowprops=dict(arrowstyle="->"))
            annot = ax.annotate("", xy=(0, 0), xytext=(20, 12), textcoords="offset points",
                                bbox=dict(boxstyle="round", fc="lightblue",
                                          ec="steelblue", alpha=1),
                                arrowprops=dict(arrowstyle="->"),
                                annotation_clip=True)
            annot.set_visible(False)
            if len(df_history) > 0:
                pic_title = '{}({}) History and Forecast Level'.format(custName, shipto)
            else:
                pic_title = '{}({}) No History Data'.format(custName, shipto)
            ax.set_title(pic_title, fontsize=20)
            ax.set_ylabel('K G')
            ax.set_ylim(bottom=0, top=full*1.18)
            # ax.set_xlabel('Date')
            ax.plot(ts_history, color='blue', marker='o', markersize=6,
                    linestyle='None', gid='point_history')
            ax.plot(ts_history, color='blue', label='Actual', linestyle='-', gid='line_history')
            ax.plot(ts_forecast, color='green', marker='o', markersize=6, alpha=0.45,
                    linestyle='None', gid='point_forecast')
            ax.plot(ts_forecast, color='green', label='Forecast', alpha=0.45,
                    linestyle='dashed', gid='line_forecast')
            ax.plot(ts_forecastBeforeTrip, color='orange', marker='o', markersize=6,
                    linestyle='None', gid='point_forecastBeforeTrip')
            ax.plot(ts_forecastBeforeTrip, color='orange',
                    label='FcstBfTrip', linestyle='dashed', gid='line_forecastBeforeTrip')
            if (len(ts_forecastBeforeTrip) > 0 and len(ts_forecast) > 0):
                ts_join = pd.concat([ts_forecastBeforeTrip.last('1S'), ts_forecast.first('1S')])
                # print(ts_join)
                ax.plot(ts_join, color='orange', linestyle='dashed', gid='line_join')
            # decide to plot manual forecast line
            if manual_plot:
                df_manual = get_manualForecast(shipto, fromTime, toTime, conn)
                global ts_manual
                ts_manual = df_manual[['Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
                ax.plot(ts_manual, color='purple', marker='o',  markersize=6,
                        linestyle='None', gid='point_manual', alpha=0.6)
                ax.plot(ts_manual, color='purple', label='Manual',
                        linestyle='dashed', alpha=0.6)
            # 以下画水平线
            ax.axhline(y=full, color='grey', linewidth=2, label='Full', gid='line_full')
            ax.axhline(y=TR, color='green', linewidth=2, label='TR', gid='line_TR')
            if Risk is not None:
                ax.axhline(y=Risk, color='yellow', linewidth=2, label='Risk', gid='line_Risk')
            ax.axhline(y=RO, color='red', linewidth=2, label='RunOut', gid='line_RO')
            # 画竖直线,较繁琐。具体函数见定义
            if TR_time is not None:
                plot_vertical_lines(fromTime, toTime, TR_time, Risk_time, RO_time, full)
            if (toTime-fromTime).days <= 12:
                ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 1)))
            elif (toTime-fromTime).days <= 24:
                ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 2)))
            else:
                ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 4)))
            # fig.autofmt_xdate()
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            # plot for second y-axis
            factor = weight_length_factor(uom)
            def kg2cm(x):
                return x / (galsperinch * factor)

            def cm2kg(x):
                return x * (galsperinch * factor)
            secay = ax.secondary_yaxis('right', functions=(kg2cm, cm2kg))
            # secay.set_ylabel(uom)

            # ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            # xticks_range = pd.date_range(start=fromTime, end=toTime)
            # xticks_lables = [i.strftime("%m-%d") for i in xticks_range]
            # # print(y)
            # ax.set_xticklabels(xticks_lables)
            # ax.grid(b=True)
            ax.grid()
            # ax.legend(fontsize=8)
            # 把 legend 放在图片外部
            # ax.legend(bbox_to_anchor=(1.04, 1.0), loc='upper left', fontsize=8)
            # 2024-04-18 新增 直方图
            beforeRD = get_beforeReading(conn, shipto)
            if len(beforeRD) > 0:
                binwidth = 200
                xymax = np.max(np.abs(beforeRD))
                lim = (int(xymax/binwidth) + 1) * binwidth
                bins = np.arange(0, lim + binwidth, binwidth)
            else:
                bins = np.arange(0, 2, 1)
            # print(type(beforeRD), beforeRD)
            # mu = np.random.uniform(low=50, high=1000)
            # beforeRD1 = np.random.normal(mu, 10, size=60)
            ax_histy.clear()
            axHist_info = ax_histy.hist(beforeRD, bins=bins, edgecolor='black', color='blue', orientation='horizontal')
            ax_histy.tick_params(
                                axis='y',
                                which='both',      # both major and minor ticks are affected
                                bottom=False,      # ticks along the bottom edge are off
                                top=False,         # ticks along the top edge are off
                                # labelbottom=False,
                                labelleft=False,
                                # left=False
                                )
            if len(beforeRD) > 0:
                max_count = np.max(axHist_info[0])
                xticks = define_xticks(max_count)
            else:
                xticks = np.arange(0, 2, 1)
            ax_histy.set_xticks(xticks)
            ax_histy.grid()
            # plt.tight_layout()
            canvas.draw_idle()
            toolbar.update()
            # print(111)
            # path = 'C:\Users\zhoud8\Documents\OneDrive - Air Products and Chemicals, Inc\python_project\gui\Forecasting'
            if save_pic:
                # print(save_pic)
                # print('global ', 'fig' in globals())
                fig.savefig('./feedback.png')
                # print(222)
            # 点击作图时,同时显示客户的充装的详细信息
            fe = get_forecastError(shipto, conn)
            t4_t6_value = get_t4_t6_value(shipto, conn)
            show_info(custName, TR_time, Risk_time, RO_time, full,
                      TR, Risk, RO, ts_forecast_usage, galsperinch, uom, fe,
                      primary_dt=current_primary_dt, max_payload=current_max_payload,
                      t4_t6_value =t4_t6_value)
            # 显示历史液位
            treeview_data(conn, shipto, reading_tree, 'reading')
            # 显示送货窗口
            treeview_data(conn, shipto, deliveryWindow_tree, 'deliveryWindow')
            if lock.locked():
                lock.release()


def define_xticks(num):
    '''对直方图设定刻度；'''
    if num >= 50:
        binwidth = 10
    elif num >= 25:
        binwidth = 5
    elif num >= 20:
        binwidth = 4
    elif num >= 13:
        binwidth = 3
    elif num >= 5:
        binwidth = 2
    else:
        binwidth = 1
    lim = (int(num/binwidth) + 1) * binwidth
    xticks = np.arange(0, lim + binwidth, binwidth)
    return xticks


def plot(event, root, conn, lock):
    '''多线程作图主函数'''
    starttime = time.time()
    # lock the thread
    while True:
        if lock.acquire(blocking=False) is True:
            break
        else:
            endtime = time.time()
            duration = round(endtime - starttime, 3)
            print('lock is not free')
            time.sleep(0.5)
            if duration > 8:
                if lock.locked():
                    lock.release()
    # print(event, type(event))
    # custName = listbox_customer.get(tk.ANCHOR)
    try:
        main_plot(root, conn, lock)
    except Exception as e:
        print(e)
        if lock.locked():
            lock.release()


# def onclick(event):
#     if event.dblclick:
#         x_pos = round(event.xdata)
#         y_pos = round(event.ydata)
#         # x1 = event.x
#         # y1 = event.y
#         print(x_pos, y_pos)
#     # print(x1, y1)
def onpick(event):
    thisline = event.artist
    xdata = thisline.get_xdata()
    ydata = thisline.get_ydata()
    ind = event.ind
    # x_info = mdates.num2date(xdata[ind]).strftime("%Y-%m-%d %H:%M")
    # y_info = mdates.num2date(ydata[ind]).strftime("%Y-%m-%d %H:%M")
    print(event.artist)
    print(isinstance(event.artist, Line2D))
    print('onpick points:', xdata[ind], ydata[ind])
    # print('xdata ydata:', xdata, ydata)

# def on_key_press(event):
#     print("you pressed {}".format(event.key))
#     key_press_handler(event, canvas, toolbar)


# canvas.mpl_connect("key_press_event", on_key_press)


# def _quit():
#     root.quit()     # stops mainloop
#     root.destroy()  # this is necessary on Windows to prevent
# Fatal Python Error: PyEval_RestoreThread: NULL tstate


def refresh_history_data(cur, conn, file_dict):
    '''刷新 历史数据,区分AS用,还是 Forecasting 用'''
    start_time = time.time()
    if len(file_dict) == 1:
        # 用于 AS
        filename = file_dict['chengdu'][1]
        df_history = pd.read_csv(filename)
    else:
        # 用于 Forecasting
        df_history = pd.DataFrame()
        for region in file_dict:
            filename = file_dict[region][2]
            filename_back = file_dict[region][3]
            try:
                df_temp = pd.read_csv(filename)
                if len(df_temp) <= 10000:
                    # 说明数据正在写,有缺失
                    print('file {} is missing data, use backup.'.format(filename))
                    df_temp = pd.read_csv(filename_back)
            except Exception as e:
                print('cannot read file {} , use backup.'.format(filename), e)
                df_temp = pd.read_csv(filename_back)
            # 20220622 modify
            if len(df_temp) > 0:
                df_history = pd.concat([df_history, df_temp], ignore_index=True)
    end_time = time.time()
    print('refresh history {} seconds'.format(round(end_time - start_time)))
    df_history.ReadingDate = pd.to_datetime(df_history.ReadingDate, format='mixed')
    df_history = df_history.sort_values(['LocNum', 'ReadingDate']).reset_index(drop=True)
    df_history.Reading_Gals = (df_history.Reading_Gals).round()
    table_name = 'historyReading'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    df_history[['LocNum', 'ReadingDate', 'Reading_Gals']].to_sql(
        table_name, con=conn, if_exists='replace', index=False)


def refresh_forecast_data(cur, conn, file_dict):
    '''刷新 预测数据,区分AS用,还是 Forecasting 用'''
    start_time = time.time()
    if len(file_dict) == 1:
        # 用于 AS
        filename = file_dict['chengdu'][0]
        df_forecast = pd.read_csv(filename)
    else:
        # 用于 Forecasting
        df_forecast = pd.DataFrame()
        for region in file_dict:
            filename = file_dict[region][0]
            filename_back = file_dict[region][1]
            try:
                df_temp = pd.read_csv(filename)
                if len(df_temp) <= 1500:
                    print('file {} is missing data, use backup.'.format(filename))
                    # 说明数据正在写,有缺失
                    df_temp = pd.read_csv(filename_back)
            except Exception:
                print('cannot read file {} , use backup.'.format(filename))
                df_temp = pd.read_csv(filename_back)
            if region == 'LB_LCT':
                # 2024-09-02 新增防止 DOL 中 混入 LCT 数据影响
                lb_lct_shiptos = list(df_temp.LocNum.unique())
            # print(filename, df_temp.shape)
            # df_temp.to_excel('{}.xlsx'.format(name))
            # 20220622 modify
            # df_forecast = df_forecast.append(df_temp)
            if len(df_temp) > 0:
                df_forecast = pd.concat([df_forecast, df_temp], ignore_index=True)
    end_time = time.time()
    print('refresh forecast {} seconds'.format(round(end_time - start_time)))
    df_forecast.Next_hr = pd.to_datetime(df_forecast.Next_hr)
    # 2024-09-02 新增：去除 DOL 中 的 LCT 数据
    f1 = df_forecast.Next_hr.isna()
    f2 = df_forecast.LocNum.isin(lb_lct_shiptos)
    # 这一步的意思是说： 如果是一个 LCT 客户，并且 Next_hr 为空，就剔除出去；
    df_forecast = df_forecast[~(f1&f2)]
    df_forecast = df_forecast.sort_values(['LocNum', 'Next_hr'])
    df_forecast.Forecasted_Reading = (df_forecast.Forecasted_Reading).astype(float).round()
    use_cols = ['LocNum', 'Next_hr', 'Hourly_Usage_Rate', 'Forecasted_Reading', 'RiskGals',
                'TargetRefillDate',	'TargetRiskDate', 'TargetRunoutDate']
    df_forecast1 = df_forecast.loc[df_forecast.Forecasted_Reading.notna(
    ), use_cols].reset_index(drop=True).copy()
    # df_forecast1.to_excel('aaa.xlsx')
    table_name = 'forecastReading'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    df_forecast1.to_sql(table_name, con=conn, if_exists='replace', index=False)


def refresh_forecastBeforeTrip_data(cur, conn, file_dict):
    '''刷新 送货前数据,区分AS用,还是 Forecasting 用'''
    start_time = time.time()
    if len(file_dict) == 1:
        # 用于 AS
        filename = file_dict['chengdu'][2]
        df_forecast = pd.read_csv(filename)
    else:
        # 用于 Forecasting
        df_forecast = pd.DataFrame()
        for region in file_dict:
            filename = file_dict[region][4]
            filename_back = file_dict[region][5]
            try:
                df_temp = pd.read_csv(filename)
            except Exception:
                print('cannot read file {} , use backup.'.format(filename))
                df_temp = pd.read_csv(filename_back)
            # 20220622 modify
            # df_forecast = df_forecast.append(df_temp)
            if len(df_temp) > 0:
                df_forecast = pd.concat([df_forecast, df_temp], ignore_index=True)
    end_time = time.time()
    print('refresh drop {} seconds'.format(round(end_time - start_time)))
    df_forecast.Next_hr = pd.to_datetime(df_forecast.Next_hr)
    df_forecast = df_forecast.sort_values(['LocNum', 'Next_hr'])
    df_forecast.Forecasted_Reading = (df_forecast.Forecasted_Reading).astype(float).round()
    df_forecast1 = df_forecast.loc[df_forecast.Forecasted_Reading.notna(
    ), ['LocNum', 'Next_hr', 'Forecasted_Reading']].reset_index(drop=True).copy()
    table_name = 'forecastBeforeTrip'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    df_forecast1.to_sql(table_name, con=conn, if_exists='replace', index=False)


def refresh_fe(cur, conn):
    '''刷新 forecast error'''
    # 2023-03-06 dongliang modified
    # filepath = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling\\ForecastErrorTesting'
    filepath = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling\\ForecastingInputOutput\\ErrorRecording'
    filename = os.path.join(filepath, 'Error Result.csv')
    df_fe = pd.read_csv(filename)
    df_fe = df_fe[df_fe.AverageError_SEH.notna()].reset_index(drop=True)
    if len(df_fe) > 0:
        df_fe['AverageError'] = df_fe.apply(lambda row: min(row['AverageError_SEH'], row['AverageError_ARIMA']), axis=1)
    else:
        df_fe['AverageError'] = None
    use_cols = ['LocNum', 'AverageError']
    df_fe = df_fe[use_cols]
    table_name = 'forecastError'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    df_fe.to_sql(table_name, con=conn, if_exists='replace', index=False)


def refresh_data(cur, conn, file_dict):
    try:
        refresh_history_data(cur, conn, file_dict)
        refresh_forecast_data(cur, conn, file_dict)
        refresh_forecastBeforeTrip_data(cur, conn, file_dict)
        refresh_fe(cur, conn)
        logConnection(log_file, 'refreshed')
        messagebox.showinfo(title='success', message='data to sqlite success!')
    except Exception:
        messagebox.showinfo(title='failure', message='failure, please check!')


def info_fiter_frame(par_frame):
    '''建立筛选部分的frame,也即第一模块'''
    frame_name = tk.LabelFrame(par_frame, text='Filter')
    frame_name.grid(row=0, column=0, padx=5, pady=5)
    return frame_name


def info_cust_frame(par_frame):
    '''建立客户名称的frame,也即第二模块'''
    frame_name = tk.LabelFrame(par_frame, text='Cust')
    frame_name.grid(row=0, column=0, padx=5, pady=5)
    return frame_name


def input_framework(framename, cur, conn, file_dict):
    # 输入 起始日期
    lb_fromtime = tk.Label(framename, text='from time')
    lb_fromtime.grid(row=0, column=0, padx=10, pady=5)
    global from_box, to_box
    from_box = tk.Entry(framename)
    # 初始化 起始日期
    startday = (datetime.now().date() - timedelta(days=2)).strftime("%Y-%m-%d")
    from_box.insert(0, startday)
    from_box.grid(row=0, column=1, padx=10, pady=5)
    # 输入 结束日期
    lb_totime = tk.Label(framename, text='to time')
    lb_totime.grid(row=1, column=0, padx=10, pady=5)
    to_box = tk.Entry(framename)
    # 初始化 结束日期
    endday = (datetime.now().date() + timedelta(days=3)).strftime("%Y-%m-%d")
    to_box.insert(0, endday)
    to_box.grid(row=1, column=1, padx=10, pady=5)
    # 设置刷新按钮
    btn_refresh = tk.Button(framename, text='Refresh data',
                            command=lambda: refresh_data(cur, conn, file_dict))
    btn_refresh.grid(row=2, column=0, padx=10, pady=10)
    # 设置是否需要 从DOL API 下载数据
    global var_TELE
    var_TELE = tk.IntVar()
    check_TELE = tk.Checkbutton(framename, text='远控 最新', variable=var_TELE, onvalue=1, offvalue=0)
    check_TELE.grid(row=2, column=1, padx=1, pady=10)


def subRegion_boxlist(framename):
    '''subRegion boxlist'''
    global listbox_subRegion
    listbox_subRegion = tk.Listbox(framename, height=5, width=10, exportselection=False)
    subRegion_list = df_name_forecast.SubRegion.unique()
    for item in sorted(subRegion_list):
        listbox_subRegion.insert(tk.END, item)
    listbox_subRegion.grid(row=0, column=0, padx=1, pady=1)


def terminal_boxlist(framename):
    '''terminal boxlist'''
    frame_name = tk.LabelFrame(framename)
    # scrollbar
    scroll_y = tk.Scrollbar(frame_name, orient=tk.VERTICAL)
    # 这里需要特别学习：exportselection=False
    # 保证了 两个 Listbox 点击一个时,不影响第二个。
    global listbox_terminal
    listbox_terminal = tk.Listbox(
        frame_name, selectmode="extended", height=6, width=12, yscrollcommand=scroll_y.set, exportselection=False)
    # terminal_list = df_name_forecast.PrimaryTerminal.unique()
    # for item in terminal_list:
    #     listbox_terminal.insert(tk.END, item)
    scroll_y.config(command=listbox_terminal.yview)
    scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
    frame_name.grid(row=0, column=1, padx=1, pady=1)
    listbox_terminal.pack()


def products_boxlist(framename):
    '''products boxlist'''
    global listbox_products
    listbox_products = tk.Listbox(framename, selectmode="extended",
                                  height=4, width=10, exportselection=False)
    # products_list = df_name_forecast.ProductClass.unique()
    # for item in products_list:
    #     listbox_products.insert(tk.END, item)
    listbox_products.grid(row=1, column=0, padx=1, pady=1)


def demandType_boxlist(framename):
    global listbox_demandType
    listbox_demandType = tk.Listbox(framename, selectmode="extended",
                                    height=4, width=10, exportselection=False)
    listbox_demandType.grid(row=1, column=1, padx=1, pady=1)


def customer_query(framename):
    global entry_name
    entry_name = tk.Entry(framename, width=20, bg='white', fg='black', borderwidth=1)
    # entry_name.insert(0, 'name or shipto:')
    entry_name.grid(row=0, column=0)


# def click_entry_name(event):
#     entry_name.delete(0, 'end')
#
#
# def leave_entry_name(event):
#     entry_name.delete(0, 'end')
#     entry_name.insert(0, 'name or shipto:')
    # root.focus()


def customer_boxlist(framename):
    ''' customer boxlist'''
    frame_name = tk.LabelFrame(framename, text='Customer Name')
    # 新增滚动轴 scrollbar
    scroll_y = tk.Scrollbar(frame_name, orient=tk.VERTICAL)
    # 这里需要特别学习：exportselection=False
    # 保证了 两个 Listbox 点击一个时,不影响第二个。
    global listbox_customer
    listbox_customer = tk.Listbox(
        frame_name, height=10, width=20, yscrollcommand=scroll_y.set, exportselection=False)
    # listbox_customer = tk.Listbox(
    #     frame_name, height=10, width=20, yscrollcommand=scroll_y.set)
    # custName_list = df_name_forecast.index
    # for item in custName_list:
    #     listbox_customer.insert(tk.END, item)
    scroll_y.config(command=listbox_customer.yview)
    scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
    frame_name.grid(row=1, column=0, padx=5, pady=5, columnspan=2)
    listbox_customer.pack()


def show_list_cust(event):
    '''当点击 terminal 的时候显示客户名单'''
    listbox_customer.delete(0, tk.END)
    if listbox_subRegion.curselection() is None or len(listbox_subRegion.curselection()) == 0:
        SubRegion = None
    else:
        SubRegion = listbox_subRegion.get(listbox_subRegion.curselection()[0])
    if listbox_terminal.curselection() is None or len(listbox_terminal.curselection()) == 0:
        cur_terminal = None
    else:
        cur_no = listbox_terminal.curselection()
        cur_terminal = [listbox_terminal.get(i) for i in cur_no]
        # print(cur_no, '->', cur_terminal)
    if listbox_products.curselection() is None or len(listbox_products.curselection()) == 0:
        cur_product = None
    else:
        cur_no = listbox_products.curselection()
        cur_product = [listbox_products.get(i) for i in cur_no]
    if listbox_demandType.curselection() is None or len(listbox_demandType.curselection()) == 0:
        cur_FO = None
    else:
        cur_no = listbox_demandType.curselection()
        cur_FO = [listbox_demandType.get(i) for i in cur_no]
    # get filter subregion
    if SubRegion is None or len(SubRegion) == 0:
        all_SubRegion = list(df_name_forecast.SubRegion.unique())
        f_SubRegion = df_name_forecast.SubRegion.isin(all_SubRegion)
    else:
        f_SubRegion = df_name_forecast.SubRegion == SubRegion
    # select for product, terminal, demandType
    if cur_product is None or len(cur_product) == 0:
        all_product = list(df_name_forecast.ProductClass.unique())
        f_product = df_name_forecast.ProductClass.isin(all_product)
    else:
        f_product = df_name_forecast.ProductClass.isin(cur_product)
    if cur_terminal is None or len(cur_terminal) == 0:
        all_terminal = list(df_name_forecast.PrimaryTerminal.unique())
        f_terminal = df_name_forecast.PrimaryTerminal.isin(all_terminal)
    else:
        f_terminal = df_name_forecast.PrimaryTerminal.isin(cur_terminal)
    if cur_FO is None or len(cur_FO) == 0:
        all_demandType = list(df_name_forecast.DemandType.unique())
        f_FO = df_name_forecast.DemandType.isin(all_demandType)
    else:
        f_FO = df_name_forecast.DemandType.isin(cur_FO)
    # get selected customers
    # global custName_list
    custName_list = sorted(df_name_forecast[f_SubRegion & f_product & f_terminal & f_FO].index)
    # print('cust no: ', len(custName_list))
    for item in custName_list:
        listbox_customer.insert(tk.END, item)


def rank_product(x):
    '''给 product 排序'''
    if x == 'LIN':
        return (x, 1)
    elif x == 'LOX':
        return (x, 2)
    elif x == 'LAR':
        return (x, 3)
    elif x == 'CO2':
        return (x, 4)
    elif x == 'LUX':
        return (x, 5)
    elif x == 'LUN':
        return (x, 6)
    else:
        return (x, 7)


def show_list_terminal_product_FO(event):
    '''当点击 subregion 的时候显示 products & terminal & FO'''
    # global terminal_list, product_list, demandType_list
    # 1 terminal
    listbox_terminal.delete(0, tk.END)
    selected_subRegion = listbox_subRegion.get(tk.ANCHOR)
    terminal_list = sorted(list(df_name_forecast.loc[df_name_forecast.SubRegion ==
                                                     selected_subRegion, 'PrimaryTerminal'].unique()))
    # print(terminal_list)
    # if len(terminal_list) == 1:
    #     listbox_terminal.insert(tk.END, terminal_list[0])
    for item in terminal_list:
        listbox_terminal.insert(tk.END, item)
    # 2 products
    listbox_products.delete(0, tk.END)
    # product_list = sorted(list(df_name_forecast.loc[df_name_forecast.SubRegion ==
    #                                                 selected_subRegion, 'ProductClass'].unique()))
    # sort products by lin lox lar co2
    product_list = df_name_forecast.loc[df_name_forecast.SubRegion ==
                                        selected_subRegion, 'ProductClass'].unique()
    product_list = [rank_product(i) for i in product_list]
    product_list = [i[0] for i in sorted(product_list, key=lambda x: x[1])]
    # product_list = product_list
    for item in product_list:
        listbox_products.insert(tk.END, item)
    # 3 Demand type
    listbox_demandType.delete(0, tk.END)
    demandType_list = list(df_name_forecast.loc[df_name_forecast.SubRegion ==
                                                selected_subRegion, 'DemandType'].unique())
    # demandType_list = demandType_list
    for item in sorted(demandType_list):
        listbox_demandType.insert(tk.END, item)
    # 4 自动选择第一个
    listbox_terminal.select_set(0)
    listbox_products.select_set(0)
    listbox_demandType.select_set(0)
    # 显示 listbox_customer
    show_list_cust(event)


def cust_btn_search(root, conn):
    '''search for customer by shipto or name'''
    info = entry_name.get().strip()
    # print(info)
    if info.isdigit():
        info = int(info)
        names = df_name_all[df_name_all.LocNum == info].index
    else:
        names = df_name_all[df_name_all.index.str.contains(info)].index
    if len(names) == 0:
        messagebox.showinfo(parent=root, title='Warning', message='Check your search!')
    else:
        listbox_customer.delete(0, tk.END)
        for item in sorted(names):
            listbox_customer.insert(tk.END, item)


def get_forecast_customer_from_sqlite(conn):
    '''对这个函数进行一下说明：
       1. 原来这个函数只针对 forecastReading 里的客户，缺点是会遗漏 有 history reading 的客户；
       2. 现在 把有 history reading 的客户 也加上去，目的是：一个客户 即便没有 forecast reading 的值
          也能显示 history reading；'''
    sql = '''SELECT DISTINCT LocNum from forecastReading;'''
    forecast_shiptos = tuple(pd.read_sql(sql, conn).LocNum)
    sql = '''SELECT DISTINCT LocNum from historyReading;'''
    history_shiptos = tuple(pd.read_sql(sql, conn).LocNum)
    full_shiptos = tuple(set(forecast_shiptos + history_shiptos))
    # print(len(forecast_shiptos), len(history_shiptos), len(full_shiptos))
    sql = '''select LocNum, CustAcronym,
                    PrimaryTerminal, SubRegion,
                    ProductClass, DemandType, GalsPerInch,
                    UnitOfLength, Subscriber
            FROM odbc_master
            WHERE
            LocNum IN {};'''.format(full_shiptos)
    df_name_forecast = pd.read_sql(sql, conn)
    # print(df_name_forecast[df_name_forecast.CustAcronym.str.contains('潍坊')])
    df_name_forecast = df_name_forecast.set_index('CustAcronym')
    return df_name_forecast


def get_all_customer_from_sqlite(conn):
    '''get_all_customer_from_sqlite'''
    sql = '''select odbc_master.LocNum, odbc_master.CustAcronym,
                    odbc_master.PrimaryTerminal, odbc_master.SubRegion,
                    odbc_master.ProductClass, odbc_master.DemandType, odbc_master.GalsPerInch,
                    odbc_master.UnitOfLength
             FROM odbc_master
          '''
    df_name_all = pd.read_sql(sql, conn).drop_duplicates().set_index('CustAcronym')
    return df_name_all


def send_feedback(event, root, conn, lock):
    # email_worker = send_email()
    # flag = 'Success'
    # message_subject, message_body, addressee = email_worker.getEmailData(flag)
    # email_worker.outlook(addressee, message_subject, message_body)
    global save_pic
    save_pic = True
    pic_name = "./feedback.png"
    if os.path.isfile(pic_name):
        os.remove(pic_name)
    # event = None
    plot(event, root, conn, lock)
    print('testing')
    save_pic = False
    email_worker = send_email()
    result = combo_assess.get()
    reason = combo_reason.get()
    time.sleep(3)
    rounds = 0
    while not os.path.isfile(pic_name):
        time.sleep(2)
        rounds = rounds+1
        if rounds > 5:
            messagebox.showinfo(parent=root, title='Warning', message='No Data To Send!')
            return
    message_subject, message_body, addressee = email_worker.getEmailData(result, reason)
    email_worker.outlook(addressee, message_subject, message_body)
    messagebox.showinfo(parent=root, title='Success', message='Email been sent!')


def detail_info_label(framename):
    '''show detailed information about tank and forecast'''
    global lb2, lb4, lb6, lb8, lb10, lb12, lb14, lb16, lb17, lb18, lb20, lb21, lb22
    pad_y = 0
    lb1 = tk.Label(framename, text='CustName')
    lb1.grid(row=0, column=0, padx=6, pady=pad_y)
    lb2 = tk.Label(framename, text='')
    lb2.grid(row=0, column=1, padx=6, pady=pad_y)
    lb3 = tk.Label(framename, text='TargetTime')
    lb3.grid(row=2, column=0, padx=6, pady=pad_y)
    lb4 = tk.Label(framename, text='')
    lb4.grid(row=2, column=1, padx=6, pady=pad_y)
    lb5 = tk.Label(framename, text='RiskTime')
    lb5.grid(row=3, column=0, padx=6, pady=pad_y)
    lb6 = tk.Label(framename, text='')
    lb6.grid(row=3, column=1, padx=6, pady=pad_y)
    lb7 = tk.Label(framename, text='RunOutTime')
    lb7.grid(row=4, column=0, padx=6, pady=pad_y)
    lb8 = tk.Label(framename, text='')
    lb8.grid(row=4, column=1, padx=6, pady=pad_y)
    lb9 = tk.Label(framename, text='FullTrycock')
    lb9.grid(row=5, column=0, padx=6, pady=pad_y)
    lb10 = tk.Label(framename, text='')
    lb10.grid(row=5, column=1, padx=6, pady=pad_y)
    lb11 = tk.Label(framename, text='TargetRefill')
    lb11.grid(row=6, column=0, padx=6, pady=pad_y)
    lb12 = tk.Label(framename, text='')
    lb12.grid(row=6, column=1, padx=6, pady=pad_y)
    lb13 = tk.Label(framename, text='Risk')
    lb13.grid(row=7, column=0, padx=6, pady=pad_y)
    lb14 = tk.Label(framename, text='')
    lb14.grid(row=7, column=1, padx=6, pady=pad_y)
    lb15 = tk.Label(framename, text='Runout')
    lb15.grid(row=8, column=0, padx=6, pady=pad_y)
    lb16 = tk.Label(framename, text='')
    lb16.grid(row=8, column=1, padx=6, pady=pad_y)
    lb17 = tk.Label(framename, text='')
    lb17.grid(row=9, column=0, padx=6, pady=pad_y)
    lb18 = tk.Label(framename, text='')
    lb18.grid(row=9, column=1, padx=6, pady=pad_y)
    lb19 = tk.Label(framename, text='ForecastError')
    lb19.grid(row=10, column=0, padx=6, pady=pad_y)
    lb20 = tk.Label(framename, text='')
    lb20.grid(row=10, column=1, padx=6, pady=pad_y)
    lb21 = tk.Label(framename, text='__MaxPayload')
    lb21.grid(row=1, column=0, padx=6, pady=pad_y)
    lb22 = tk.Label(framename, text='')
    lb22.grid(row=1, column=1, padx=6, pady=pad_y)

def frame_warning_label(framename):
    global t4_t6_value_label

    # 添加一个标签作为示例
    t4_t6_label = tk.Label(framename, text="T6-T4 recent 3-time average (h): ")
    t4_t6_label.grid(row=0, column=0, padx=6, pady=0)

    t4_t6_value_label = tk.Label(framename, text="")
    t4_t6_value_label.grid(row=0, column=1, padx=6, pady=0)


def manual_input_label(framename, lock, cur, conn):
    '''for schedulers manually input their estimation about hourly usage'''
    pad_y = 0
    lb_cm = tk.Label(framename, text='CM Hourly')
    lb_cm.grid(row=0, column=0, padx=1, pady=pad_y)
    global box_cm, box_kg
    box_cm = tk.Entry(framename, width=10)
    box_cm.grid(row=0, column=1, padx=1, pady=pad_y)
    lb_kg = tk.Label(framename, text='KG Hourly')
    lb_kg.grid(row=1, column=0, padx=1, pady=pad_y)
    box_kg = tk.Entry(framename, width=10)
    box_kg.grid(row=1, column=1, padx=1, pady=pad_y)
    btn_calculate = tk.Button(framename, text='Calculate by Input', width=15,
                              command=lambda: calculate_by_manual(cur, conn, root, lock))
    btn_calculate.grid(row=2, column=0, pady=3, columnspan=2)
    btn_reset = tk.Button(framename, text='Reset', width=15,
                          command=lambda: reset_manual(root, conn, lock))
    btn_reset.grid(row=3, column=0, pady=3, columnspan=2)
    lb_assess = tk.Label(framename, text='Feedback: ')
    lb_assess.grid(row=4, column=0, padx=1, pady=pad_y)
    global combo_assess, combo_reason
    assess_options = ['', '预测准确', '预测误差小', '预测误差大']
    combo_assess = ttk.Combobox(framename, value=assess_options)
    # combo_assess.current(1)
    combo_assess.grid(row=4, column=1, padx=1, pady=pad_y)
    lb_reason = tk.Label(framename, text='Reason: ')
    lb_reason.grid(row=5, column=0, padx=1, pady=pad_y)
    reason_options = ['', '并联罐', '生产计划原因', '节日长假', '突发情况', '模型有改进空间']
    combo_reason = ttk.Combobox(framename, value=reason_options)
    combo_reason.grid(row=5, column=1, padx=1, pady=5)
    # event = None
    # btn_email = tk.Button(framename, text='Send Email', width=15,
    #                       command=lambda: send_feedback(root, conn))
    # btn_email = tk.Button(framename, text='Send Email', width=15,
    #                       command=Thread(target=send_feedback, args=(event, root, conn)).start())
    # global btn_email
    btn_email = tk.Button(framename, text='Send Email', width=15)
    btn_email.grid(row=6, column=0, pady=1, columnspan=2)
    btn_email.bind('<Button-1>', lambda event: threading.Thread(target=send_feedback,
                                                                args=(event, root, conn, lock)).start())
    lb_time1 = tk.Label(framename, text='Last Time: ')
    lb_time1.grid(row=7, column=0, padx=1, pady=pad_y)
    sql = 'select MAX(ReadingDate) from historyReading '
    lastTime = pd.read_sql(sql, conn).values.flatten()[0]
    lb_time2 = tk.Label(framename, text='{}'.format(lastTime))
    lb_time2.grid(row=7, column=1, padx=1, pady=pad_y)


def create_manual_forecast_data(conn, shipto, input_value):
    '''create_manual_forecast_data'''
    table_name = 'forecastBeforeTrip'
    sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
    df = pd.read_sql(sql, conn)
    if len(df) == 0:
        table_name = 'historyReading'
        sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
        df = pd.read_sql(sql, conn)
        if len(df) == 0:
            messagebox.showinfo(parent=root, title='Warning', message='No history Data To Show')
            return
    else:
        table_name = 'forecastReading'
        sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
        df = pd.read_sql(sql, conn)
        df = df[df.Forecasted_Reading.notna()].reset_index(drop=True)
        if len(df) == 0:
            messagebox.showinfo(parent=root, title='Warning', message='No forecast Data To Show')
            return
    # create new manual forecast data
    # print(df.head())
    if table_name == 'forecastReading':
        start_time = df.head(1).Next_hr.values[0]
        start_level = df.head(1).Forecasted_Reading.values[0]
    else:
        start_time = df.tail(1).ReadingDate.values[0]
        start_level = df.tail(1).Reading_Gals.values[0]
    level_temp = start_level
    new_level_list = [start_level]
    for i in range(72):
        level_temp = level_temp - input_value
        new_level_list.append(level_temp)
    new_time_list = pd.date_range(start=start_time, periods=73, freq='H')
    df1 = pd.DataFrame(data={'Next_hr': new_time_list, 'Forecasted_Reading': new_level_list})
    df1['LocNum'] = shipto
    df1['Hourly_Usage_Rate'] = input_value
    df1 = df1.loc[df1.Forecasted_Reading >= 0, :].reset_index(drop=True)
    return df1


def calculate_by_manual(cur, conn, root, lock):
    input_value1 = box_kg.get()
    input_value2 = box_cm.get()
    if len(input_value1) > 0 and len(input_value2) > 0:
        messagebox.showinfo(parent=root, title='Warning', message='Cannot KM+CM')
        return
    if len(input_value1) > 0:
        try:
            input_value = float(input_value1)
        except ValueError:
            messagebox.showinfo(parent=root, title='Warning', message='Input Wrong')
    else:
        try:
            galsperinch = df_info.GalsPerInch.values[0]
            input_value = float(input_value2)*galsperinch
        except ValueError:
            messagebox.showinfo(parent=root, title='Warning', message='Input Wrong')
            return
    if input_value < 0 or input_value > 50000:
        messagebox.showinfo(parent=root, title='Warning', message='Input Wrong')
        return
    # print(input_value1, input_value2, type(input_value1), type(input_value2))
    custName = listbox_customer.get(tk.ANCHOR)
    if custName not in df_name_forecast.index:
        messagebox.showinfo(parent=root, title='Warning', message='No Data To Show.')
        return
    else:
        shipto = int(df_name_forecast.loc[custName].values[0])
        validate_flag = time_validate_check(conn, shipto)
    # if not validate_flag[0]:
    #     messagebox.showinfo(parent=root, title='Warning', message=validate_flag[1])
    #     return
    # else:
        # fromTime = pd.to_datetime(from_box.get())
        # toTime = pd.to_datetime(to_box.get())
    df = create_manual_forecast_data(conn, shipto, input_value)
    table_name = 'manual_forecast'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    global manual_plot
    manual_plot = True
    event = None
    plot(event, root, conn, lock)
    manual_plot = False


def reset_manual(root, conn, lock):
    box_kg.delete(0, 'end')
    box_cm.delete(0, 'end')
    event = None
    plot(event, root, conn, lock)


def treeView_design(framename, width, height, row, column, y_scroll):
    '''增加 treeView'''
    # create frame
    # 固定大小,并且不让Frame根据内容自由变化大小
    # myFrame = tk.Frame(framename, width=335, height=120)
    myFrame = tk.Frame(framename, width=width, height=height)
    myFrame.pack_propagate(0)
    # myFrame.pack(pady=1)
    # myFrame.pack_propagate(0)
    # myFrame.grid(row=0, column=3, padx=10)
    myFrame.grid(row=row, column=column, padx=10, pady=5)
    # treeview scrollbar
    if y_scroll:
        tree_scroll_y = tk.Scrollbar(myFrame, orient=tk.VERTICAL)
        tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        # tree_scroll_x = tk.Scrollbar(myFrame, orient=tk.HORIZONTAL)
        # tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
    # create treeview
    # global myTree
        myTree = ttk.Treeview(myFrame, yscrollcommand=tree_scroll_y.set, selectmode='extended')
        # configure the scrollbar
        tree_scroll_y.config(command=myTree.yview)
    else:
        myTree = ttk.Treeview(myFrame, selectmode='extended')
    # Add Style
    style = ttk.Style()
    # pick a theme
    style.theme_use('default')
    style.configure('Treeview', rowheight=25)
    # tree_scroll_x.config(command=myTree.xview)
    # striped row tags
    myTree.tag_configure('oddRow', background='white')
    myTree.tag_configure('evenRow', background='lightblue')
    # 点击属性
    # myTree.bind("<Double-1>", lambda event: OnDoubleClick(event, root))
    return myTree


def clear_tree(treename):
    # 如只删除 treeview
    treename.delete(*treename.get_children())


def treeview_data(conn, shipto, treename, purpose):
    '''显示数据'''
    if purpose == 'reading':
        df = get_recent_reading(shipto, conn)
        clear_tree(treename)
    else:
        df = get_deliveryWindow(shipto, conn)
        clear_tree(treename)
    # print(df)
    # set up new tree view
    treename['column'] = list(df.columns)
    # print(df)
    # 设置 column 的 属性, 主要是列宽
    for col in df.columns:
        if 'No' in col:
            treename.column(col, anchor=tk.CENTER, width=35)
        elif 'ReadingDate' in col:
            treename.column(col, anchor=tk.CENTER, width=120)
        elif 'Trailer_' in col:
            treename.column(col, anchor=tk.CENTER, width=100)
        elif 'LeaveTime' in col:
            treename.column(col, anchor=tk.CENTER, width=120)
        elif 'CheckInfo' in col:
            treename.column(col, anchor=tk.CENTER, width=120)
        elif 'title' in col:
            treename.column(col, anchor=tk.CENTER, width=80)
        elif col in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
            treename.column(col, anchor=tk.CENTER, width=40)
        else:
            treename.column(col, anchor=tk.CENTER, width=65)

    treename['show'] = 'headings'
    # look thru column list for headers
    global count
    count = 0
    for column in treename['column']:
        treename.heading(column, text=column, anchor=tk.CENTER)
    df_rows = df.to_numpy().tolist()
    # 填入内容
    for row in df_rows:
        if count % 2 == 0:
            treename.insert(parent='', index='end', values=row, tags=('evenRow', ))
        else:
            treename.insert(parent='', index='end', values=row, tags=('oddRow', ))
        count += 1
    treename.pack()




def forecaster_run(root, path1, cur, conn):
    # 补丁
    global plot_flag
    plot_flag = True

    print('start check_refresh_deliveryWindow')
    check_refresh_deliveryWindow(cur=cur, conn=conn)
    print('finish check_refresh_deliveryWindow')

    file_dict = get_filename(path1, purpose='LB_CNS')
    print('start refresh sharefolder')
    refresh_history_data(cur, conn, file_dict)
    refresh_forecast_data(cur, conn, file_dict)
    refresh_forecastBeforeTrip_data(cur, conn, file_dict)
    refresh_fe(cur, conn)
    print('finish refresh sharefolder.')
    global df_name_forecast, df_name_all
    df_name_forecast = get_forecast_customer_from_sqlite(conn)
    df_name_all = get_all_customer_from_sqlite(conn)
    # 建立 作图区域
    plot_frame = tk.LabelFrame(root, text='Plot')
    # plot_frame.pack(fill='x', expand=True, padx=20, pady=1)
    plot_frame.pack(fill='x',  expand=True, padx=5, pady=1)
    plot_frame.rowconfigure(0, weight=1)
    plot_frame.columnconfigure(1, weight=1)
    f_frame = tk.LabelFrame(plot_frame, text='Filter')
    f_frame.grid(row=0, column=0, padx=5, pady=1)
    subRegion_boxlist(f_frame)
    terminal_boxlist(f_frame)
    products_boxlist(f_frame)
    demandType_boxlist(f_frame)
    # 重新排版,建立 frame_input
    frame_input = tk.LabelFrame(plot_frame, text='input')
    frame_input.grid(row=1, column=0, padx=10, pady=5)
    input_framework(frame_input, cur=cur, conn=conn, file_dict=file_dict)
    pic_frame = tk.LabelFrame(plot_frame)
    # pic_frame.grid(row=0, column=1, padx=5, pady=5)
    pic_frame.grid(row=0, column=1, rowspan=2, sticky=tk.E+tk.W+tk.N+tk.S)
    pic_frame.rowconfigure(0, weight=1)
    pic_frame.columnconfigure(0, weight=1)
    global fig, ax, ax_histy, canvas, toolbar, annot
    fig, ax, ax_histy, canvas, toolbar = get_plot_basic(pic_frame)

    annot = None

    lock = threading.Lock()
    canvas.mpl_connect("motion_notify_event", hover)

    # 最大的frame：par_frame
    par_frame = tk.LabelFrame(root)
    par_frame.pack(fill='x', expand=True, padx=20, pady=1)

    cust_frame = info_cust_frame(par_frame)
    customer_query(cust_frame)
    global btn_query
    btn_query = tk.Button(cust_frame, text='Search', command=lambda: cust_btn_search(root, conn))
    btn_query.grid(row=0, column=1, padx=2)
    customer_boxlist(cust_frame)
    global save_pic, manual_plot
    save_pic = False
    manual_plot = False
    global unitOfLength_dict
    unitOfLength_dict = {1: 'CM', 2: 'Inch', 3: 'M', 4: 'MM', 5: 'Percent', 6: 'Liters'}

    listbox_subRegion.bind("<<ListboxSelect>>", show_list_terminal_product_FO)
    listbox_terminal.bind("<<ListboxSelect>>", show_list_cust)
    listbox_products.bind("<<ListboxSelect>>", show_list_cust)
    listbox_demandType.bind("<<ListboxSelect>>", show_list_cust)

    listbox_customer.bind("<<ListboxSelect>>", lambda event: threading.Thread(
        target=plot, args=(event, root, conn, lock)).start())

    # 重新排版,建立 frame_detail
    frame_detail = tk.LabelFrame(par_frame, text='Detailed Info')
    frame_detail.grid(row=0, column=1, padx=10, pady=2)
    # 输入 起始日期
    detail_info_label(frame_detail)

    second_col_frame =  tk.LabelFrame(par_frame)
    second_col_frame.grid(row=0, column=2, padx=10, pady=2)

    frame_warning = tk.LabelFrame(second_col_frame, text='Warning')
    frame_warning.grid(row=0, column=0, padx=10, pady=2)

    frame_warning_label(frame_warning)

    # 重新排版,建立 frame_detail
    frame_manual = tk.LabelFrame(second_col_frame, text='Manual Input')
    frame_manual.grid(row=1, column=0, padx=10, pady=2)
    # 输入 起始日期
    manual_input_label(frame_manual, lock, cur, conn)
    # 新增两个 Treeview
    frame_tree = tk.LabelFrame(par_frame)
    frame_tree.grid(row=0, column=3, padx=5, pady=1)
    # 增加历史液位记录
    global reading_tree, deliveryWindow_tree
    reading_tree = treeView_design(framename=frame_tree, width=380,
                                   height=120, row=0, column=0, y_scroll=True)
    deliveryWindow_tree = treeView_design(framename=frame_tree, width=380,
                                          height=120, row=1, column=0, y_scroll=False)
    global log_file
    log_file = os.path.join(path1, 'LB_Forecasting\\log.txt')
    logConnection(log_file, 'opened')


def update_font():
    # font
    font_path = os.path.join('./', 'SimHei.ttf')
    try:
        from matplotlib.font_manager import fontManager
        fontManager.addfont(font_path)
        matplotlib.rc('font', family='SimHei')
    except Exception as e:
        print(e)

@decorator.record_time_decorator("拷贝数据库")
def copyfile(dbname: str, to_dir: str, from_dir: str):
    import shutil
    to_delivery_file = os.path.join(to_dir, dbname)
    from_file = os.path.join(from_dir, dbname)
    try:
        if os.path.isfile(from_file):
            shutil.copyfile(from_file, to_delivery_file)
        info = "DATABASE TRANSFER SUCCESS"
        print(info)
    except Exception as e:
        info = "DATABASE TRANSFER FAILURE"
        print(info, e)


if __name__ == '__main__':
    from src.forecast.daily_data_refresh import DataRefresh
    update_font()

    # 刷新 ODBC Master Data
    path1 = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling'

    db_name = 'AutoSchedule.sqlite'
    conn = connect_sqlite(db_name)
    cur = conn.cursor()

    daily_refresh = DataRefresh(local_cur=cur, local_conn=conn)
    daily_refresh.refresh_earliest_part_data()

    # 建立窗口
    root = tk.Tk()
    root.wm_title("Air Products Forecasting Viz")
    root.iconbitmap('./csl.ico')
    # print('screenwidth, screenheight', root.winfo_screenwidth(), root.winfo_screenheight())
    width, height = root.winfo_screenwidth(), root.winfo_screenheight()
    print('screenwidth, screenheight', width, height)
    # root.geometry('%dx%d+0+0' % (width, height))
    forecaster_run(root, path1, cur, conn)
    root.mainloop()
