from . import decorator
import os
import datetime
import sqlite3
import numpy as np


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

def log_connection(filename: str, action: str):
    f = open(filename, "a")
    use_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    home = str(os.path.expanduser("~")).split('\\')
    if len(home) > 2:
        home_name = home[2]
    else:
        home_name = 'unknown person'
    f.write("{} -- {} -- {}.\n".format(use_time, home_name, action))
    f.close()

def connect_sqlite(db_name: str):
    '''连接 SQLITE'''
    conn = sqlite3.connect(db_name, check_same_thread=False)
    print('sqlite {} connected'.format(db_name))
    return conn

def weight_length_factor(uom):
    '''因为 LBSHELL 与 odbc 导出 单位转化原因，需要设置一个 factor 进行还原'''
    if uom == 'Inch':
        return 2.54
    elif uom == 'M':
        return 10
    elif uom == 'MM':
        return 1 / 10
    else:
        return 1

def define_xticks( num):
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
    lim = (int(num / binwidth) + 1) * binwidth
    xticks = np.arange(0, lim + binwidth, binwidth)
    return xticks

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

def is_file_modified_today(file_path):
    # 获取文件的最后修改时间
    file_mod_time = os.path.getmtime(file_path)
    # 将时间戳转换为日期
    file_mod_date = datetime.datetime.fromtimestamp(file_mod_time).date()
    # 获取当前日期
    today = datetime.date.today()
    # 比较日期
    return file_mod_date == today

def generate_new_forecast_order_id():
    '''
    生成新的订单ID
    '''
    now = datetime.datetime.now()
    return now.strftime('%y%m%d%H%M%S')

def get_user_name():
    home = str(os.path.expanduser("~")).split('\\')
    if len(home) > 2:
        home_name = home[2]
    else:
        home_name = 'unknown person'
    return home_name

def summarize_delivery_times(delivery_times):
    summary = []
    grouped_times = {}

    weekday_to_number = {
        "周一": 1, "周二": 2, "周三": 3, "周四": 4,
        "周五": 5, "周六": 6, "周日": 7
    }

    for day, time_windows in delivery_times.items():
        for time_window in time_windows:
            if time_window == ("00:00", "00:00"):
                grouped_times.setdefault("不收", []).append(day)
            else:
                grouped_times.setdefault(time_window, []).append(day)

    def merge_consecutive_days(days):
        if not days:
            return ""
        merged_days = []
        start_day = days[0]
        prev_day = days[0]
        for day in days[1:]:
            if weekday_to_number[day] == weekday_to_number[prev_day] + 1:
                prev_day = day
            else:
                if start_day == prev_day:
                    merged_days.append(start_day)
                else:
                    merged_days.append(f"{start_day}到{prev_day}")
                start_day = day
                prev_day = day
        if start_day == prev_day:
            merged_days.append(start_day)
        else:
            merged_days.append(f"{start_day}到{prev_day}")
        return "、".join(merged_days)

    for time_window, days in grouped_times.items():
        days_sorted = sorted(days, key=lambda x: weekday_to_number[x])
        if time_window == "不收":
            summary.append(f"{merge_consecutive_days(days_sorted)} 不收")
        else:
            summary.append(f"{merge_consecutive_days(days_sorted)} {time_window[0]}-{time_window[1]}")

    return "，".join(summary)

