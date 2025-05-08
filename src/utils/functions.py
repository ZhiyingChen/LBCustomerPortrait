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
    print('sqlite connected')
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