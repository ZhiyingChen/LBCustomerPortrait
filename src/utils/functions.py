from . import decorator
import os
import datetime
import sqlite3


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