from src.utils.log import setup_log
from src.utils import functions as func
from src.gui.forecast_ui import LBForecastUI
import tkinter as tk
import os

if __name__ == '__main__':

    setup_log("./output/")

    # 保持数据库连接
    db_name = 'AutoSchedule.sqlite'
    path1 = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling'
    to_dir = './'

    if not os.path.exists(os.path.join(to_dir, db_name)) or \
            not func.is_file_modified_today(os.path.join(to_dir, db_name)):
        # 数据库不存在或过期，拷贝数据库
        print('数据库不存在或过期，拷贝数据库')
        func.copyfile(dbname=db_name, from_dir=path1, to_dir='./')


    # 创建 UI 实例
    app = LBForecastUI(
        path1=path1
    )

    app.mainloop()
