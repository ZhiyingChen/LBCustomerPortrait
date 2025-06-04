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

    root = tk.Tk()
    root.update()  # 确保窗口已经初始化

    # 获取屏幕分辨率
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    # 设置窗口大小为屏幕的 80%
    window_width = int(screen_width * 0.9)
    window_height = int((screen_height * 0.9) )
    root.geometry(f"{window_width}x{window_height}")

    root.wm_title("Air Products Forecasting Viz")
    root.resizable(True, True)

    # 创建 UI 实例
    lb_forecast_ui = LBForecastUI(
        root=root,
        path1=path1
    )
    root.mainloop()
