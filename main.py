from src.utils.log import setup_log
from src.utils import functions as func
from src.gui.forecast_ui import LBForecastUI
import tkinter as tk
import os
import schedule
import tkinter
import threading
import time

# 定义一个函数来运行自动刷新任务
def run_auto_refresh(ui_instance):
    while True:
        schedule.run_pending()
        time.sleep(30)  # 等待1秒，然后检查是否有任务需要执行

if __name__ == '__main__':

    setup_log("./output/")

    # 保持数据库连接
    db_name = 'AutoSchedule.sqlite'
    path1 = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling'
    to_dir = './'

    if not os.path.exists(os.path.join(to_dir, db_name)) or \
            not func.is_file_modified_today(os.path.join(to_dir, db_name)):
        # 数据库不存在或过期，拷贝数据库
        print('数据库不存在或过期，正在拷贝数据库...')
        func.copyfile(dbname=db_name, from_dir=path1, to_dir='./')

    root = tk.Tk()
    width = int(root.winfo_screenwidth())
    height = int(root.winfo_screenheight() * 0.9)
    root.geometry(f"{width}x{height}")
    root.wm_title("Air Products Forecasting Viz")
    root.resizable(True, True)
    lb_forecast_ui = LBForecastUI(
        root=root,
        path1=path1
    )

    # 安排每小时30分钟运行自动刷新任务
    schedule.every().hour.at(":30").do(lb_forecast_ui.refresh_data, show_message=False)

    # 在单独的线程中运行自动刷新任务
    auto_refresh_thread = threading.Thread(target=run_auto_refresh, args=(lb_forecast_ui,))
    auto_refresh_thread.daemon = True  # 设置为守护线程，确保主线程结束时它也会结束
    auto_refresh_thread.start()
    root.mainloop()

