from src.utils.log import setup_log
from src.utils import functions as func
from src.gui.forecast_ui import LBForecastUI
import tkinter as tk

if __name__ == '__main__':

    setup_log("./output/")

    # 保持数据库连接
    db_name = 'AutoSchedule.sqlite'
    path1 = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling'

    func.copyfile(dbname=db_name, from_dir=path1, to_dir='./')
    local_conn = func.connect_sqlite('./{}'.format(db_name))
    local_cur = local_conn.cursor()

    root = tk.Tk()
    root.geometry("1400x800")
    root.wm_title("Air Products Forecasting Viz")
    root.resizable(True, True)
    lb_forecast_ui = LBForecastUI(
        root=root,
        conn=local_conn,
        cur=local_cur,
        path1=path1
    )
    root.mainloop()
