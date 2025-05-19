import tkinter as tk
from tkinter import ttk
from datetime import datetime, timedelta
import pandas as pd
from ..utils import enums
from .. import domain_object as do
from .lb_order_data_manager import LBOrderDataManager

class ConfirmOrderPopupUI:
    def __init__(
            self,
            root,
            order_data_manager: LBOrderDataManager,
            df_info: pd.DataFrame,
            show_time,
            loadAMT
    ):
        super().__init__()
        self.root = root
        self.df_info = df_info
        self.order_data_manager = order_data_manager
        # 默认时间
        dt = datetime.strptime(show_time, "%Y-%m-%d %H:%M")
        self.from_time = (dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
        self.to_time = (dt + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
        self.amt = loadAMT
        self.note = ""

        self.popup = tk.Toplevel(self.root)

        self.popup.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - (self.popup.winfo_width() // 2)
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - (self.popup.winfo_height() // 2)
        self.popup.geometry(f"+{x}+{y}")

        self.popup.title("编辑FO订单信息")
        self._setup_popup()


    def _submit(self):
        self.from_time = self.from_entry.get()
        self.to_time = self.to_entry.get()
        self.amt = self.amt_entry.get()
        self.note = self.note_entry.get("1.0", tk.END).strip()

        self.add_forecast_order()
        print('current orders: {}'.format(self.order_data_manager.forecast_order_dict.keys()))
        self.popup.destroy()

    def add_forecast_order(self):
        shipto = str(self.df_info.LocNum.values[0])
        if shipto in self.order_data_manager.forecast_order_dict:
            tk.messagebox.showwarning(
                '订单已存在提示', '{}的FO订单已存在中，请勿重复添加'.format(shipto)
            )
            return
        # 生成一个订单
        forecast_order = do.Order(
            shipto=shipto,
            cust_name=str(self.df_info.CustAcronym.values[0]),
            product=str(self.df_info.ProductClass.values[0]),
            from_time=pd.to_datetime(self.from_time),
            to_time=pd.to_datetime(self.to_time),
            drop_kg=self.amt,
            comments=self.note,
            order_type=enums.OrderType.FO
        )
        self.order_data_manager.add_forecast_order(forecast_order)

        #todo: 如果界面是打开的，在FO订单界面展示出来


    def _setup_popup(self):
        '''弹出可编辑的界面'''

        # From 时间
        ttk.Label(self.popup, text="From 时间:").grid(row=0, column=0, padx=5, pady=5)
        self.from_entry = ttk.Entry(self.popup)
        self.from_entry.insert(0, self.from_time)
        self.from_entry.grid(row=0, column=1, padx=5, pady=5)

        # To 时间
        ttk.Label(self.popup, text="To 时间:").grid(row=1, column=0, padx=5, pady=5)
        self.to_entry = ttk.Entry(self.popup)
        self.to_entry.insert(0, self.to_time)
        self.to_entry.grid(row=1, column=1, padx=5, pady=5)

        # 可卸货量
        ttk.Label(self.popup, text="可卸货量 (KG):").grid(row=2, column=0, padx=5, pady=5)
        self.amt_entry = ttk.Entry(self.popup)
        self.amt_entry.insert(0, str(self.amt))
        self.amt_entry.grid(row=2, column=1, padx=5, pady=5)

        # 备注
        ttk.Label(self.popup, text="备注:").grid(row=3, column=0, padx=5, pady=5)
        self.note_entry = tk.Text(self.popup, height=4, width=30)
        self.note_entry.grid(row=3, column=1, padx=5, pady=5)

        # 提交按钮
        self.submit_btn = ttk.Button(self.popup, text="提交：建立FO订单", command=self._submit)
        self.submit_btn.grid(row=4, column=0, columnspan=2, pady=10)