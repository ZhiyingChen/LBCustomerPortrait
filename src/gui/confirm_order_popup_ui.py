import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from datetime import datetime, timedelta
import pandas as pd
from ..utils import enums
from .. import domain_object as do
from .lb_order_data_manager import LBOrderDataManager
from .order_popup_ui import OrderPopupUI
from ..utils import functions as func

class ConfirmOrderPopupUI:
    def __init__(
            self,
            root,
            order_data_manager: LBOrderDataManager,
            df_info: pd.DataFrame,
            show_time,
            loadAMT,
            order_popup_ui: OrderPopupUI
    ):
        super().__init__()
        self.root = root
        self.df_info = df_info
        self.order_data_manager = order_data_manager
        self.order_popup_ui = order_popup_ui
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
        try:
            from_time = pd.to_datetime(self.from_entry.get())
            to_time = pd.to_datetime(self.to_entry.get())
        except ValueError:
            messagebox.showwarning(
                parent=self.popup,
                title='时间格式错误提示',
                message='请输入正确的From时间格式，如2021-01-01 00:00'
            )
            return

        if from_time >= to_time:
            messagebox.showwarning(
                parent=self.popup,
                title='时间范围错误提示',
                message='From时间必须小于To时间'
            )
            return

        try:
            amt = float(self.amt_entry.get())
        except ValueError:
            messagebox.showwarning(
                parent=self.popup,
                title='可卸货量格式错误提示',
                message='请输入正确的可卸货量格式，如100.0'
            )
            return

        self.from_time = self.from_entry.get()
        self.to_time = self.to_entry.get()
        self.amt = self.amt_entry.get()
        self.note = self.note_entry.get("1.0", tk.END).strip()

        self.add_forecast_order()
        print('current orders: {}'.format(self.order_data_manager.forecast_order_dict.keys()))
        self.popup.destroy()

    def add_forecast_order(self):
        shipto = str(self.df_info.LocNum.values[0])
        if shipto in {v.shipto for v in self.order_data_manager.forecast_order_dict.values()}:
            confirm =messagebox.askyesno(
                parent=self.popup,
                title='订单已存在提示',
                message='该shipto已存在FO订单，是否创建一个新的？'
            )
            if not confirm:
                return
        # 生成一个订单
        forecast_order = do.Order(
            order_id=func.generate_new_forecast_order_id(),
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

        # 如果界面是打开的，在FO订单界面展示出来
        if self.order_popup_ui is None or (self.order_popup_ui is not None and self.order_popup_ui.closed):
            return

        self.order_popup_ui.add_order_display_in_working_tree(order=forecast_order)


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