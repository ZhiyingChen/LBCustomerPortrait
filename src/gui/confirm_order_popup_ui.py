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
from .lb_data_manager import LBDataManager

class ConfirmOrderPopupUI:
    def __init__(
            self,
            root,
            order_data_manager: LBOrderDataManager,
            lb_data_manager: LBDataManager,
            df_info: pd.DataFrame,
            order_popup_ui: OrderPopupUI,
            note: str = "",
            show_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
            loadAMT = 0,
            target_date: pd.Timestamp = None,
            risk_date: pd.Timestamp = None,
            run_out_date: pd.Timestamp = None,

    ):
        super().__init__()
        self.closed = False
        self.root = root
        self.df_info = df_info
        self.order_data_manager = order_data_manager
        self.lb_data_manager = lb_data_manager

        self.order_popup_ui = order_popup_ui
        # 默认时间
        dt = datetime.strptime(show_time, "%Y-%m-%d %H:%M")
        self.from_time = (dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
        self.to_time = (dt + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
        self.amt = loadAMT
        self.note = note

        self.target_date = target_date
        self.risk_date = risk_date
        self.run_out_date = run_out_date

        self.popup = tk.Toplevel(self.root)
        self.popup.protocol("WM_DELETE_WINDOW", self._on_close)

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
            amt = int(self.amt_entry.get())
        except ValueError:
            messagebox.showwarning(
                parent=self.popup,
                title='可卸货量格式错误提示',
                message='请输入正确的可卸货量格式且是整数，如100'
            )
            return

        max_drop_kg = self.lb_data_manager.get_max_payload_value_by_ship2(ship2=str(self.df_info.LocNum.values[0]))
        if amt <= 0 or amt > max_drop_kg:
            messagebox.showwarning(
                parent=self.popup,
                title='可卸货量错误提示',
                message='请输入正确的可卸货量，必须大于0且小于或等于最大可卸货量{}'.format(max_drop_kg)
                )
            return

        self.from_time = self.from_entry.get()
        self.to_time = self.to_entry.get()
        self.amt = self.amt_entry.get()
        self.note = self.note_entry.get("1.0", tk.END).strip()

        self.add_forecast_order()
        print('current orders: {}'.format(self.order_data_manager.forecast_order_dict.keys()))
        self.closed = True
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
            cust_name='{}, {}'.format(self.df_info.CustAcronym.values[0], self.df_info.TankAcronym.values[0]),
            product=str(self.df_info.ProductClass.values[0]),
            corporate_idn=self.df_info.PrimaryTerminal.values[0],
            from_time=pd.to_datetime(self.from_time),
            to_time=pd.to_datetime(self.to_time),
            drop_kg=self.amt,
            comments=self.note,
            po_number='',
            order_type=enums.OrderType.FO,
            target_date=self.target_date,
            risk_date=self.risk_date,
            run_out_date=self.run_out_date,
        )
        self.order_data_manager.add_order(forecast_order)

        # 如果界面是打开的，在FO订单界面展示出来
        try:
            if self.order_popup_ui is not None:
                self.order_popup_ui.add_order_to_ui(order=forecast_order)

        except Exception as e:
            print(e)


    def _setup_popup(self):
        '''弹出可编辑的界面'''
        # 展示当前shipto
        shipto_label = ttk.Label(self.popup, text="当前shipto: ",
                                 font=("Arial", 12, "bold"), background="#f0f0f0")
        shipto_label.grid(row=0, column=0, padx=10, pady=10, sticky='we')

        # 展示当前shipto的值
        shipto_value_label = ttk.Label(self.popup, text="{} ({})".format(str(self.df_info.CustAcronym.values[0]),
                                                                         str(self.df_info.LocNum.values[0])),
                                       font=("Arial", 12, "bold"), background="yellow")
        shipto_value_label.grid(row=0, column=1, padx=10, pady=10, sticky='we')

        # From 时间
        ttk.Label(self.popup, text="From 时间:").grid(row=1, column=0, padx=5, pady=5)
        self.from_entry = ttk.Entry(self.popup)
        self.from_entry.insert(0, self.from_time)
        self.from_entry.grid(row=1, column=1, padx=5, pady=5)

        # To 时间
        ttk.Label(self.popup, text="To 时间:").grid(row=2, column=0, padx=5, pady=5)
        self.to_entry = ttk.Entry(self.popup)
        self.to_entry.insert(0, self.to_time)
        self.to_entry.grid(row=2, column=1, padx=5, pady=5)

        # 可卸货量
        ttk.Label(self.popup, text="可卸货量 (KG):").grid(row=3, column=0, padx=5, pady=5)
        self.amt_entry = ttk.Entry(self.popup)
        self.amt_entry.insert(0, str(self.amt))
        self.amt_entry.grid(row=3, column=1, padx=5, pady=5)

        # 备注
        ttk.Label(self.popup, text="备注:").grid(row=4, column=0, padx=5, pady=5)
        self.note_entry = tk.Text(self.popup, height=4, width=28)
        self.note_entry.insert(tk.END, self.note)
        self.note_entry.grid(row=4, column=1, padx=5, pady=5)

        # 提交按钮
        self.submit_btn = ttk.Button(self.popup, text="提交：建立FO订单", command=self._submit)
        self.submit_btn.grid(row=6, column=0, columnspan=2, pady=10)

    def _on_close(self):
        self.closed = True
        self.popup.destroy()