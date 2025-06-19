from src.utils.Email_forecast import send_email
from datetime import datetime
from datetime import timedelta
import matplotlib.pylab as pylab
import tkinter as tk
import pandas as pd
import numpy as np
import os
from matplotlib.backends.backend_tkagg import (
    FigureCanvasTkAgg, NavigationToolbar2Tk)
from matplotlib.figure import Figure
import matplotlib.dates as mdates
from matplotlib.dates import DayLocator
from tkinter import messagebox
import matplotlib
import time
import threading
from typing import Dict
from .confirm_order_popup_ui import ConfirmOrderPopupUI
from . import ui_structure
from ..utils.dol_api import updateDOL
from ..utils.lct_api import updateLCT
from ..forecast_data_refresh.daily_data_refresh import ForecastDataRefresh
from .lb_data_manager import LBDataManager
from ..utils import functions as func
from ..utils.constant import unitOfLength_dict
from .order_popup_ui import OrderPopupUI
from .lb_order_data_manager import LBOrderDataManager
from .. import domain_object as do
from ..utils import enums

# 设置使用的字体（需要显示中文的时候使用）
font = {'family': 'SimHei'}
# 设置显示中文,与字体配合使用
matplotlib.rc('font', **font)
matplotlib.rcParams['axes.unicode_minus'] = False
params = {'legend.fontsize': 'x-large',
          'axes.labelsize': 'x-large',
          'axes.titlesize': 'x-large',
          'xtick.labelsize': 'x-large',
          'ytick.labelsize': 'x-large'}
pylab.rcParams.update(params)

class LBForecastUI:
    def __init__(
            self,
            root,
            path1: str
    ):

        self.root = root
        # lock
        self.lock = threading.Lock()
        self.annot = None
        # 提取数据的类
        self.data_manager = LBDataManager()
        self.order_data_manager = LBOrderDataManager()

        # 送货前五后十客户
        self.delivery_shipto_dict: Dict[str, do.TripShipto] = self.data_manager.generate_trip_shipto_dict()
        self.supplement_delivery_shipto_latest_called()

        self.df_name_forecast = self.data_manager.get_forecast_customer_from_sqlite()
        self.df_info = None
        self.ts_history = None
        self.ts_forecast = None
        self.ts_forecast_before_trip = None
        self.ts_manual = None

        # 日志记录
        self.log_file = os.path.join(path1, 'LB_Forecasting\\log.txt')
        func.log_connection(self.log_file, 'opened')

        self.order_popup_ui = None
        self.confirm_order_popup = None
        # setup ui
        self._setup_ui()

    def supplement_delivery_shipto_latest_called(self):
        results = self.order_data_manager.get_latest_call_log()

        for shipto, cust_name, timestamp in results:
            shipto_obj = self.delivery_shipto_dict.get(cust_name)
            if shipto_obj is not None:
                shipto_obj.latest_called = pd.to_datetime(timestamp)

    def _set_subregion_boxlist(self):
        '''subRegion boxlist'''

        self.listbox_subregion = tk.Listbox(self.filter_frame, height=3, width=10, exportselection=False)
        subregion_list = self.df_name_forecast.SubRegion.unique()
        for item in sorted(subregion_list):
            self.listbox_subregion.insert(tk.END, item)
        self.listbox_subregion.grid(row=0, column=0, padx=2, pady=1)


    def _set_terminal_boxlist(self):
        '''terminal boxlist'''
        self.terminal_frame = tk.LabelFrame(self.filter_frame)
        # scrollbar
        scroll_y = tk.Scrollbar(self.terminal_frame, orient=tk.VERTICAL)
        # 这里需要特别学习：exportselection=False
        # 保证了 两个 Listbox 点击一个时,不影响第二个。
        self.listbox_terminal = tk.Listbox(
            self.terminal_frame, selectmode="extended", height=7, width=8, yscrollcommand=scroll_y.set, exportselection=False)
        scroll_y.config(command=self.listbox_terminal.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.terminal_frame.grid(rowspan=2, row=0,column=2, padx=2, pady=1)
        self.listbox_terminal.pack()


    def _set_products_boxlist(self):
        '''products boxlist'''
        self.listbox_products = tk.Listbox(self.filter_frame, selectmode="extended",
                                      height=4, width=8, exportselection=False)
        self.listbox_products.grid(row=1, column=1, padx=2, pady=1)


    def _set_demand_type_boxlist(self):
        self.listbox_demand_type = tk.Listbox(self.filter_frame, selectmode="extended",
                                        height=2, width=8, exportselection=False)
        self.listbox_demand_type.grid(row=0, column=1, padx=2, pady=1)

    def _set_delivery_type_boxlist(self):
        self.listbox_delivery_type = tk.Listbox(self.filter_frame, selectmode=tk.SINGLE,
                                        height=3, width=10, exportselection=False)
        delivery_type_lt = ['已安排行程', '前五后十', '全量客户']
        for item in delivery_type_lt:
            self.listbox_delivery_type.insert(tk.END, item)

        # 设置默认选择为第一项
        self.listbox_delivery_type.select_set(1)  # 1 是 '送货前五后十' 的索引
        self.listbox_delivery_type.grid(row=1, column=0, padx=2, pady=1)

    def _set_customer_query(self):
        # 添加搜索框
        self.entry_name = tk.Entry(self.cust_frame, width=20, bg='white', fg='black', borderwidth=1)
        self.entry_name.grid(row=0, column=0)

        self.btn_query = tk.Button(self.cust_frame, text='搜索', command=lambda: self.cust_btn_search())
        self.btn_query.grid(row=0, column=1, padx=2)

        # 添加客户列表
        self.cust_name_selection_frame = tk.LabelFrame(self.cust_frame)
        self.cust_name_selection_frame.grid(row=2, column=0, padx=5, pady=5, columnspan=2)

        self._decorate_cust_name_selection_frame()


    def _decorate_cust_name_selection_frame(self):
        ''' customer boxlist'''
        # 新增滚动轴 scrollbar
        scroll_y = tk.Scrollbar(self.cust_name_selection_frame, orient=tk.VERTICAL)
        # 这里需要特别学习：exportselection=False
        # 保证了 两个 Listbox 点击一个时,不影响第二个。
        self.listbox_customer = tk.Listbox(
            self.cust_name_selection_frame, height=13, width=20, yscrollcommand=scroll_y.set, exportselection=False
            , selectmode=tk.SINGLE)
        scroll_y.config(command=self.listbox_customer.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        self.listbox_customer.pack()
        self.listbox_customer.bind("<<ListboxSelect>>", lambda event: threading.Thread(
            target=self.plot).start())


        self.listbox_customer.bind("<Button-3>", self.listbox_customer_right_click)

        # 创建一个菜单
        self.popup_menu = tk.Menu(self.listbox_customer, tearoff=0)
        self.popup_menu.add_command(label="复制选中的 客户简称 和 shipto", command=self.open_copy_window)

    def listbox_customer_right_click(self, event):
        # 显示右键菜单
        try:
            self.popup_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.popup_menu.grab_release()

    def open_copy_window(self):
        # 获取选中的客户名称
        selected_index = self.listbox_customer.curselection()
        if not selected_index:
            return

        custName = self.listbox_customer.get(selected_index[0])
        if not self.check_cust_name_valid(custName):
            return

        shipto = int(self.df_name_forecast.loc[custName].values[0])

        # 创建一个新的窗口
        copy_window = tk.Toplevel(self.root)
        copy_window.title("复制 选中的 客户简称 和 shipto")

        # 创建 Entry 小部件
        entry_cust_name = tk.Entry(copy_window, width=30, bg='white', fg='black', borderwidth=1)
        entry_cust_name.insert(0, custName)
        entry_cust_name.pack(padx=10, pady=5)
        entry_cust_name.bind("<Button-1>", lambda event: self.copy_text(entry_cust_name))

        entry_ship_to = tk.Entry(copy_window, width=30, bg='white', fg='black', borderwidth=1)
        entry_ship_to.insert(0, str(shipto))
        entry_ship_to.pack(padx=10, pady=5)
        entry_ship_to.bind("<Button-1>", lambda event: self.copy_text(entry_ship_to))

        # 添加一个关闭按钮
        close_button = tk.Button(copy_window, text="关闭", command=copy_window.destroy)
        close_button.pack(pady=10)

    def copy_text(self, entry):
        # 复制 Entry 小部件中的文本到剪贴板
        self.root.clipboard_clear()
        self.root.clipboard_append(entry.get())
        self.root.update()

    def show_list_cust(self, event):
        '''当点击 terminal 的时候显示客户名单'''
        df_name_forecast = self.df_name_forecast
        self.listbox_customer.delete(0, tk.END)
        if self.listbox_subregion.curselection() is None or len(self.listbox_subregion.curselection()) == 0:
            SubRegion = None
        else:
            SubRegion = self.listbox_subregion.get(self.listbox_subregion.curselection()[0])
        if self.listbox_terminal.curselection() is None or len(self.listbox_terminal.curselection()) == 0:
            cur_terminal = None
        else:
            cur_no = self.listbox_terminal.curselection()
            cur_terminal = [self.listbox_terminal.get(i) for i in cur_no]
            # print(cur_no, '->', cur_terminal)
        if self.listbox_products.curselection() is None or len(self.listbox_products.curselection()) == 0:
            cur_product = None
        else:
            cur_no = self.listbox_products.curselection()
            cur_product = [self.listbox_products.get(i) for i in cur_no]
        if self.listbox_demand_type.curselection() is None or len(self.listbox_demand_type.curselection()) == 0:
            cur_FO = None
        else:
            cur_no = self.listbox_demand_type.curselection()
            cur_FO = [self.listbox_demand_type.get(i) for i in cur_no]

        if self.listbox_delivery_type.curselection() is None or len(self.listbox_delivery_type.curselection()) == 0:
            delivery_type = None
        else:
            delivery_type = self.listbox_delivery_type.get(self.listbox_delivery_type.curselection()[0])


        # get filter subregion
        if SubRegion is None or len(SubRegion) == 0:
            all_SubRegion = list(df_name_forecast.SubRegion.unique())
            f_SubRegion = df_name_forecast.SubRegion.isin(all_SubRegion)
        else:
            f_SubRegion = df_name_forecast.SubRegion == SubRegion
        # select for product, terminal, demandType
        if cur_product is None or len(cur_product) == 0:
            all_product = list(df_name_forecast.ProductClass.unique())
            f_product = df_name_forecast.ProductClass.isin(all_product)
        else:
            f_product = df_name_forecast.ProductClass.isin(cur_product)
        if cur_terminal is None or len(cur_terminal) == 0:
            all_terminal = list(df_name_forecast.PrimaryTerminal.unique())
            f_terminal = df_name_forecast.PrimaryTerminal.isin(all_terminal)
        else:
            f_terminal = df_name_forecast.PrimaryTerminal.isin(cur_terminal)
        if cur_FO is None or len(cur_FO) == 0:
            all_demandType = list(df_name_forecast.DemandType.unique())
            f_FO = df_name_forecast.DemandType.isin(all_demandType)
        else:
            f_FO = df_name_forecast.DemandType.isin(cur_FO)



        # get selected customers
        custName_list = list(df_name_forecast[f_SubRegion & f_product & f_terminal & f_FO].index)

        # filter by delivery type
        if delivery_type == '已安排行程':
            custName_list = [
                i for i in custName_list
                if i in self.delivery_shipto_dict and
                   self.delivery_shipto_dict[i].is_trip_planned
            ]
        elif delivery_type == '前五后十':
            custName_list = [
                i for i in custName_list
                if i in self.delivery_shipto_dict
            ]

        ten_days_later = pd.Timestamp.now() + timedelta(days=10)

        trip_start_by_cust = {
            i: (self.delivery_shipto_dict[i].nearest_trip_start_time, self.delivery_shipto_dict[i].nearest_trip)
            if i in self.delivery_shipto_dict and self.delivery_shipto_dict[i].nearest_trip is not None else
            (ten_days_later, '')
            for i in custName_list
        }

        custName_list = sorted(
            custName_list
            , key=lambda x: trip_start_by_cust[x][0]
        )

        for item in custName_list:
            self.listbox_customer.insert(tk.END, item)
            if item in self.delivery_shipto_dict and self.delivery_shipto_dict[item].turn_red:
                self.listbox_customer.itemconfig(tk.END, {'fg': 'red'})


    def show_list_terminal_product_FO(self, event):
        '''当点击 subregion 的时候显示 products & terminal & FO'''
        # 1 terminal
        df_name_forecast = self.df_name_forecast

        self.listbox_terminal.delete(0, tk.END)
        selected_subRegion = self.listbox_subregion.get(tk.ANCHOR)
        terminal_list = sorted(list(df_name_forecast.loc[df_name_forecast.SubRegion ==
                                                         selected_subRegion, 'PrimaryTerminal'].unique()))
        for item in terminal_list:
            self.listbox_terminal.insert(tk.END, item)
        # 2 products
        self.listbox_products.delete(0, tk.END)
        product_list = df_name_forecast.loc[df_name_forecast.SubRegion ==
                                            selected_subRegion, 'ProductClass'].unique()
        product_list = [func.rank_product(i) for i in product_list]
        product_list = [i[0] for i in sorted(product_list, key=lambda x: x[1])]
        for item in product_list:
            self.listbox_products.insert(tk.END, item)
        # 3 Demand type
        self.listbox_demand_type.delete(0, tk.END)
        demandType_list = list(df_name_forecast.loc[df_name_forecast.SubRegion ==
                                                    selected_subRegion, 'DemandType'].unique())
        # demandType_list = demandType_list
        for item in sorted(demandType_list):
            self.listbox_demand_type.insert(tk.END, item)
        # 4 自动选择第一个
        self.listbox_terminal.select_set(0)
        self.listbox_products.select_set(0)
        self.listbox_demand_type.select_set(0)
        # 显示 self.listbox_customer
        self.show_list_cust(event)


    def cust_btn_search(self):
        '''search for customer by shipto or name'''
        info = self.entry_name.get().strip()

        df_name_all = self.data_manager.get_all_customer_from_sqlite()
        if info.isdigit():
            info = int(info)
            names = df_name_all[df_name_all.LocNum == info].index
        else:
            names = df_name_all[df_name_all.index.str.contains(info)].index
        if len(names) == 0:
            messagebox.showinfo( title='Warning', message='Check your search!')
        else:
            self.listbox_customer.delete(0, tk.END)
            for item in sorted(names):
                self.listbox_customer.insert(tk.END, item)

    def send_feedback(self, event):
        self.save_pic = True
        pic_name = "./feedback.png"
        if os.path.isfile(pic_name):
            os.remove(pic_name)
        # event = None
        self.plot()
        print('testing')
        self.save_pic = False
        email_worker = send_email()
        result = self.combo_assess.get()
        reason = self.combo_reason.get()
        time.sleep(3)
        rounds = 0
        while not os.path.isfile(pic_name):
            time.sleep(2)
            rounds = rounds + 1
            if rounds > 5:
                messagebox.showinfo( title='Warning', message='No Data To Send!')
                return
        message_subject, message_body, addressee = email_worker.getEmailData(result, reason)
        email_worker.outlook(addressee, message_subject, message_body)
        messagebox.showinfo( title='Success', message='Email been sent!')

    def _set_detail_info_label(self):
        '''show detailed information about tank and forecast'''
        self.detail_labels = {}

        pad_y = 0
        label_info = [
            # ("客户简称", "cust_name"),
            ("__ 最大装载量 (T)", "max_payload_label"),
            ("目标送货时间", "target_time"),
            ("最佳充装时间", "risk_time"),
            ("断气时间", "runout_time"),
            ("满液位", "full_trycock"),
            ("目标送货液位", "target_refill"),
            ("最佳充装液位", "risk"),
            ("断气液位", "runout"),
            ("最佳卸货量", "best_drop_size"),
            ("预测小时用量", "forecast_hourly_usage"),
            ("预测错误率", "forecast_error"),
        ]

        for i, (label_text, key) in enumerate(label_info):
            lb_label = tk.Label(self.frame_detail, text=label_text)
            lb_label.grid(row=i, column=0, padx=6, pady=pad_y)

            lb_value = tk.Label(self.frame_detail, text="")
            lb_value.grid(row=i, column=1, padx=6, pady=pad_y)

            if label_text in ["__ 最大装载量 (T)", "预测小时用量"]:
                self.detail_labels[label_text] = lb_label
            self.detail_labels[key] = lb_value
    
    def _set_manipulate_frame(self):
        # 重新排版,建立 frame_detail
        self.frame_manual = tk.LabelFrame(self.manipulate_frame, text='用量计算器')
        self.frame_manual.grid(row=0, column=0, padx=2, pady=2)
        # 输入 起始日期
        self._set_manual_input_label()



    def _set_frame_warning_label(self):
        # 添加一个标签作为示例
        self.t4_t6_label = tk.Label(self.frame_warning, text="T6-T4 近三次平均 (h) : ")
        self.t4_t6_label.grid(row=0, column=0, padx=6, pady=0)

        self.t4_t6_value_label = tk.Label(self.frame_warning, text="")
        self.t4_t6_value_label.grid(row=0, column=1, padx=6, pady=0)


    def _set_manual_input_label(self):
        '''for schedulers manually input their estimation about hourly usage'''

        pad_y = 0
        lb_cm = tk.Label(self.frame_manual, text='每小时 CM')
        lb_cm.grid(row=0, column=0, padx=1, pady=pad_y)
    
        self.box_cm = tk.Entry(self.frame_manual, width=10)
        self.box_cm.grid(row=0, column=1, padx=1, pady=pad_y)
        lb_ton = tk.Label(self.frame_manual, text='每小时 Ton')
        lb_ton.grid(row=1, column=0, padx=1, pady=pad_y)
        self.box_ton = tk.Entry(self.frame_manual, width=10)
        self.box_ton.grid(row=1, column=1, padx=1, pady=pad_y)
        btn_calculate = tk.Button(self.frame_manual, text='手工计算', width=10,
                                  command=self.calculate_by_manual)
        btn_calculate.grid(row=2, column=0, pady=3)
        btn_reset = tk.Button(self.frame_manual, text='重置', width=5,
                              command=self.reset_manual)
        btn_reset.grid(row=2, column=1, pady=3)



    def create_manual_forecast_data(self, shipto, input_value):
        '''create_manual_forecast_data'''
        table_name = 'forecastBeforeTrip'
        sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
        df = pd.read_sql(sql, self.data_manager.conn)
        if len(df) == 0:
            table_name = 'historyReading'
            sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
            df = pd.read_sql(sql, self.data_manager.conn)
            if len(df) == 0:
                messagebox.showinfo( title='Warning', message='No history Data To Show')
                return
        else:
            table_name = 'forecastReading'
            sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
            df = pd.read_sql(sql, self.data_manager.conn)
            df = df[df.Forecasted_Reading.notna()].reset_index(drop=True)
            if len(df) == 0:
                messagebox.showinfo( title='Warning', message='No forecast_data_refresh Data To Show')
                return
        # create new manual forecast_data_refresh data
        # print(df.head())
        if table_name == 'forecastReading':
            start_time = df.head(1).Next_hr.values[0]
            start_level = df.head(1).Forecasted_Reading.values[0]
        else:
            start_time = df.tail(1).ReadingDate.values[0]
            start_level = df.tail(1).Reading_Gals.values[0]
        level_temp = start_level
        new_level_list = [start_level]
        for i in range(72):
            level_temp = level_temp - input_value
            new_level_list.append(level_temp)
        new_time_list = pd.date_range(start=start_time, periods=73, freq='H')
        df1 = pd.DataFrame(data={'Next_hr': new_time_list, 'Forecasted_Reading': new_level_list})
        df1['LocNum'] = shipto
        df1['Hourly_Usage_Rate'] = input_value
        df1 = df1.loc[df1.Forecasted_Reading >= 0, :].reset_index(drop=True)
        return df1


    def calculate_by_manual(self):
        cur = self.data_manager.cur
        conn = self.data_manager.conn
        df_name_forecast = self.df_name_forecast

        input_value1 = self.box_ton.get()
        input_value2 = self.box_cm.get()
        if len(input_value1) > 0 and len(input_value2) > 0:
            messagebox.showinfo( title='Warning', message='Cannot KM+CM')
            return
        if len(input_value1) > 0:
            try:
                input_value = float(input_value1) * 1000
            except ValueError:
                messagebox.showinfo( title='Warning', message='Input Wrong')
        else:
            try:
                galsperinch = self.df_info.GalsPerInch.values[0]
                input_value = float(input_value2) * galsperinch
            except ValueError:
                messagebox.showinfo( title='Warning', message='Input Wrong')
                return
        if input_value < 0 or input_value > 50000:
            messagebox.showinfo( title='Warning', message='Input Wrong')
            return
        # print(input_value1, input_value2, type(input_value1), type(input_value2))
        custName = self.listbox_customer.get(tk.ANCHOR)
        if custName not in df_name_forecast.index:
            messagebox.showinfo( title='Warning', message='No Data To Show.')
            return
        else:
            shipto = int(df_name_forecast.loc[custName].values[0])
        df = self.create_manual_forecast_data(shipto, input_value)
        table_name = 'manual_forecast'
        cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        conn.commit()
        df.to_sql(table_name, con=conn, if_exists='replace', index=False)

        self.manual_plot = True
        self.plot()
        self.manual_plot = False


    def reset_manual(self):
        self.box_ton.delete(0, 'end')
        self.box_cm.delete(0, 'end')
        event = None
        self.plot()


    def _decorate_delivery_frame(self):

        self.delivery_record_frame = tk.LabelFrame(self.delivery_frame)
        self.delivery_record_frame.pack(fill='both', expand=True, padx=5, pady=2)
        # self._set_delivery_record_frame()

        # 下方 Frame：临近客户模块
        self.frame_near_customer = tk.LabelFrame(self.delivery_frame)
        self.frame_near_customer.pack(fill='both', expand=True, padx=5, pady=2)

        self._set_near_customer_label()

    def _set_delivery_record_frame(self):
        columns = ["送货时间", "卸货量(T)", "频率", "行程状态"]
        col_widths = [70, 35, 30, 50]

        self.delivery_record_table = ui_structure.SimpleTable(
            self.delivery_record_frame, columns=columns, col_widths=col_widths, height=5)
        self.delivery_record_table.frame.pack(fill="both", expand=True)

    def _set_reading_tree(self):
        columns = ["读取时间", "T", "CM", "CM/H"]
        col_widths = [70, 40, 40, 40]

        self.reading_tree_table = ui_structure.SimpleTable(
            self.reading_tree_frame, columns=columns, col_widths=col_widths, height=7)
        self.reading_tree_table.frame.pack(fill="both", expand=True)

    def _set_delivery_window_tree(self):
        columns = ["标题", "周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        col_widths = [70, 30, 30, 30, 30, 30, 30, 30]

        self.delivery_window_tree_table = ui_structure.SimpleTable(
            self.delivery_window_tree_frame, columns=columns, col_widths=col_widths, height=5)
        self.delivery_window_tree_table.frame.pack(fill="both", expand=True)

    def _decorate_portrait_frame(self):
        # 上方：特殊备注
        self.frame_comment = tk.LabelFrame(self.portrait_frame)
        self.frame_comment.pack(fill='both', expand=True, padx=5, pady=2)

        self._set_comment_frame()

        # 第二行 最新联络
        self.frame_contact = tk.LabelFrame(self.portrait_frame)
        self.frame_contact.pack(fill='both', expand=True, padx=5, pady=2)

        self._set_contact_frame()

        # 中间： 生产计划和收货窗口
        self.frame_production = tk.LabelFrame(self.portrait_frame)
        self.frame_production.pack(fill='both', expand=True, padx=5, pady=2)

        
        self._set_production_frame()


        # 下方 Frame：Terminal/Source DTD 模块
        self.frame_dtd = tk.LabelFrame(self.portrait_frame)
        self.frame_dtd.pack(fill='both', expand=True, padx=5, pady=2)

        self._set_dtd_label()
    
    def _set_production_frame(self):
        columns = ["P&W", "平时", "临时被限制"]
        col_widths = [70, 80, 30]
        data = [
            ["生产计划", "", ""],
            ["收货窗口", "", ""],
        ]
        col_stretch = [False, True, True]
        self.production_table = ui_structure.SimpleTable(self.frame_production, columns=columns, col_widths=col_widths, height=2, col_stretch=col_stretch)
        self.production_table.frame.pack(fill="both")
        self.production_table.insert_rows(data)

    def _set_comment_frame(self):
        columns = ["内容"]
        col_widths = [70, 100]
        data = [
            ["特殊备注", ""],
        ]
        col_stretch = [False, True]

        self.comment_table = ui_structure.NoHeaderTable(
            self.frame_comment, columns=columns, col_widths=col_widths, height=1, col_stretch=col_stretch,
            show_header=False
        )
        self.comment_table.frame.pack(fill="both")
        self.comment_table.insert_rows(data)

    def _set_contact_frame(self):
        columns = ["内容"]
        col_widths = [70, 100]
        data = [
            ["最新联络", ""],
        ]
        col_stretch = [False, True]

        self.contact_table = ui_structure.NoHeaderTable(
            self.frame_contact, columns=columns, col_widths=col_widths, height=1, col_stretch=col_stretch,
            show_header=False
        )
        self.contact_table.frame.pack(fill="both")
        self.contact_table.insert_rows(data)

    def _set_dtd_label(self):
        columns = ["DT", "KM", "时长(h)", "发车时间", "数据源"]
        col_widths = [15, 15, 15, 40, 20]

        self.dtd_table = ui_structure.SimpleTable(self.frame_dtd, columns=columns, col_widths=col_widths, height=3)
        self.dtd_table.frame.pack(fill="both")

    def _set_near_customer_label(self):
        columns = ["临近客户", "KM", "DDER"]
        col_widths = [70, 15, 20]

        self.near_customer_table = ui_structure.SimpleTable(self.frame_near_customer, columns=columns,
                                                            col_widths=col_widths,
                                                            height=3)
        self.near_customer_table.frame.pack(fill="both", expand=True)

    def _set_refresh_frame(self):
        refresh_time_text = self.data_manager.get_last_refresh_time()
        self.refresh_time_label = tk.Label(
            self.refresh_frame, text='最新液位时间:\n{}'.format(refresh_time_text), anchor='w',
            fg='#009A49', font=("Arial", 11)
        )
        self.refresh_time_label.pack(side=tk.RIGHT, padx=2, pady=2, expand=True)

        self.btn_refresh = tk.Button(self.refresh_frame, text='刷新液位数据',
                                     command=self.refresh_data)
        self.btn_refresh.pack(side=tk.LEFT, padx=2, pady=2, expand=True)

    def _decorate_filter_frame(self):
        # 配置 f_frame 的行和列权重
        self.f_frame.grid_rowconfigure(0, weight=0)
        self.f_frame.grid_rowconfigure(1, weight=0)
        self.f_frame.grid_columnconfigure(0, weight=1)
        # 设置刷新按钮
        # 创建一个单独的Frame来放置刷新按钮
        self.refresh_frame = tk.Frame(self.f_frame)
        self.refresh_frame.grid(row=0, column=0, padx=0, pady=10)
        self._set_refresh_frame()

        # 设置筛选区域
        self.filter_frame = tk.LabelFrame(self.f_frame, text='筛选')
        self.filter_frame.grid(row=1, column=0, padx=0, pady=1)

        self._set_subregion_boxlist()
        self._set_delivery_type_boxlist()

        self._set_demand_type_boxlist()
        self._set_products_boxlist()

        self._set_terminal_boxlist()

        self.listbox_subregion.bind("<<ListboxSelect>>", self.show_list_terminal_product_FO)
        self.listbox_terminal.bind("<<ListboxSelect>>", self.show_list_cust)
        self.listbox_products.bind("<<ListboxSelect>>", self.show_list_cust)
        self.listbox_demand_type.bind("<<ListboxSelect>>", self.show_list_cust)
        self.listbox_delivery_type.bind("<<ListboxSelect>>", self.show_list_cust)

    def _open_order_window(self):
        if self.order_popup_ui is not None and not self.order_popup_ui.closed:
            # 已经打开了，不再打开
            messagebox.showinfo( title='提示', message='已经打开了订单界面，请勿重复打开。')
            return
        self.order_popup_ui = OrderPopupUI(
            root=self.root,
            order_data_manager=self.order_data_manager,
            data_manager=self.data_manager,
        )

    def _decorate_plot_frame(self):

        # plot_frame column 0, row 0: 筛选区域
        self.plot_frame.columnconfigure(0, weight=1)
        self.f_frame = tk.LabelFrame(self.plot_frame)
        self.f_frame.grid(row=0, column=0, padx=2, pady=1, sticky="nsew")
        self._decorate_filter_frame()

        # plot_frame column 1, row 0：作图区域
        self.plot_frame.columnconfigure(1, weight=8)

        self.pic_frame = tk.LabelFrame(self.plot_frame)
        self.pic_frame.grid(row=0, column=1, rowspan=3, sticky=tk.E + tk.W + tk.N + tk.S)
        self.pic_frame.rowconfigure(0, weight=1)
        self.pic_frame.columnconfigure(0, weight=2)
        self._set_pic_frame()

        self.annot = None
        self.save_pic = False
        self.manual_plot = False

        # plot_frame column 2, row 0: 建立 手工操作区域
        self.plot_frame.columnconfigure(2, weight=2)
        self.additional_info_frame = tk.LabelFrame(self.plot_frame)
        self.additional_info_frame.grid(row=0, column=2, padx=2, pady=2)
        self._set_additional_info_frame()

    def _set_additional_info_frame(self):
        self.manipulate_frame = tk.Frame(self.additional_info_frame)
        self.manipulate_frame.grid(row=0, column=0, padx=2, pady=2)
        self._set_manipulate_frame()

        # additional_info_frame row 1: 新增按钮区域
        self.button_order_frame = tk.Frame(self.additional_info_frame)
        self.button_order_frame.grid(row=1, column=0, pady=5, sticky="ew")

        self.btn_open_order_window = tk.Button(
            self.button_order_frame, text="打开FO订单界面", command=self._open_order_window,
            bg='#ADD8E6', fg="black", relief="raised", font=("Arial", 10)
        )
        self.btn_open_order_window.pack(padx=10, pady=5)

        #  additional_info_frame row 2: 新增历史记录区域

        self.reading_tree_frame = tk.Frame(self.additional_info_frame)
        self.reading_tree_frame.grid(row=2, column=0, pady=5, sticky="ew")
        self._set_reading_tree()


    def _decorate_par_frame(self):
        # par_frame column 0, row 0: 客户筛选区域
        self.par_frame.columnconfigure(0, weight=1)

        self.cust_frame = tk.LabelFrame(self.par_frame)
        self.cust_frame.grid(row=0, column=0, padx=5, pady=5)
        self._set_customer_query()

        # par_frame column 1, row 0: 建立 frame_detail
        self.par_frame.columnconfigure(1, weight=1)
        self.level_frame = tk.LabelFrame(self.par_frame)
        self.level_frame.grid(row=0, column=1, padx=10, pady=2)


        self.frame_warning = tk.LabelFrame(self.level_frame)
        self.frame_warning.grid(row=0, column=0, padx=10, pady=2)
        self._set_frame_warning_label()

        self.frame_detail = tk.LabelFrame(self.level_frame)
        self.frame_detail.grid(row=1, column=0, padx=10, pady=2)
        self._set_detail_info_label()

        # par_frame column 2, row 0：: 新增 DTD and Cluster 的 Frame
        self.par_frame.columnconfigure(2, weight=2)
        self.portrait_frame = tk.Frame(self.par_frame, width=200)
        self.portrait_frame.grid(row=0, column=2, padx=10, pady=2, sticky="nsew")
        self.portrait_frame.pack_propagate(False)
        self._decorate_portrait_frame()


        # par_frame column 3, row 0: 两个 Treeview 历史液位记录和 临近客户
        self.par_frame.columnconfigure(3, weight=2)
        self.delivery_frame = tk.Frame(self.par_frame, width=150)
        self.delivery_frame.grid(row=0, column=3, padx=10, pady=2, sticky="nsew")
        self.delivery_frame.pack_propagate(False)
        self._decorate_delivery_frame()



    def _setup_ui(self):
        # 建立上半区：作图区域 plot frame
        self.plot_frame = tk.LabelFrame(self.root, text='Plot')
        self.plot_frame.pack(fill='x', expand=True, padx=2, pady=1)
        self._decorate_plot_frame()


        # 建立下半区：信息区域：par_frame
        self.par_frame = tk.LabelFrame(self.root)
        self.par_frame.pack(fill='x', expand=True, padx=5, pady=1)
        self._decorate_par_frame()


    # region 刷新相关函数
    def update_production_table(self, shipto_id: str):
        ordinary_delivery_text, restricted_delivery_text = self.data_manager.get_delivery_window_by_shipto(shipto_id)
        data = [
            ['生产计划', '', ''],
            ['收货窗口', ordinary_delivery_text, restricted_delivery_text]
        ]
        self.production_table.insert_rows(data)

    def update_contact_table(self, shipto_id: str):
        call_log_text = self.data_manager.get_call_log_by_shipto(shipto_id)
        data = [
            ["最新联络", call_log_text],
        ]
        self.contact_table.insert_rows(data)

    def update_dtd_table(self, shipto_id: str, risk_time: pd.Timestamp):
        results = self.data_manager.get_primary_terminal_dtd_info(shipto_id)

        # 添加 Primary DTD 信息
        primary_info = []
        for row in results:
            primary_dt, distance, duration, data_source = row
            primary_info.append('T{}'.format(primary_dt))
            try:
                distance = int(float(distance))
            except ValueError:
                distance = '?'
            primary_info.append(distance)
            try:
                duration = round(float(duration), 1)
            except ValueError:
                duration = '?'
            primary_info.append(duration)

            departure_time = ''
            try:
                departure_time = risk_time - pd.Timedelta(minutes=int(float(duration) * 60))
                departure_time = departure_time.strftime('%m-%d %H')
            except Exception as e:
                # pass
                pass

            primary_info.append(departure_time)
            primary_info.append(data_source)

        results = self.data_manager.get_sourcing_terminal_dtd_info(shipto_id)
        # 添加 Source DTD 信息
        source_list = []
        for row in results:
            source_info = list()
            source_dt, distance, duration, data_source = row
            source_info.append('S{}'.format(source_dt))
            try:
                distance = int(float(distance))
            except ValueError:
                distance = '?'
            source_info.append(distance)
            try:
                duration = round(float(duration), 1)
            except ValueError:
                duration = '?'
            source_info.append(duration)

            departure_time = ''
            try:
                departure_time = risk_time - pd.Timedelta(minutes=int(float(duration) * 60))
                departure_time = departure_time.strftime('%m-%d %H')
            except Exception as e:
                # pass
                pass
            source_info.append(departure_time)
            source_info.append(data_source)
            source_list.append(source_info)


        rows = [primary_info] + source_list
        self.dtd_table.insert_rows(rows)


    def update_near_customer_table(self, shipto_id: str):
        results = self.data_manager.get_near_customer_info(shipto_id)
        update_rows = list()
        for row in results:
            update_row = list()
            to_loc_num, to_cust_acronym, distance_km, dder, data_source = row

            if to_cust_acronym is None or len(to_cust_acronym.strip()) == 0:
                to_cust_acronym = to_loc_num

            try:
                dder = int(float(dder) * 100)
            except Exception as e:
                dder = '?'

            try:
                distance_km = int(float(distance_km))
            except Exception as e:
                distance_km = '?'

            update_row.append(to_cust_acronym)
            update_row.append(distance_km)
            update_row.append('{}%'.format(dder))

            update_rows.append(update_row)

        self.near_customer_table.insert_rows(update_rows)


    def update_reading_tree_table(self, shipto_id: str):
        historical_reading_df = self.data_manager.get_recent_reading(shipto_id)
        rows = historical_reading_df.to_numpy().tolist()
        self.reading_tree_table.insert_rows(rows)

    def update_delivery_window_tree_table(self, shipto_id: str):
        delivery_window_df = self.data_manager.get_delivery_window(shipto_id)
        rows = delivery_window_df.to_numpy().tolist()
        self.delivery_window_tree_table.insert_rows(rows)

    def clean_detailed_info(self):
        for key, label in self.detail_labels.items():
            label.config(text='')

    def show_info(self, shipto, TR_time, Risk_time, RO_time, full, TR,
                  Risk, RO, ts_forecast_usage, galsperinch, uom):
        '''显示客户的充装的详细信息'''
        self.clean_detailed_info()

        factor = func.weight_length_factor(uom)

        full_cm = int(full / galsperinch / factor)
        self.detail_labels['full_trycock'].config(text=f'{round(full / 1000, 1)} T / {full_cm} {uom}')
        TR_cm = int(TR / galsperinch / factor)
        self.detail_labels['target_refill'].config(text=f'{round(TR / 1000, 1)} T / {TR_cm} {uom}')
        RO_cm = int(RO / galsperinch / factor)
        self.detail_labels['runout'].config(text=f'{ round(RO / 1000, 1)} T / {RO_cm} {uom}')

        Risk_cm = int(Risk / galsperinch / factor)
        self.detail_labels['risk'].config(text=f'{round(Risk / 1000, 1)} T / {Risk_cm} {uom}')
        self.detail_labels['best_drop_size'].config(text=f'{round( (full - Risk) / 1000, 1)} T / {round(full_cm - Risk_cm)} {uom}')


        if Risk_time is not None:
            tr = TR_time.strftime("%m-%d %H:%M")
            risk = Risk_time.strftime("%m-%d %H:%M")
            ro = RO_time.strftime("%m-%d %H:%M")
            self.detail_labels['target_time'].config(text=tr)
            self.detail_labels['risk_time'].config(text=risk)
            self.detail_labels['runout_time'].config(text=ro)


        if len(ts_forecast_usage) >= 2:
            s_time = ts_forecast_usage.index[0].strftime("%m-%d %H")
            e_time = ts_forecast_usage.index[min(7, len(ts_forecast_usage) - 1)].strftime("%m-%d %H")
            hourly_usage = round(ts_forecast_usage[:8].mean().values[0] / 1000, 1)
            hourly_usage_cm = round(hourly_usage / (galsperinch * factor), 1)
            self.detail_labels['预测小时用量'].config(
                text=f'{s_time}~{e_time}\n 预测小时用量'
            )
            self.detail_labels['forecast_hourly_usage'].config(
                text=f'{hourly_usage} T / {hourly_usage_cm} {uom}'
            )


        fe = self.data_manager.get_forecast_error(shipto)
        self.detail_labels['forecast_error'].config(text=fe)

        current_primary_dt, current_max_payload = self.get_primary_dt_and_max_payload(shipto)
        current_max_payload = round(current_max_payload / 1000, 1) if isinstance(current_max_payload, float) else current_max_payload
        self.detail_labels['__ 最大装载量 (T)'].config(text=f'{current_primary_dt} 最大装载量 (T)')
        self.detail_labels['max_payload_label'].config(text=f'{current_max_payload}')

        t4_t6_value = self.data_manager.get_t4_t6_value(shipto=shipto)
        self.t4_t6_value_label.config(text=t4_t6_value)

        self.update_production_table(shipto_id=str(shipto))
        self.update_contact_table(shipto_id=str(shipto))
        # 显示历史液位
        self.update_reading_tree_table(shipto_id=str(shipto))

        self.update_dtd_table(shipto_id=str(shipto), risk_time=Risk_time)
        self.update_near_customer_table(shipto_id=str(shipto))

    def time_validate_check(self, shipto):
        ''''检查box的内容是否正确'''
        validate_flag = (True, True)
        try:
            fromTime = pd.to_datetime(self.from_box.get())
        except ValueError:
            validate_flag = (False, 'From Time Wrong!')
            return validate_flag
        try:
            toTime = pd.to_datetime(self.to_box.get())
        except ValueError:
            validate_flag = (False, 'To Time Wrong!')
            return validate_flag
        df = self.data_manager.get_forecast_reading(shipto, fromTime, toTime)
        # 为了防止 df 是空的：
        if len(df) == 0:
            # 这表明没有预测数据， 但是也要显示历史数据
            return (True, True)
        checkValue = df.Forecasted_Reading.values[0]
        if checkValue == 777777:
            validate_flag = (False, '此shipto无法抓取读数数据!')
        elif checkValue == 888888:
            validate_flag = (False, '读数少于一个月,不足以提供预测!')
        elif checkValue == 999999:
            validate_flag = (False, '近2日的读数缺失!')
        # print(checkValue, validate_flag)
        return validate_flag

    def plot_vertical_lines(self, fromTime, toTime, TR_time, Risk_time, RO_time, full):
        '''以下画垂直线, 一共有 10 种情况'''
        alpha = 0.4
        if fromTime < toTime < TR_time < Risk_time < RO_time:
            # 其实这表明查询的是历史记录
            pass
        if fromTime <= TR_time <= toTime <= Risk_time <= RO_time:
            self.forecast_plot_ax.axvline(x=TR_time, color='green', linewidth=1)
            self.forecast_plot_ax.fill_between(x=[TR_time, toTime], y1=full, facecolor='green', alpha=alpha)
        if fromTime <= TR_time <= Risk_time <= toTime <= RO_time:
            self.forecast_plot_ax.axvline(x=TR_time, color='green', linewidth=1)
            self.forecast_plot_ax.axvline(x=Risk_time, color='yellow', linewidth=1)
            self.forecast_plot_ax.fill_between(x=[TR_time, Risk_time], y1=full, facecolor='green', alpha=alpha)
            self.forecast_plot_ax.fill_between(x=[Risk_time, toTime], y1=full, facecolor='red', alpha=alpha)
        if fromTime <= TR_time <= Risk_time <= RO_time <= toTime:
            # 这个是最完整形态
            self.forecast_plot_ax.axvline(x=TR_time, color='green', linewidth=1)
            self.forecast_plot_ax.axvline(x=Risk_time, color='yellow', linewidth=1, )
            self.forecast_plot_ax.axvline(x=RO_time, color='red', linewidth=1)
            self.forecast_plot_ax.fill_between(x=[TR_time, Risk_time], y1=full, facecolor='green', alpha=alpha)
            self.forecast_plot_ax.fill_between(x=[Risk_time, RO_time], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= fromTime <= toTime <= Risk_time <= RO_time:
            self.forecast_plot_ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='green', alpha=alpha)
        if TR_time <= fromTime <= Risk_time <= toTime <= RO_time:
            self.forecast_plot_ax.axvline(x=Risk_time, color='yellow', linewidth=1)
            self.forecast_plot_ax.fill_between(x=[fromTime, Risk_time], y1=full, facecolor='green', alpha=alpha)
            self.forecast_plot_ax.fill_between(x=[Risk_time, toTime], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= fromTime <= Risk_time <= RO_time <= toTime:
            self.forecast_plot_ax.axvline(x=Risk_time, color='green', linewidth=1)
            self.forecast_plot_ax.axvline(x=RO_time, color='red', linewidth=1)
            self.forecast_plot_ax.fill_between(x=[fromTime, Risk_time], y1=full, facecolor='green', alpha=alpha)
            self.forecast_plot_ax.fill_between(x=[Risk_time, RO_time], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= Risk_time <= fromTime <= toTime <= RO_time:
            self.forecast_plot_ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= Risk_time <= fromTime <= RO_time <= toTime:
            self.forecast_plot_ax.axvline(x=RO_time, color='red', linewidth=1)
            self.forecast_plot_ax.fill_between(x=[fromTime, RO_time], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= Risk_time <= RO_time <= fromTime <= toTime:
            self.forecast_plot_ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='red', alpha=alpha)

    def _set_pic_frame(self):
        '''获取作图框架'''
        framename = self.pic_frame

        button_frame = tk.Frame(framename)
        button_frame.pack(side=tk.TOP, fill=tk.X)

        self.lb_fromtime = tk.Label(button_frame, text='开始时间')
        self.lb_fromtime.grid(row=0, column=0, padx=10, pady=5)
        self.from_box = tk.Entry(button_frame)
        # 初始化 起始日期
        start_day = (datetime.now().date() - timedelta(days=2)).strftime("%Y-%m-%d")
        self.from_box.insert(0, start_day)
        self.from_box.grid(row=0, column=1, padx=10, pady=5)
        # 输入 结束日期
        self.lb_totime = tk.Label(button_frame, text='结束时间')
        self.lb_totime.grid(row=0, column=2, padx=10, pady=5)
        self.to_box = tk.Entry(button_frame)
        # 初始化 结束日期
        end_day = (datetime.now().date() + timedelta(days=3)).strftime("%Y-%m-%d")

        self.to_box.insert(0, end_day)
        self.to_box.grid(row=0, column=3, padx=10, pady=5)

        # 设置是否需要 从DOL API 下载数据
        self.var_telemetry_flag = tk.IntVar()
        self.check_telemetry_flag = tk.Checkbutton(button_frame, text='远控 最新 （无液位时勾选）', variable=self.var_telemetry_flag, onvalue=1, offvalue=0)
        self.check_telemetry_flag.grid(row=0, column=4, padx=1, pady=10)

        self.pic_figure = Figure(figsize=(5, 4), dpi=80)
        gs = self.pic_figure.add_gridspec(1, 2, width_ratios=(6, 1),
                              left=0.08, right=0.96, bottom=0.1, top=0.9,
                              wspace=0.1, hspace=0.05)
        self.forecast_plot_ax = self.pic_figure.add_subplot(gs[0, 0])
        self.forecast_plot_ax_histy = self.pic_figure.add_subplot(gs[0, 1], sharey=self.forecast_plot_ax)
      
        self.canvas = FigureCanvasTkAgg(self.pic_figure, master=framename)  # A tk.DrawingArea.
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
       
        self.canvas.mpl_connect("motion_notify_event", self.hover)
        # 连接鼠标按钮事件
        self.canvas.mpl_connect("button_press_event", self.on_click)
        self.toolbar = NavigationToolbar2Tk(self.canvas, framename)


    def hover(self, event):
        '''悬浮'''

        if self.annot is None:
            return
        vis = self.annot.get_visible()
        for curve in self.forecast_plot_ax.get_lines():
            # Searching which data member corresponds to current mouse position
            if curve.contains(event)[0]:
                graph_id = curve.get_gid()
                graph_dict = {'point_history': self.ts_history,
                              'point_forecast': self.ts_forecast,
                              'point_forecastBeforeTrip': self.ts_forecast_before_trip,
                              'point_manual': self.ts_manual}
                if graph_id in graph_dict.keys():
                    if vis:
                        # 说明已经有了一个 annot, 就不再显示第二个了。
                        return
                    df_data = graph_dict[graph_id]
                    full = self.df_info.FullTrycockGals.values[0]
                    ind = curve.contains(event)[1]['ind'][0]
                    # pos = (event.x, event.y)
                    pos = (event.xdata, event.ydata)
                    show_time = df_data.index[ind].strftime("%Y-%m-%d %H:%M")
                    show_level = int(df_data.values.flatten()[ind])
                    # 转成长度单位
                    galsperinch = self.df_info.GalsPerInch.values[0]
                    unitOfLength = self.df_info.UnitOfLength.values[0]
                    uom = unitOfLength_dict[unitOfLength]
                    factor = func.weight_length_factor(uom)
                    show_level_cm = int(round(show_level / (galsperinch * factor), 1))
                    # 可卸货量
                    loadAMT = int(full - show_level)
                    loadAMT_cm = int(round(loadAMT / (galsperinch * factor), 1))
                    text = '''{}\nLevel: {} KG / {} {}\n可卸货量: {} KG / {} {}'''.format(
                        show_time, show_level, show_level_cm, uom, loadAMT, loadAMT_cm, uom)
                    self.update_annot(pos, text)
                    self.annot.set_visible(True)
                    self.canvas.draw_idle()
                else:
                    if vis:
                        self.annot.set_visible(False)
                        self.canvas.draw_idle()
            else:
                # pass
                if vis:
                    self.annot.set_visible(False)
                    self.canvas.draw_idle()

    def on_click(self, event):
        '''处理鼠标点击事件'''

        if event.button != 3:  # 右键点击
            return

        if self.confirm_order_popup is not None and not self.confirm_order_popup.closed:
            """
            已经弹出确认订单的弹窗，不再弹出
            """
            messagebox.showinfo( title='提示', message='订单确认弹窗已经打开，请勿重复打开!')
            return

        for curve in self.forecast_plot_ax.get_lines():
            if curve.contains(event)[0]:
                graph_id = curve.get_gid()
                graph_dict = {'point_history': self.ts_history,
                              'point_forecast': self.ts_forecast,
                              'point_forecastBeforeTrip': self.ts_forecast_before_trip,
                              'point_manual': self.ts_manual}
                if graph_id in graph_dict.keys():
                    df_data = graph_dict[graph_id]
                    full = self.df_info.FullTrycockGals.values[0]
                    ind = curve.contains(event)[1]['ind'][0]
                    pos = (event.xdata, event.ydata)
                    show_time = df_data.index[ind].strftime("%Y-%m-%d %H:%M")
                    show_level = int(df_data.values.flatten()[ind])

                    loadAMT = int(full - show_level)

                    # 弹出消息框
                    self.confirm_order_popup = ConfirmOrderPopupUI(
                        root=self.root,
                        order_data_manager=self.order_data_manager,
                        df_info = self.df_info,
                        show_time=show_time,
                        loadAMT=loadAMT,
                        order_popup_ui=self.order_popup_ui
                    )
                    return

        confirm_create_empty_order = messagebox.askyesno(
            title='确认订单',
            message='是否确认在无液位辅助的情况下创建订单？'
        )
        if confirm_create_empty_order:
            if self.df_info is None or len(self.df_info) == 0:
                messagebox.showerror( title='警告', message='请先选择客户!')
                return
            # 弹出消息框
            self.confirm_order_popup = ConfirmOrderPopupUI(
                root=self.root,
                order_data_manager=self.order_data_manager,
                df_info=self.df_info,
                order_popup_ui=self.order_popup_ui
            )
            return

    def update_annot(self, pos, text):
        '''更新注释'''

        if self.annot is None:
            # 创建注释
            self.annot = self.forecast_plot_ax.annotate(text,
                                                        xy=pos, xytext=(-20, 20),
                                                        textcoords="offset points",
                                                        bbox=dict(boxstyle="round", fc="w"),
                                                        arrowprops=dict(arrowstyle="->"))
        else:
            # 更新注释的位置和文本
            self.annot.xy = pos
            self.annot.set_text(text)

    def check_cust_name_valid(self, cust_name):
        if cust_name not in self.df_name_forecast.index:
            messagebox.showinfo( title='Warning', message='No Data To Show!')
            if self.lock.locked():
                self.lock.release()
            return False
        return True

    def check_validate_shipto(self, shipto):
        validate_flag = self.time_validate_check(shipto)

        # 如果查询得到 shipto,则显示 shipto,否则 将 shipto 设为 1
        if (not validate_flag[0]) and self.var_telemetry_flag.get() == 0:
            # 2023-10-31 新增逻辑
            # 如果 self.var_telemetry_flag 为 1,说明正在使用 api,
            error_msg = validate_flag[1]
            if 'Time Wrong' in error_msg:
                # 说明时间填错
                messagebox.showinfo( title='Warning', message=error_msg)
                if self.lock.locked():
                    self.lock.release()
            else:
                # 说明时间没有填错, 遇到了 无法预测的情况
                # 提醒采用 dol api 的选项
                error_msg = error_msg + ' -> 请使用 远控 最新 选项 试试'
                messagebox.showinfo( title='Warning', message=error_msg)
                if self.lock.locked():
                    self.lock.release()
            return False
        return True

    def get_primary_dt_and_max_payload(self, shipto):
        df_max_payload = self.data_manager.get_max_payload_by_ship2(
            ship2=str(shipto)
        )

        current_primary_dt = '__'
        current_max_payload = 'unknown'
        for i, row in df_max_payload.iterrows():
            if not pd.isna(row['LicenseFill']) and row['LicenseFill'] > 0:
                current_max_payload = row['LicenseFill']
            current_primary_dt = row['CorporateIdn']
        return current_primary_dt, current_max_payload

    def main_plot(self):
        '''作图主函数'''
        conn = self.data_manager.conn
        df_name_forecast = self.df_name_forecast

        custName = self.listbox_customer.get(self.listbox_customer.curselection()[0])
        # 检查 From time 和 to time 是否正确
        if not self.check_cust_name_valid(custName):
            return

        shipto = int(df_name_forecast.loc[custName].values[0])
        self.order_data_manager.insert_call_log(shipto= str(shipto), cust_name=custName)

        if custName in self.delivery_shipto_dict:
            self.delivery_shipto_dict[custName].latest_called = pd.Timestamp.now()

        selected_index = self.listbox_customer.curselection()
        if selected_index:
            index = selected_index[0]
            # 将选中的项的文字颜色变成黑色
            self.listbox_customer.itemconfig(index, {'fg': 'black'})

        TELE_type = df_name_forecast.loc[custName, 'Subscriber']
        if not self.check_validate_shipto(shipto=shipto):
            return

        fromTime = pd.to_datetime(self.from_box.get())
        toTime = pd.to_datetime(self.to_box.get())
        # 2023-09-04 更新 DOL API 数据
        # print(self.var_telemetry_flag.get())
        # 如果 shipto 是龙口的,不需要更新,不是龙口的,需要 api 查询后更新
        # 2024-09-03 更新： 只有是 DOL 或 LCT 才需要更新；
        if self.var_telemetry_flag.get() == 1:
            if TELE_type == 3:
                updateDOL(shipto, conn)
            elif TELE_type == 7:
                updateLCT(shipto, conn)

        self.df_info = self.data_manager.get_customer_info(shipto)
        df_history = self.data_manager.get_history_reading(shipto, fromTime, toTime)
        df_forecastBeforeTrip = self.data_manager.get_forecast_before_trip(shipto, fromTime, toTime)
        df_forecast = self.data_manager.get_forecast_reading(shipto, fromTime, toTime)

        # 2023-10-31 需要做一步判断：如果 df_forecast 的 Forecasted_Reading 异常,那么就需要清空。
        if len(df_forecast) > 0:
            if df_forecast.Forecasted_Reading.values[0] in [777777, 888888, 999999]:
                df_forecast.Forecasted_Reading = None

        # 作图数据处理
        self.ts_history = df_history[['ReadingDate', 'Reading_Gals']].set_index('ReadingDate')
        self.ts_forecast = df_forecast[['Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
        self.ts_forecast_before_trip = df_forecastBeforeTrip[[
            'Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
        ts_forecast_usage = df_forecast[['Next_hr',
                                         'Hourly_Usage_Rate']].set_index('Next_hr')
        # 记录四个液位值
        full = self.df_info.FullTrycockGals.values[0]
        TR = self.df_info.TargetGalsUser.values[0]
        RO = self.df_info.RunoutGals.values[0]
        Risk = (RO + TR) / 2
        # 防止 Risk 是 None 而 无法 int
        Risk = Risk if Risk is None else int(Risk)
        galsperinch = self.df_info.GalsPerInch.values[0]
        unitOfLength = self.df_info.UnitOfLength.values[0]
        uom = unitOfLength_dict[unitOfLength]
        # 记录三个液位时间
        if len(df_forecast) > 0:
            TR_time = df_forecast.iloc[0].TargetRefillDate
            Risk_time = df_forecast.iloc[0].TargetRiskDate
            RO_time = df_forecast.iloc[0].TargetRunoutDate
        else:
            TR_time = None
            Risk_time = None
            RO_time = None

        self.draw_picture_for_current_shipto(
            custName=custName,
            shipto=shipto,
            fromTime=fromTime,
            toTime=toTime,
            full=full,
            TR=TR,
            Risk=Risk,
            RO=RO,
            TR_time=TR_time,
            Risk_time=Risk_time,
            RO_time=RO_time,
            df_history=df_history,
            uom=uom
        )

        # 点击作图时,同时显示客户的充装的详细信息
        self.show_info(
            shipto=shipto,
            TR_time=TR_time,
            Risk_time=Risk_time,
            RO_time=RO_time,
            full=full,
            TR=TR,
            Risk=Risk,
            RO=RO,
            ts_forecast_usage=ts_forecast_usage,
            galsperinch=galsperinch,
            uom=uom
        )

        if self.lock.locked():
            self.lock.release()

    def draw_picture_for_current_shipto(
            self,
            custName: str,
            shipto: int,
            fromTime: datetime,
            toTime: datetime,
            full: float,
            TR: float,
            Risk: float,
            RO: float,
            TR_time: datetime,
            Risk_time: datetime,
            RO_time: datetime,
            df_history: pd.DataFrame,
            uom: str = 'Ton',
    ):
        # 开始作图
        self.forecast_plot_ax.clear()
        # 下面设置zorder，防止主图和直方图的重叠，以及防止直方图挡得住主图的annotation
        self.forecast_plot_ax.set_zorder(3)
        self.forecast_plot_ax_histy.set_zorder(1)
        self.forecast_plot_ax.patch.set_visible(False)  # 防止主图的背景覆盖直方图
        # 新增注释
        self.annot = self.forecast_plot_ax.annotate("", xy=(0, 0), xytext=(20, 12), textcoords="offset points",
                                                    bbox=dict(boxstyle="round", fc="lightblue",
                                                              ec="steelblue", alpha=1),
                                                    arrowprops=dict(arrowstyle="->"),
                                                    annotation_clip=True, zorder=5)
        self.annot.set_visible(False)

        # 转换历史数据和预测数据为吨
        if len(df_history) > 0:
            df_history['Reading_Gals'] = df_history['Reading_Gals']
        if self.manual_plot:
            df_manual = self.data_manager.get_manual_forecast(shipto, fromTime, toTime)
            df_manual['Forecasted_Reading'] = df_manual['Forecasted_Reading']

        if len(df_history) > 0:
            pic_title = '{}({}) History and Forecast Level'.format(custName, shipto)
        else:
            pic_title = '{}({}) No History Data'.format(custName, shipto)
        self.forecast_plot_ax.set_title(pic_title, fontsize=20)

        # 调整 Y 轴标签为 'Ton'
        self.forecast_plot_ax.set_ylabel('Ton')

        # 转换满载量和阈值为吨
        full_ton = full / 1000
        TR_ton = TR / 1000
        Risk_ton = Risk / 1000 if Risk is not None else None
        RO_ton = RO / 1000

        # 设置 Y 轴范围
        self.forecast_plot_ax.set_ylim(bottom=0, top=full_ton * 1.18)

        # 绘制历史数据和预测数据
        if len(df_history) > 0:
            self.ts_history = df_history[['ReadingDate', 'Reading_Gals']].set_index('ReadingDate')
            self.forecast_plot_ax.plot(self.ts_history  / 1000 , color='blue', marker='o', markersize=6,
                                       linestyle='None', gid='point_history')
            self.forecast_plot_ax.plot(self.ts_history  / 1000, color='blue', label='Actual', linestyle='-', gid='line_history')

        self.forecast_plot_ax.plot(self.ts_forecast / 1000, color='green', marker='o', markersize=6, alpha=0.45,
                                   linestyle='None', gid='point_forecast')
        self.forecast_plot_ax.plot(self.ts_forecast / 1000, color='green', label='Forecast', alpha=0.45,
                                   linestyle='dashed', gid='line_forecast')
        self.forecast_plot_ax.plot(self.ts_forecast_before_trip / 1000, color='orange', marker='o', markersize=6,
                                   linestyle='None', gid='point_forecastBeforeTrip')
        self.forecast_plot_ax.plot(self.ts_forecast_before_trip / 1000, color='orange',
                                   label='FcstBfTrip', linestyle='dashed', gid='line_forecastBeforeTrip')

        if len(self.ts_forecast_before_trip) > 0 and len(self.ts_forecast) > 0:
            ts_join = pd.concat([self.ts_forecast_before_trip.last('1S'), self.ts_forecast.first('1S')])
            self.forecast_plot_ax.plot(ts_join / 1000, color='orange', linestyle='dashed', gid='line_join')

        # 绘制手动预测数据
        if self.manual_plot:
            self.ts_manual = df_manual[['Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
            self.forecast_plot_ax.plot(self.ts_manual/ 1000  , color='purple', marker='o', markersize=6,
                                       linestyle='None', gid='point_manual', alpha=0.6)
            self.forecast_plot_ax.plot(self.ts_manual / 1000, color='purple', label='Manual',
                                       linestyle='dashed', alpha=0.6)

        # 绘制水平线
        self.forecast_plot_ax.axhline(y=full_ton, color='grey', linewidth=2, label='Full', gid='line_full')
        self.forecast_plot_ax.axhline(y=TR_ton, color='green', linewidth=2, label='TR', gid='line_TR')
        if Risk_ton is not None:
            self.forecast_plot_ax.axhline(y=Risk_ton, color='yellow', linewidth=2, label='Risk', gid='line_Risk')
        self.forecast_plot_ax.axhline(y=RO_ton, color='red', linewidth=2, label='RunOut', gid='line_RO')

        # 绘制竖直线
        if TR_time is not None:
            self.plot_vertical_lines(fromTime, toTime, TR_time, Risk_time, RO_time, full_ton)

        # 设置 X 轴主刻度
        if (toTime - fromTime).days <= 12:
            self.forecast_plot_ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 1)))
        elif (toTime - fromTime).days <= 24:
            self.forecast_plot_ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 2)))
        else:
            self.forecast_plot_ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 4)))

        self.forecast_plot_ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

        # 绘制第二 Y 轴
        factor = func.weight_length_factor(uom)

        self.forecast_plot_ax.grid()

        # 新增 直方图
        beforeRD = self.data_manager.get_before_reading(shipto)
        if len(beforeRD) > 0:
            beforeRD = beforeRD / 1000  # 将直方图数据转换为吨
            binwidth = 0.2  # 适当的binwidth，单位为吨
            xymax = np.max(np.abs(beforeRD))
            lim = (int(xymax / binwidth) + 1) * binwidth
            bins = np.arange(0, lim + binwidth, binwidth)
        else:
            bins = np.arange(0, 2, 1)

        self.forecast_plot_ax_histy.clear()
        axHist_info = self.forecast_plot_ax_histy.hist(beforeRD, bins=bins, edgecolor='black', color='blue',
                                                       orientation='horizontal')
        self.forecast_plot_ax_histy.tick_params(
            axis='y',
            which='both',  # both major and minor ticks are affected
            bottom=False,  # ticks along the bottom edge are off
            top=False,  # ticks along the top edge are off
            labelleft=False,
        )
        if len(beforeRD) > 0:
            max_count = np.max(axHist_info[0])
            xticks = func.define_xticks(max_count)
        else:
            xticks = np.arange(0, 2, 1)
        self.forecast_plot_ax_histy.set_xticks(xticks)
        self.forecast_plot_ax_histy.grid()
        self.canvas.draw_idle()
        self.toolbar.update()

        if self.save_pic:
            self.pic_figure.savefig('./feedback.png')

    def plot(self):
        '''多线程作图主函数'''
        starttime = time.time()
        lock = self.lock
        # lock the thread
        while True:
            if lock.acquire(blocking=False) is True:
                break
            else:
                endtime = time.time()
                duration = round(endtime - starttime, 3)
                print('lock is not free')
                time.sleep(0.5)
                if duration > 8:
                    if lock.locked():
                        lock.release()
        try:
            self.main_plot()
        except Exception as e:
            if lock.locked():
                lock.release()
    # endregion

    # region 刷新数据
    def refresh_data(self, show_message=True):
        try:
            conn = self.data_manager.conn
            cur = self.data_manager.cur
            data_refresh = ForecastDataRefresh(local_cur=cur, local_conn=conn)
            data_refresh.refresh_lb_hourly_data()
            self.delivery_shipto_dict = self.data_manager.generate_trip_shipto_dict()
            self.supplement_delivery_shipto_latest_called()
            self.show_list_cust(None)
            refresh_time_text = self.data_manager.get_last_refresh_time()
            self.refresh_time_label.config(text='最新液位时间:\n{}'.format(refresh_time_text))

            func.log_connection(self.log_file, 'refreshed')
            if show_message:
                messagebox.showinfo(title='success', message='data to sqlite success!')
        except Exception as e:
            messagebox.showinfo(title='failure', message='failure, please check! {}'.format(e))


    # endregion