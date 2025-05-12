from src.utils.Email_forecast import send_email
from matplotlib.lines import Line2D
from src.forecast_data_refresh.odbc_master import check_refresh_deliveryWindow
from src.utils import decorator
from datetime import datetime
from datetime import timedelta
import matplotlib.pylab as pylab
import tkinter as tk
from tkinter import ttk
import pandas as pd
import numpy as np
import sqlite3
import os
from matplotlib.backends.backend_tkagg import (
    FigureCanvasTkAgg, NavigationToolbar2Tk)
from matplotlib.figure import Figure
import matplotlib.dates as mdates
from matplotlib.dates import DayLocator
from tkinter import messagebox
# from dateEntry import DateEntry
import matplotlib
import time
import threading
from . import ui_structure
from ..utils.dol_api import updateDOL
from ..utils.lct_api import updateLCT
from ..forecast_data_refresh.daily_data_refresh import ForecastDataRefresh
from .lb_data_manager import LBDataManager
from ..utils import functions as func
from ..utils.constant import unitOfLength_dict
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
            conn,
            cur,
            path1: str
    ):

        self.root = root
        self.conn = conn
        self.cur = cur

        # lock
        self.lock = threading.Lock()
        self.annot = None
        # 提取数据的类
        self.data_manager = LBDataManager(conn, cur)

        self.df_name_forecast = self.data_manager.get_forecast_customer_from_sqlite()
        self.df_info = None
        self.ts_history = None
        self.ts_forecast = None
        self.ts_forecast_before_trip = None
        self.ts_manual = None

        # 日志记录
        self.log_file = os.path.join(path1, 'LB_Forecasting\\log.txt')
        func.log_connection(self.log_file, 'opened')

        # setup ui
        self._setup_ui()

    def info_cust_frame(self):
        '''建立客户名称的frame,也即第二模块'''
        frame_name = tk.LabelFrame(self.par_frame, text='Cust')
        frame_name.grid(row=0, column=0, padx=5, pady=5)
        return frame_name


    def _decorate_input_framework(self):
        # 输入 起始日期
        framename = self.frame_input

        self.lb_fromtime = tk.Label(framename, text='from time')
        self.lb_fromtime.grid(row=0, column=0, padx=10, pady=5)
        self.from_box = tk.Entry(framename)
        # 初始化 起始日期
        start_day = (datetime.now().date() - timedelta(days=2)).strftime("%Y-%m-%d")
        self.from_box.insert(0, start_day)
        self.from_box.grid(row=0, column=1, padx=10, pady=5)
        # 输入 结束日期
        self.lb_totime = tk.Label(framename, text='to time')
        self.lb_totime.grid(row=1, column=0, padx=10, pady=5)
        self.to_box = tk.Entry(framename)
        # 初始化 结束日期
        end_day = (datetime.now().date() + timedelta(days=3)).strftime("%Y-%m-%d")

        self.to_box.insert(0, end_day)
        self.to_box.grid(row=1, column=1, padx=10, pady=5)

        # 设置刷新按钮
        self.btn_refresh = tk.Button(framename, text='Refresh data',
                                command=self.refresh_data)
        self.btn_refresh.grid(row=2, column=0, padx=10, pady=10)
        
        # 设置是否需要 从DOL API 下载数据
        self.var_telemetry_flag = tk.IntVar()
        self.check_telemetry_flag = tk.Checkbutton(framename, text='远控 最新', variable=self.var_telemetry_flag, onvalue=1, offvalue=0)
        self.check_telemetry_flag.grid(row=2, column=1, padx=1, pady=10)


    def _set_subregion_boxlist(self):
        '''subRegion boxlist'''

        self.listbox_subregion = tk.Listbox(self.f_frame, height=5, width=10, exportselection=False)
        subregion_list = self.df_name_forecast.SubRegion.unique()
        for item in sorted(subregion_list):
            self.listbox_subregion.insert(tk.END, item)
        self.listbox_subregion.grid(row=0, column=0, padx=1, pady=1)


    def _set_terminal_boxlist(self):
        '''terminal boxlist'''
        self.terminal_frame = tk.LabelFrame(self.f_frame)
        # scrollbar
        scroll_y = tk.Scrollbar(self.terminal_frame, orient=tk.VERTICAL)
        # 这里需要特别学习：exportselection=False
        # 保证了 两个 Listbox 点击一个时,不影响第二个。
        self.listbox_terminal = tk.Listbox(
            self.terminal_frame, selectmode="extended", height=6, width=12, yscrollcommand=scroll_y.set, exportselection=False)
        scroll_y.config(command=self.listbox_terminal.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.terminal_frame.grid(row=0, column=1, padx=1, pady=1)
        self.listbox_terminal.pack()


    def _set_products_boxlist(self):
        '''products boxlist'''
        self.listbox_products = tk.Listbox(self.f_frame, selectmode="extended",
                                      height=4, width=10, exportselection=False)
        self.listbox_products.grid(row=1, column=0, padx=1, pady=1)


    def _set_demand_type_boxlist(self):
        self.listbox_demand_type = tk.Listbox(self.f_frame, selectmode="extended",
                                        height=4, width=10, exportselection=False)
        self.listbox_demand_type.grid(row=1, column=1, padx=1, pady=1)


    def _set_customer_query(self):
        self.entry_name = tk.Entry(self.cust_frame, width=20, bg='white', fg='black', borderwidth=1)
        self.entry_name.grid(row=0, column=0)

        self.btn_query = tk.Button(self.cust_frame, text='Search', command=lambda: self.cust_btn_search())
        self.btn_query.grid(row=0, column=1, padx=2)

        self.cust_name_selection_frame = tk.LabelFrame(self.cust_frame, text='Customer Name')
        self.cust_name_selection_frame.grid(row=1, column=0, padx=5, pady=5, columnspan=2)

        self._decorate_cust_name_selection_frame()



    def _decorate_cust_name_selection_frame(self):
        ''' customer boxlist'''
        # 新增滚动轴 scrollbar
        scroll_y = tk.Scrollbar(self.cust_name_selection_frame, orient=tk.VERTICAL)
        # 这里需要特别学习：exportselection=False
        # 保证了 两个 Listbox 点击一个时,不影响第二个。
        self.listbox_customer = tk.Listbox(
            self.cust_name_selection_frame, height=10, width=20, yscrollcommand=scroll_y.set, exportselection=False)
        scroll_y.config(command=self.listbox_customer.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        self.listbox_customer.pack()
        self.listbox_customer.bind("<<ListboxSelect>>", lambda event: threading.Thread(
            target=self.plot).start())


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
        custName_list = sorted(df_name_forecast[f_SubRegion & f_product & f_terminal & f_FO].index)
        # print('cust no: ', len(custName_list))
        for item in custName_list:
            self.listbox_customer.insert(tk.END, item)


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
            ("CustName", "cust_name"),
            ("__ MaxPayload", "max_payload_label"),
            ("TargetTime", "target_time"),
            ("RiskTime", "risk_time"),
            ("RunOutTime", "runout_time"),
            ("FullTrycock", "full_trycock"),
            ("TargetRefill", "target_refill"),
            ("Risk", "risk"),
            ("Runout", "runout"),
            ("forecast_hour_range", "forecast_hourly_usage"),
            ("ForecastError", "forecast_error"),
        ]

        for i, (label_text, key) in enumerate(label_info):
            lb_label = tk.Label(self.frame_detail, text=label_text)
            lb_label.grid(row=i, column=0, padx=6, pady=pad_y)

            lb_value = tk.Label(self.frame_detail, text="")
            lb_value.grid(row=i, column=1, padx=6, pady=pad_y)

            if label_text in ["__ MaxPayload", "forecast_hour_range"]:
                self.detail_labels[label_text] = lb_label
            self.detail_labels[key] = lb_value
    
    def _set_manipulate_frame(self):
        self.frame_warning = tk.LabelFrame(self.manipulate_frame, text='Warning')
        self.frame_warning.grid(row=0, column=0, padx=2, pady=2)

        self._set_frame_warning_label()

        # 重新排版,建立 frame_detail
        self.frame_manual = tk.LabelFrame(self.manipulate_frame, text='Manual Input')
        self.frame_manual.grid(row=1, column=0, padx=2, pady=2)
        # 输入 起始日期
        self._set_manual_input_label()

    def _set_frame_warning_label(self):
        # 添加一个标签作为示例
        self.t4_t6_label = tk.Label(self.frame_warning, text="T6-T4 recent 3-time average (h): ")
        self.t4_t6_label.grid(row=0, column=0, padx=6, pady=0)

        self.t4_t6_value_label = tk.Label(self.frame_warning, text="")
        self.t4_t6_value_label.grid(row=0, column=1, padx=6, pady=0)


    def _set_manual_input_label(self):
        '''for schedulers manually input their estimation about hourly usage'''

        conn = self.conn
        pad_y = 0
        lb_cm = tk.Label(self.frame_manual, text='CM Hourly')
        lb_cm.grid(row=0, column=0, padx=1, pady=pad_y)
    
        self.box_cm = tk.Entry(self.frame_manual, width=10)
        self.box_cm.grid(row=0, column=1, padx=1, pady=pad_y)
        lb_kg = tk.Label(self.frame_manual, text='KG Hourly')
        lb_kg.grid(row=1, column=0, padx=1, pady=pad_y)
        self.box_kg = tk.Entry(self.frame_manual, width=10)
        self.box_kg.grid(row=1, column=1, padx=1, pady=pad_y)
        btn_calculate = tk.Button(self.frame_manual, text='Calculate by Input', width=15,
                                  command=self.calculate_by_manual)
        btn_calculate.grid(row=2, column=0, pady=3, columnspan=2)
        btn_reset = tk.Button(self.frame_manual, text='Reset', width=15,
                              command=self.reset_manual)
        btn_reset.grid(row=3, column=0, pady=3, columnspan=2)
        lb_assess = tk.Label(self.frame_manual, text='Feedback: ')
        lb_assess.grid(row=4, column=0, padx=1, pady=pad_y)

        assess_options = ['', '预测准确', '预测误差小', '预测误差大']
        self.combo_assess = ttk.Combobox(self.frame_manual, value=assess_options)
        self.combo_assess.grid(row=4, column=1, padx=1, pady=pad_y)
        lb_reason = tk.Label(self.frame_manual, text='Reason: ')
        lb_reason.grid(row=5, column=0, padx=1, pady=pad_y)
        reason_options = ['', '并联罐', '生产计划原因', '节日长假', '突发情况', '模型有改进空间']
        self.combo_reason = ttk.Combobox(self.frame_manual, value=reason_options)
        self.combo_reason.grid(row=5, column=1, padx=1, pady=5)
        btn_email = tk.Button(self.frame_manual, text='Send Email', width=15)
        btn_email.grid(row=6, column=0, pady=1, columnspan=2)
        btn_email.bind('<Button-1>', lambda event: threading.Thread(target=self.send_feedback,
                                                                    args=(event,)).start())
        lb_time1 = tk.Label(self.frame_manual, text='Last Time: ')
        lb_time1.grid(row=7, column=0, padx=1, pady=pad_y)
        sql = 'select MAX(ReadingDate) from historyReading '
        lastTime = pd.read_sql(sql, conn).values.flatten()[0]
        lb_time2 = tk.Label(self.frame_manual, text='{}'.format(lastTime))
        lb_time2.grid(row=7, column=1, padx=1, pady=pad_y)


    def create_manual_forecast_data(self, shipto, input_value):
        '''create_manual_forecast_data'''
        conn = self.conn
        table_name = 'forecastBeforeTrip'
        sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
        df = pd.read_sql(sql, conn)
        if len(df) == 0:
            table_name = 'historyReading'
            sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
            df = pd.read_sql(sql, conn)
            if len(df) == 0:
                messagebox.showinfo( title='Warning', message='No history Data To Show')
                return
        else:
            table_name = 'forecastReading'
            sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
            df = pd.read_sql(sql, conn)
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
        cur = self.cur
        conn = self.conn
        df_name_forecast = self.df_name_forecast

        input_value1 = self.box_kg.get()
        input_value2 = self.box_cm.get()
        if len(input_value1) > 0 and len(input_value2) > 0:
            messagebox.showinfo( title='Warning', message='Cannot KM+CM')
            return
        if len(input_value1) > 0:
            try:
                input_value = float(input_value1)
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
        self.box_kg.delete(0, 'end')
        self.box_cm.delete(0, 'end')
        event = None
        self.plot()


    def _decorate_historical_readings_frame(self):
        self.reading_tree_frame = tk.LabelFrame(self.historical_readings_frame)
        self.reading_tree_frame.pack(fill='both', expand=True, padx=5, pady=2)
        self._set_reading_tree()

        self.delivery_window_tree_frame = tk.LabelFrame(self.historical_readings_frame)
        self.delivery_window_tree_frame.pack(fill='both', expand=True, padx=5, pady=2)
        self._set_delivery_window_tree()

    def _set_reading_tree(self):
        columns = ["No", "ReadingDate", "Read_KG", "Read_CM", "Hour_CM"]
        col_widths = [10, 100, 20, 20, 20]

        self.reading_tree_table = ui_structure.SimpleTable(
            self.reading_tree_frame, columns=columns, col_widths=col_widths, height=5)
        self.reading_tree_table.frame.pack(fill="both", expand=True)

    def _set_delivery_window_tree(self):
        columns = ["title", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        col_widths = [70, 30, 30, 30, 30, 30, 30, 30]

        self.delivery_window_tree_table = ui_structure.SimpleTable(
            self.delivery_window_tree_frame, columns=columns, col_widths=col_widths, height=5)
        self.delivery_window_tree_table.frame.pack(fill="both", expand=True)

    def _decorate_dtd_cluster_label(self):
        dtd_cluster_frame = self.dtd_cluster_frame
        # 上方 Frame：Terminal/Source DTD 模块
        self.frame_dtd = tk.LabelFrame(dtd_cluster_frame, text="Terminal/Source DTD")
        self.frame_dtd.pack(fill='both', expand=True, padx=5, pady=2)

        self._set_dtd_label()

        # 下方 Frame：临近客户模块
        self.frame_near_customer = tk.LabelFrame(dtd_cluster_frame, text="临近客户")
        self.frame_near_customer.pack(fill='both', expand=True, padx=5, pady=2)

        self._set_near_customer_label()


    def _set_dtd_label(self):
        columns = ["DT", "距离(km)", "时长(h)", "发车时间"]
        col_widths = [10, 20, 20, 100]

        self.dtd_table = ui_structure.SimpleTable(self.frame_dtd, columns=columns, col_widths=col_widths, height=5)
        self.dtd_table.frame.pack(fill="both", expand=True)

    def _set_near_customer_label(self):
        columns = ["临近客户简称", "距离(km)", "DDER"]
        col_widths = [100, 20, 10]

        self.near_customer_table = ui_structure.SimpleTable(self.frame_near_customer, columns=columns,
                                                            col_widths=col_widths,
                                                            height=4)
        self.near_customer_table.frame.pack(fill="both", expand=True)
    
    def _decorate_filter_frame(self):
        self._set_subregion_boxlist()
        self._set_terminal_boxlist()
        self._set_products_boxlist()
        self._set_demand_type_boxlist()

        self.listbox_subregion.bind("<<ListboxSelect>>", self.show_list_terminal_product_FO)
        self.listbox_terminal.bind("<<ListboxSelect>>", self.show_list_cust)
        self.listbox_products.bind("<<ListboxSelect>>", self.show_list_cust)
        self.listbox_demand_type.bind("<<ListboxSelect>>", self.show_list_cust)

    def _decorate_plot_frame(self):
        # plot_frame column 0, row 0: 筛选区域
        self.plot_frame.columnconfigure(0, weight=1)
        self.f_frame = tk.LabelFrame(self.plot_frame, text='Filter')
        self.f_frame.grid(row=0, column=0, padx=2, pady=1)
        self._decorate_filter_frame()

        # plot_frame column 0, row 1: 重新排版,建立 frame_input
        self.frame_input = tk.LabelFrame(self.plot_frame, text='input')
        self.frame_input.grid(row=1, column=0, padx=2, pady=5)
        self._decorate_input_framework()

        # plot_frame column 1, row 0：作图区域
        self.plot_frame.columnconfigure(1, weight=8)
        self.pic_frame = tk.LabelFrame(self.plot_frame)
        self.pic_frame.grid(row=0, column=1, rowspan=2, sticky=tk.E + tk.W + tk.N + tk.S)
        self.pic_frame.rowconfigure(0, weight=1)
        self.pic_frame.columnconfigure(0, weight=1)
        self._set_pic_frame()

        self.annot = None
        self.save_pic = False
        self.manual_plot = False

        # plot_frame column 2, row 0：: 新增 DTD and Cluster 的 Frame
        self.plot_frame.columnconfigure(2, weight=3)
        self.dtd_cluster_frame = tk.LabelFrame(self.plot_frame)
        self.dtd_cluster_frame.grid(row=0, column=2, rowspan=2, padx=2, pady=2, sticky="nsew")
        self._decorate_dtd_cluster_label()

    def _decorate_par_frame(self):
        # par_frame column 0, row 0: 客户筛选区域
        self.par_frame.columnconfigure(0, weight=1)
        self.cust_frame = tk.LabelFrame(self.par_frame, text='Cust')
        self.cust_frame.grid(row=0, column=0, padx=5, pady=5)
        self._set_customer_query()

        # par_frame column 1, row 0: 建立 frame_detail
        self.par_frame.columnconfigure(1, weight=1)
        self.frame_detail = tk.LabelFrame(self.par_frame, text='Detailed Info')
        self.frame_detail.grid(row=0, column=1, padx=10, pady=2)
        self._set_detail_info_label()

        # par_frame column 2, row 0: 建立 手工操作区域
        self.par_frame.columnconfigure(2, weight=1)
        self.manipulate_frame = tk.LabelFrame(self.par_frame)
        self.manipulate_frame.grid(row=0, column=2, padx=2, pady=2)
        self._set_manipulate_frame()

        # par_frame column 3, row 0: 两个 Treeview 历史液位记录和时间窗
        self.par_frame.columnconfigure(3, weight=2)
        self.historical_readings_frame = tk.LabelFrame(self.par_frame)
        self.historical_readings_frame.grid(row=0, column=3, padx=2, pady=1, sticky="nsew")
        self._decorate_historical_readings_frame()

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
    def update_dtd_table(self, shipto_id: str, risk_time: pd.Timestamp):
        results = self.data_manager.get_primary_terminal_dtd_info(shipto_id)

        # 添加 Primary DTD 信息
        primary_info = []
        for row in results:
            primary_dt, distance, duration = row
            primary_info.append('T{}'.format(primary_dt))
            primary_info.append(distance)
            primary_info.append(duration)

            departure_time = ''
            try:
                departure_time = risk_time - pd.Timedelta(minutes=int(float(duration) * 60))
                departure_time = departure_time.strftime('%Y-%m-%d %H:%M')
            except Exception as e:
                print(e)

            primary_info.append(departure_time)

        results = self.data_manager.get_sourcing_terminal_dtd_info(shipto_id)
        # 添加 Source DTD 信息
        source_list = []
        for row in results:
            source_info = list()
            source_dt, distance, duration = row
            source_info.append('S{}'.format(source_dt))
            source_info.append(distance)
            source_info.append(duration)

            departure_time = ''
            try:
                departure_time = risk_time - pd.Timedelta(minutes=int(float(duration) * 60))
                departure_time = departure_time.strftime('%Y-%m-%d %H:%M')
            except Exception as e:
                print(e)
            source_info.append(departure_time)

            source_list.append(source_info)

        rows = [primary_info] + source_list
        self.dtd_table.insert_rows(rows)


    def update_near_customer_table(self, shipto_id: str):
        results = self.data_manager.get_near_customer_info(shipto_id)
        update_rows = list()
        for row in results:
            update_row = list()
            to_loc_num, to_cust_acronym, distance_km, dder = row

            if to_cust_acronym is None or len(to_cust_acronym.strip()) == 0:
                to_cust_acronym = to_loc_num

            try:
                dder = round(float(dder) * 100, 2)
            except Exception as e:
                print(e)
                dder = '?'

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

    def show_info(self, shipto, custName, TR_time, Risk_time, RO_time, full, TR,
                  Risk, RO, ts_forecast_usage, galsperinch, uom):
        '''显示客户的充装的详细信息'''
        self.clean_detailed_info()

        factor = func.weight_length_factor(uom)

        if Risk_time is None:
            # 只挑选部分内容显示
            self.detail_labels['cust_name'].config(text=custName)
            full_cm = int(full / galsperinch / factor)
            self.detail_labels['full_trycock'].config(text=f'{full} KG / {full_cm} {uom}')
            TR_cm = int(TR / galsperinch / factor)
            self.detail_labels['target_refill'].config(text=f'{TR} KG / {TR_cm} {uom}')
            RO_cm = int(RO / galsperinch / factor)
            self.detail_labels['runout'].config(text=f'{RO} KG / {RO_cm} {uom}')
        else:
            tr = TR_time.strftime("%Y-%m-%d %H:%M")
            risk = Risk_time.strftime("%Y-%m-%d %H:%M")
            ro = RO_time.strftime("%Y-%m-%d %H:%M")
            self.detail_labels['cust_name'].config(text=custName)
            self.detail_labels['target_time'].config(text=tr)
            self.detail_labels['risk_time'].config(text=risk)
            self.detail_labels['runout_time'].config(text=ro)

            full_cm = int(full / galsperinch / factor)
            self.detail_labels['full_trycock'].config(text=f'{full} KG / {full_cm} {uom}')
            TR_cm = int(TR / galsperinch / factor)
            self.detail_labels['target_refill'].config(text=f'{TR} KG / {TR_cm} {uom}')
            Risk_cm = int(Risk / galsperinch / factor)
            self.detail_labels['risk'].config(text=f'{Risk} KG / {Risk_cm} {uom}')
            RO_cm = int(RO / galsperinch / factor)
            self.detail_labels['runout'].config(text=f'{RO} KG / {RO_cm} {uom}')

            if len(ts_forecast_usage) >= 2:
                s_time = ts_forecast_usage.index[0].strftime("%m-%d %H:%M")
                e_time = ts_forecast_usage.index[min(7, len(ts_forecast_usage) - 1)].strftime("%m-%d %H:%M")
                hourly_usage = round(ts_forecast_usage[:8].mean().values[0], 1)
                hourly_usage_cm = round(hourly_usage / (galsperinch * factor), 1)
                self.detail_labels['forecast_hour_range'].config(
                    text=f'{s_time}~{e_time}\n 预测小时用量'
                )
                self.detail_labels['forecast_hourly_usage'].config(
                    text=f'{hourly_usage} KG / {hourly_usage_cm} {uom}'
                )
            else:
                self.detail_labels['forecast_hour_range'].config(text='')
                self.detail_labels['forecast_hourly_usage'].config(text='')

        fe = self.data_manager.get_forecast_error(shipto)
        self.detail_labels['forecast_error'].config(text=fe)

        current_primary_dt, current_max_payload = self.get_primary_dt_and_max_payload(shipto)
        current_max_payload = int(current_max_payload) if isinstance(current_max_payload, float) else current_max_payload
        self.detail_labels['__ MaxPayload'].config(text=f'{current_primary_dt} MaxPayload')
        self.detail_labels['max_payload_label'].config(text=f'{current_max_payload}')

        t4_t6_value = self.data_manager.get_t4_t6_value(shipto=shipto)
        self.t4_t6_value_label.config(text=t4_t6_value)

        # 显示历史液位
        self.update_reading_tree_table(shipto_id=str(shipto))
        # # 显示送货窗口
        self.update_delivery_window_tree_table(shipto_id=str(shipto))

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
        self.pic_figure = Figure(figsize=(5, 4), dpi=80)
        gs = self.pic_figure.add_gridspec(1, 2, width_ratios=(6, 1),
                              left=0.08, right=0.96, bottom=0.1, top=0.9,
                              wspace=0.1, hspace=0.05)
        self.forecast_plot_ax = self.pic_figure.add_subplot(gs[0, 0])
        self.forecast_plot_ax_histy = self.pic_figure.add_subplot(gs[0, 1], sharey=self.forecast_plot_ax)
      
        self.canvas = FigureCanvasTkAgg(self.pic_figure, master=framename)  # A tk.DrawingArea.
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
       
        self.canvas.mpl_connect("motion_notify_event", self.hover)
        self.toolbar = NavigationToolbar2Tk(self.canvas, framename)


    def update_annot(self, pos, text):
        '''填写注释内容'''
        self.annot.xy = pos
        self.annot.set_text(text)

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

    def hover_disappear(self, event):
        '''取消悬浮'''

        if mutex.acquire(2):
            if self.annot is None:
                mutex.release()
                return
            vis = self.annot.get_visible()
            if vis:
                for curve in self.forecast_plot_ax.get_lines():
                    if curve.contains(event)[0]:
                        graph_id = curve.get_gid()
                        print('vis test:', vis, id(self.annot), graph_id)
                        hover_curves = ['point_history', 'line_history', 'point_forecast',
                                        'line_forecast', 'point_forecastBeforeTrip',
                                        'line_forecastBeforeTrip', 'line_join']
                        if graph_id not in hover_curves:
                            time.sleep(2)
                            self.annot.set_visible(False)
                            self.canvas.draw_idle()
                            mutex.release()
                            return
                    else:
                        time.sleep(2)
                        self.annot.set_visible(False)
                        self.canvas.draw_idle()
                        print('no touch:', self.annot.get_visible(), id(self.annot))
            mutex.release()

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
                error_msg = error_msg + ' -> 请使用 api 试试'
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
        conn = self.conn
        df_name_forecast = self.df_name_forecast

        custName = self.listbox_customer.get(self.listbox_customer.curselection()[0])
        print('Customer: {}'.format(custName))
        # 检查 From time 和 to time 是否正确
        if not self.check_cust_name_valid(custName):
            return

        shipto = int(df_name_forecast.loc[custName].values[0])
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
            custName=custName,
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
            uom: str = 'KG',
    ):
        # 开始作图
        # 没想到这句话还这么重要(在hover的时候造成了极大的困扰)
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
        if len(df_history) > 0:
            pic_title = '{}({}) History and Forecast Level'.format(custName, shipto)
        else:
            pic_title = '{}({}) No History Data'.format(custName, shipto)
        self.forecast_plot_ax.set_title(pic_title, fontsize=20)
        self.forecast_plot_ax.set_ylabel('K G')
        self.forecast_plot_ax.set_ylim(bottom=0, top=full * 1.18)
        # self.forecast_plot_ax.set_xlabel('Date')
        self.forecast_plot_ax.plot(self.ts_history, color='blue', marker='o', markersize=6,
                                   linestyle='None', gid='point_history')
        self.forecast_plot_ax.plot(self.ts_history, color='blue', label='Actual', linestyle='-', gid='line_history')
        self.forecast_plot_ax.plot(self.ts_forecast, color='green', marker='o', markersize=6, alpha=0.45,
                                   linestyle='None', gid='point_forecast')
        self.forecast_plot_ax.plot(self.ts_forecast, color='green', label='Forecast', alpha=0.45,
                                   linestyle='dashed', gid='line_forecast')
        self.forecast_plot_ax.plot(self.ts_forecast_before_trip, color='orange', marker='o', markersize=6,
                                   linestyle='None', gid='point_forecastBeforeTrip')
        self.forecast_plot_ax.plot(self.ts_forecast_before_trip, color='orange',
                                   label='FcstBfTrip', linestyle='dashed', gid='line_forecastBeforeTrip')
        if len(self.ts_forecast_before_trip) > 0 and len(self.ts_forecast) > 0:
            ts_join = pd.concat([self.ts_forecast_before_trip.last('1S'), self.ts_forecast.first('1S')])
            # print(ts_join)
            self.forecast_plot_ax.plot(ts_join, color='orange', linestyle='dashed', gid='line_join')
        # decide to plot manual forecast_data_refresh line
        if self.manual_plot:
            df_manual = self.data_manager.get_manual_forecast(shipto, fromTime, toTime)
            self.ts_manual = df_manual[['Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
            self.forecast_plot_ax.plot(self.ts_manual, color='purple', marker='o', markersize=6,
                                       linestyle='None', gid='point_manual', alpha=0.6)
            self.forecast_plot_ax.plot(self.ts_manual, color='purple', label='Manual',
                                       linestyle='dashed', alpha=0.6)
        # 以下画水平线
        self.forecast_plot_ax.axhline(y=full, color='grey', linewidth=2, label='Full', gid='line_full')
        self.forecast_plot_ax.axhline(y=TR, color='green', linewidth=2, label='TR', gid='line_TR')
        if Risk is not None:
            self.forecast_plot_ax.axhline(y=Risk, color='yellow', linewidth=2, label='Risk', gid='line_Risk')
        self.forecast_plot_ax.axhline(y=RO, color='red', linewidth=2, label='RunOut', gid='line_RO')
        # 画竖直线,较繁琐。具体函数见定义
        if TR_time is not None:
            self.plot_vertical_lines(fromTime, toTime, TR_time, Risk_time, RO_time, full)
        if (toTime - fromTime).days <= 12:
            self.forecast_plot_ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 1)))
        elif (toTime - fromTime).days <= 24:
            self.forecast_plot_ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 2)))
        else:
            self.forecast_plot_ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 4)))

        self.forecast_plot_ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        # plot for second y-axis
        factor = func.weight_length_factor(uom)

        self.forecast_plot_ax.grid()
        # 2024-04-18 新增 直方图
        beforeRD = self.data_manager.get_before_reading(shipto)
        if len(beforeRD) > 0:
            binwidth = 200
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
            # labelbottom=False,
            labelleft=False,
            # left=False
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
            print(e)
            if lock.locked():
                lock.release()
    # endregion

    # region 刷新数据
    def refresh_data(self, show_message=True):
        try:
            conn = self.conn
            cur = self.cur
            data_refresh = ForecastDataRefresh(local_cur=cur, local_conn=conn)
            data_refresh.refresh_lb_hourly_data()
            func.log_connection(self.log_file, 'refreshed')
            if show_message:
                messagebox.showinfo(title='success', message='data to sqlite success!')
        except Exception as e:
            messagebox.showinfo(title='failure', message='failure, please check! {}'.format(e))
    # endregion