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
            cur
    ):
        self.root = root
        self.conn = conn
        self.cur = cur

        self.lock = threading.Lock()

        self.data_manager = LBDataManager(conn, cur)


    def clean_detailed_info(self):
        for key, label in self.detail_labels.items():
            label.config(text='')

    def show_info(self, custName, TR_time, Risk_time, RO_time, full, TR,
                  Risk, RO, ts_forecast_usage, galsperinch, uom, fe,
                  primary_dt, max_payload):
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


        self.detail_labels['forecast_error'].config(text=fe)
        payload = int(max_payload) if isinstance(max_payload, float) else max_payload
        self.detail_labels['__ MaxPayload'].config(text=f'{primary_dt} MaxPayload')
        self.detail_labels['max_payload_label'].config(text=f'{payload}')


    def time_validate_check(self, shipto):
        ''''检查box的内容是否正确'''
        validate_flag = (True, True)
        try:
            fromTime = pd.to_datetime(from_box.get())
        except ValueError:
            validate_flag = (False, 'From Time Wrong!')
            return validate_flag
        try:
            toTime = pd.to_datetime(to_box.get())
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
            ax.axvline(x=TR_time, color='green', linewidth=1)
            ax.fill_between(x=[TR_time, toTime], y1=full, facecolor='green', alpha=alpha)
        if fromTime <= TR_time <= Risk_time <= toTime <= RO_time:
            ax.axvline(x=TR_time, color='green', linewidth=1)
            ax.axvline(x=Risk_time, color='yellow', linewidth=1)
            ax.fill_between(x=[TR_time, Risk_time], y1=full, facecolor='green', alpha=alpha)
            ax.fill_between(x=[Risk_time, toTime], y1=full, facecolor='red', alpha=alpha)
        if fromTime <= TR_time <= Risk_time <= RO_time <= toTime:
            # 这个是最完整形态
            ax.axvline(x=TR_time, color='green', linewidth=1)
            ax.axvline(x=Risk_time, color='yellow', linewidth=1, )
            ax.axvline(x=RO_time, color='red', linewidth=1)
            ax.fill_between(x=[TR_time, Risk_time], y1=full, facecolor='green', alpha=alpha)
            ax.fill_between(x=[Risk_time, RO_time], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= fromTime <= toTime <= Risk_time <= RO_time:
            ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='green', alpha=alpha)
        if TR_time <= fromTime <= Risk_time <= toTime <= RO_time:
            ax.axvline(x=Risk_time, color='yellow', linewidth=1)
            ax.fill_between(x=[fromTime, Risk_time], y1=full, facecolor='green', alpha=alpha)
            ax.fill_between(x=[Risk_time, toTime], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= fromTime <= Risk_time <= RO_time <= toTime:
            ax.axvline(x=Risk_time, color='green', linewidth=1)
            ax.axvline(x=RO_time, color='red', linewidth=1)
            ax.fill_between(x=[fromTime, Risk_time], y1=full, facecolor='green', alpha=alpha)
            ax.fill_between(x=[Risk_time, RO_time], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= Risk_time <= fromTime <= toTime <= RO_time:
            ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= Risk_time <= fromTime <= RO_time <= toTime:
            ax.axvline(x=RO_time, color='red', linewidth=1)
            ax.fill_between(x=[fromTime, RO_time], y1=full, facecolor='red', alpha=alpha)
        if TR_time <= Risk_time <= RO_time <= fromTime <= toTime:
            ax.fill_between(x=[fromTime, toTime], y1=full, facecolor='red', alpha=alpha)


    def get_plot_basic(self, framename):
        '''获取作图框架'''
        fig = Figure(figsize=(5, 4), dpi=80)
        gs = fig.add_gridspec(1, 2, width_ratios=(6, 1),
                              left=0.08, right=0.96, bottom=0.1, top=0.9,
                              wspace=0.1, hspace=0.05)
        ax = fig.add_subplot(gs[0, 0])
        ax_histy = fig.add_subplot(gs[0, 1], sharey=ax)
        # ax = fig.add_subplot(111)
        canvas = FigureCanvasTkAgg(fig, master=framename)  # A tk.DrawingArea.
        canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        # canvas.get_tk_widget().grid(row=0, column=1)
        toolbar = NavigationToolbar2Tk(canvas, framename)
        return fig, ax, ax_histy, canvas, toolbar


    def update_annot(self, pos, text):
        '''填写注释内容'''
        # pos = sc.get_offsets()[ind["ind"][0]]
        global annot
        annot.xy = pos
        annot.set_text(text)


    def hover(self, event):
        '''悬浮'''
        global annot
        global canvas
        if annot is None:
            return
        vis = annot.get_visible()
        for curve in ax.get_lines():
            # Searching which data member corresponds to current mouse position
            if curve.contains(event)[0]:
                graph_id = curve.get_gid()
                if 'ts_manual' in globals():
                    graph_dict = {'point_history': ts_history,
                                  'point_forecast': ts_forecast,
                                  'point_forecastBeforeTrip': ts_forecastBeforeTrip,
                                  'point_manual': ts_manual}
                else:
                    graph_dict = {'point_history': ts_history,
                                  'point_forecast': ts_forecast,
                                  'point_forecastBeforeTrip': ts_forecastBeforeTrip}
                if graph_id in graph_dict.keys():
                    if vis:
                        # 说明已经有了一个 annot, 就不再显示第二个了。
                        return
                    df_data = graph_dict[graph_id]
                    full = df_info.FullTrycockGals.values[0]
                    ind = curve.contains(event)[1]['ind'][0]
                    # pos = (event.x, event.y)
                    pos = (event.xdata, event.ydata)
                    show_time = df_data.index[ind].strftime("%Y-%m-%d %H:%M")
                    show_level = int(df_data.values.flatten()[ind])
                    # 转成长度单位
                    galsperinch = df_info.GalsPerInch.values[0]
                    unitOfLength = df_info.UnitOfLength.values[0]
                    uom = unitOfLength_dict[unitOfLength]
                    factor = func.weight_length_factor(uom)
                    show_level_cm = int(round(show_level / (galsperinch * factor), 1))
                    # 可卸货量
                    loadAMT = int(full - show_level)
                    loadAMT_cm = int(round(loadAMT / (galsperinch * factor), 1))
                    text = '''{}\nLevel: {} KG / {} {}\n可卸货量: {} KG / {} {}'''.format(
                        show_time, show_level, show_level_cm, uom, loadAMT, loadAMT_cm, uom)
                    self.update_annot(pos, text)
                    annot.set_visible(True)
                    canvas.draw_idle()
                else:
                    if vis:
                        annot.set_visible(False)
                        canvas.draw_idle()
            else:
                # pass
                if vis:
                    annot.set_visible(False)
                    canvas.draw_idle()


    def hover_disappear(self, event):
        '''取消悬浮'''
        global annot
        global canvas
        if mutex.acquire(2):
            if annot is None:
                mutex.release()
                return
            vis = annot.get_visible()
            if vis:
                for curve in ax.get_lines():
                    if curve.contains(event)[0]:
                        graph_id = curve.get_gid()
                        print('vis test:', vis, id(annot), graph_id)
                        hover_curves = ['point_history', 'line_history', 'point_forecast',
                                        'line_forecast', 'point_forecastBeforeTrip',
                                        'line_forecastBeforeTrip', 'line_join']
                        if graph_id not in hover_curves:
                            time.sleep(2)
                            annot.set_visible(False)
                            canvas.draw_idle()
                            mutex.release()
                            return
                    else:
                        time.sleep(2)
                        annot.set_visible(False)
                        canvas.draw_idle()
                        print('no touch:', annot.get_visible(), id(annot))
            mutex.release()


    def main_plot(self):
        '''作图主函数'''
        root = self.root
        conn = self.conn
        lock = self.lock
        custName = listbox_customer.get(listbox_customer.curselection()[0])
        print('Customer: {}'.format(custName))
        # 检查 From time 和 to time 是否正确
        if custName not in df_name_forecast.index:
            messagebox.showinfo(parent=root, title='Warning', message='No Data To Show!')
            if lock.locked():
                lock.release()
        else:
            shipto = int(df_name_forecast.loc[custName].values[0])
            TELE_type = df_name_forecast.loc[custName, 'Subscriber']
            validate_flag = self.time_validate_check(shipto)
            # print(custName, type(custName))
            # 如果查询得到 shipto,则显示 shipto,否则 将 shipto 设为 1
            if (not validate_flag[0]) and var_TELE.get() == 0:
                # 2023-10-31 新增逻辑
                # 如果 var_TELE 为 1,说明正在使用 api,
                error_msg = validate_flag[1]
                if 'Time Wrong' in error_msg:
                    # 说明时间填错
                    messagebox.showinfo(parent=root, title='Warning', message=error_msg)
                    if lock.locked():
                        lock.release()
                else:
                    # 说明时间没有填错, 遇到了 无法预测的情况
                    # 提醒采用 dol api 的选项
                    error_msg = error_msg + ' -> 请使用 api 试试'
                    messagebox.showinfo(parent=root, title='Warning', message=error_msg)
                    if lock.locked():
                        lock.release()
            else:
                fromTime = pd.to_datetime(from_box.get())
                toTime = pd.to_datetime(to_box.get())
                # 2023-09-04 更新 DOL API 数据
                # print(var_TELE.get())
                # 如果 shipto 是龙口的,不需要更新,不是龙口的,需要 api 查询后更新
                # 2024-09-03 更新： 只有是 DOL 或 LCT 才需要更新；
                if var_TELE.get() == 1:
                    if TELE_type == 3:
                        updateDOL(shipto, conn)
                    elif TELE_type == 7:
                        updateLCT(shipto, conn)
                    else:
                        pass
                # 获取数据
                # 首先根据客户简称,获取 shipto
                global df_info
                df_info = self.data_manager.get_customer_info(shipto)
                df_history = self.data_manager.get_history_reading(shipto, fromTime, toTime)
                df_forecastBeforeTrip = self.data_manager.get_forecast_before_trip(shipto, fromTime, toTime)
                df_forecast = self.data_manager.get_forecast_reading(shipto, fromTime, toTime)
                df_max_payload = self.data_manager.get_max_payload_by_ship2(
                    ship2=str(shipto),
                )
                # 2023-10-31 需要做一步判断：如果 df_forecast 的 Forecasted_Reading 异常,那么就需要清空。
                if len(df_forecast) > 0:
                    if df_forecast.Forecasted_Reading.values[0] in [777777, 888888, 999999]:
                        df_forecast.Forecasted_Reading = None

                current_primary_dt = '__'
                current_max_payload = 'unknown'
                for i, row in df_max_payload.iterrows():
                    if not pd.isna(row['LicenseFill']) and row['LicenseFill'] > 0:
                        current_max_payload = row['LicenseFill']
                    current_primary_dt = row['CorporateIdn']

                # 作图数据处理
                global ts_history, ts_forecast, ts_forecastBeforeTrip
                ts_history = df_history[['ReadingDate', 'Reading_Gals']].set_index('ReadingDate')
                ts_forecast = df_forecast[['Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
                ts_forecastBeforeTrip = df_forecastBeforeTrip[[
                    'Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
                ts_forecast_usage = df_forecast[['Next_hr',
                                                 'Hourly_Usage_Rate']].set_index('Next_hr')
                # 记录四个液位值
                full = df_info.FullTrycockGals.values[0]
                TR = df_info.TargetGalsUser.values[0]
                RO = df_info.RunoutGals.values[0]
                Risk = (RO + TR) / 2
                # 防止 Risk 是 None 而 无法 int
                Risk = Risk if Risk is None else int(Risk)
                galsperinch = df_info.GalsPerInch.values[0]
                unitOfLength = df_info.UnitOfLength.values[0]
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
                # 开始作图
                # 没想到这句话还这么重要(在hover的时候造成了极大的困扰)
                ax.clear()
                # 下面设置zorder，防止主图和直方图的重叠，以及防止直方图挡得住主图的annotation
                ax.set_zorder(3)
                ax_histy.set_zorder(1)
                ax.patch.set_visible(False)  # 防止主图的背景覆盖直方图
                # 新增注释
                global annot
                annot = ax.annotate("", xy=(0, 0), xytext=(20, 12), textcoords="offset points",
                                    bbox=dict(boxstyle="round", fc="lightblue",
                                              ec="steelblue", alpha=1),
                                    arrowprops=dict(arrowstyle="->"),
                                    annotation_clip=True, zorder=5)
                annot.set_visible(False)
                if len(df_history) > 0:
                    pic_title = '{}({}) History and Forecast Level'.format(custName, shipto)
                else:
                    pic_title = '{}({}) No History Data'.format(custName, shipto)
                ax.set_title(pic_title, fontsize=20)
                ax.set_ylabel('K G')
                ax.set_ylim(bottom=0, top=full * 1.18)
                # ax.set_xlabel('Date')
                ax.plot(ts_history, color='blue', marker='o', markersize=6,
                        linestyle='None', gid='point_history')
                ax.plot(ts_history, color='blue', label='Actual', linestyle='-', gid='line_history')
                ax.plot(ts_forecast, color='green', marker='o', markersize=6, alpha=0.45,
                        linestyle='None', gid='point_forecast')
                ax.plot(ts_forecast, color='green', label='Forecast', alpha=0.45,
                        linestyle='dashed', gid='line_forecast')
                ax.plot(ts_forecastBeforeTrip, color='orange', marker='o', markersize=6,
                        linestyle='None', gid='point_forecastBeforeTrip')
                ax.plot(ts_forecastBeforeTrip, color='orange',
                        label='FcstBfTrip', linestyle='dashed', gid='line_forecastBeforeTrip')
                if (len(ts_forecastBeforeTrip) > 0 and len(ts_forecast) > 0):
                    ts_join = pd.concat([ts_forecastBeforeTrip.last('1S'), ts_forecast.first('1S')])
                    # print(ts_join)
                    ax.plot(ts_join, color='orange', linestyle='dashed', gid='line_join')
                # decide to plot manual forecast_data_refresh line
                if manual_plot:
                    df_manual = self.data_manager.get_manual_forecast(shipto, fromTime, toTime)
                    global ts_manual
                    ts_manual = df_manual[['Next_hr', 'Forecasted_Reading']].set_index('Next_hr')
                    ax.plot(ts_manual, color='purple', marker='o', markersize=6,
                            linestyle='None', gid='point_manual', alpha=0.6)
                    ax.plot(ts_manual, color='purple', label='Manual',
                            linestyle='dashed', alpha=0.6)
                # 以下画水平线
                ax.axhline(y=full, color='grey', linewidth=2, label='Full', gid='line_full')
                ax.axhline(y=TR, color='green', linewidth=2, label='TR', gid='line_TR')
                if Risk is not None:
                    ax.axhline(y=Risk, color='yellow', linewidth=2, label='Risk', gid='line_Risk')
                ax.axhline(y=RO, color='red', linewidth=2, label='RunOut', gid='line_RO')
                # 画竖直线,较繁琐。具体函数见定义
                if TR_time is not None:
                    self.plot_vertical_lines(fromTime, toTime, TR_time, Risk_time, RO_time, full)
                if (toTime - fromTime).days <= 12:
                    ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 1)))
                elif (toTime - fromTime).days <= 24:
                    ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 2)))
                else:
                    ax.xaxis.set_major_locator(DayLocator(bymonthday=range(1, 32, 4)))
                # fig.autofmt_xdate()
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
                # plot for second y-axis
                factor = func.weight_length_factor(uom)

                def kg2cm(x):
                    return x / (galsperinch * factor)

                def cm2kg(x):
                    return x * (galsperinch * factor)

                ax.grid()

                # 2024-04-18 新增 直方图
                beforeRD = self.data_manager.get_before_reading(shipto)
                if len(beforeRD) > 0:
                    binwidth = 200
                    xymax = np.max(np.abs(beforeRD))
                    lim = (int(xymax / binwidth) + 1) * binwidth
                    bins = np.arange(0, lim + binwidth, binwidth)
                else:
                    bins = np.arange(0, 2, 1)

                ax_histy.clear()
                axHist_info = ax_histy.hist(beforeRD, bins=bins, edgecolor='black', color='blue', orientation='horizontal')
                ax_histy.tick_params(
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
                ax_histy.set_xticks(xticks)
                ax_histy.grid()
                # plt.tight_layout()
                canvas.draw_idle()
                toolbar.update()
                # print(111)
                # path = 'C:\Users\zhoud8\Documents\OneDrive - Air Products and Chemicals, Inc\python_project\gui\Forecasting'
                if save_pic:
                    fig.savefig('./feedback.png')
                # 点击作图时,同时显示客户的充装的详细信息
                fe = self.data_manager.get_forecast_error(shipto)

                self.show_info(custName, TR_time, Risk_time, RO_time, full,
                          TR, Risk, RO, ts_forecast_usage, galsperinch, uom, fe,
                          primary_dt=current_primary_dt, max_payload=current_max_payload
                          )
                t4_t6_value = self.data_manager.get_t4_t6_value(shipto=shipto)
                t4_t6_value_label.config(text=t4_t6_value)
                # 显示历史液位
                self.treeview_data(shipto, reading_tree, 'reading')
                # 显示送货窗口
                self.treeview_data(shipto, deliveryWindow_tree, 'deliveryWindow')
                if lock.locked():
                    lock.release()

                self.update_dtd_table(shipto_id=str(shipto), risk_time=Risk_time)
                self.update_near_customer_table(shipto_id=str(shipto))



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


    def refresh_data(self,show_message=True):
        try:
            conn = self.conn
            cur = self.cur
            data_refresh = ForecastDataRefresh(local_cur=cur, local_conn=conn)
            data_refresh.refresh_lb_hourly_data()
            func.log_connection(log_file, 'refreshed')
            if show_message:
                messagebox.showinfo(title='success', message='data to sqlite success!')
        except Exception as e:
            messagebox.showinfo(title='failure', message='failure, please check! {}'.format(e))


    def info_fiter_frame(self, par_frame):
        '''建立筛选部分的frame,也即第一模块'''
        frame_name = tk.LabelFrame(par_frame, text='Filter')
        frame_name.grid(row=0, column=0, padx=5, pady=5)
        return frame_name


    def info_cust_frame(self, par_frame):
        '''建立客户名称的frame,也即第二模块'''
        frame_name = tk.LabelFrame(par_frame, text='Cust')
        frame_name.grid(row=0, column=0, padx=5, pady=5)
        return frame_name


    def input_framework(self, framename):
        # 输入 起始日期
        lb_fromtime = tk.Label(framename, text='from time')
        lb_fromtime.grid(row=0, column=0, padx=10, pady=5)
        global from_box, to_box
        from_box = tk.Entry(framename)
        # 初始化 起始日期
        startday = (datetime.now().date() - timedelta(days=2)).strftime("%Y-%m-%d")
        from_box.insert(0, startday)
        from_box.grid(row=0, column=1, padx=10, pady=5)
        # 输入 结束日期
        lb_totime = tk.Label(framename, text='to time')
        lb_totime.grid(row=1, column=0, padx=10, pady=5)
        to_box = tk.Entry(framename)
        # 初始化 结束日期
        endday = (datetime.now().date() + timedelta(days=3)).strftime("%Y-%m-%d")
        to_box.insert(0, endday)
        to_box.grid(row=1, column=1, padx=10, pady=5)
        # 设置刷新按钮
        btn_refresh = tk.Button(framename, text='Refresh data',
                                command=self.refresh_data)
        btn_refresh.grid(row=2, column=0, padx=10, pady=10)
        # 设置是否需要 从DOL API 下载数据
        global var_TELE
        var_TELE = tk.IntVar()
        check_TELE = tk.Checkbutton(framename, text='远控 最新', variable=var_TELE, onvalue=1, offvalue=0)
        check_TELE.grid(row=2, column=1, padx=1, pady=10)


    def subRegion_boxlist(self, framename):
        '''subRegion boxlist'''
        global listbox_subRegion
        listbox_subRegion = tk.Listbox(framename, height=5, width=10, exportselection=False)
        subRegion_list = df_name_forecast.SubRegion.unique()
        for item in sorted(subRegion_list):
            listbox_subRegion.insert(tk.END, item)
        listbox_subRegion.grid(row=0, column=0, padx=1, pady=1)


    def terminal_boxlist(self, framename):
        '''terminal boxlist'''
        frame_name = tk.LabelFrame(framename)
        # scrollbar
        scroll_y = tk.Scrollbar(frame_name, orient=tk.VERTICAL)
        # 这里需要特别学习：exportselection=False
        # 保证了 两个 Listbox 点击一个时,不影响第二个。
        global listbox_terminal
        listbox_terminal = tk.Listbox(
            frame_name, selectmode="extended", height=6, width=12, yscrollcommand=scroll_y.set, exportselection=False)
        scroll_y.config(command=listbox_terminal.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        frame_name.grid(row=0, column=1, padx=1, pady=1)
        listbox_terminal.pack()


    def products_boxlist(self, framename):
        '''products boxlist'''
        global listbox_products
        listbox_products = tk.Listbox(framename, selectmode="extended",
                                      height=4, width=10, exportselection=False)
        listbox_products.grid(row=1, column=0, padx=1, pady=1)


    def demandType_boxlist(self, framename):
        global listbox_demandType
        listbox_demandType = tk.Listbox(framename, selectmode="extended",
                                        height=4, width=10, exportselection=False)
        listbox_demandType.grid(row=1, column=1, padx=1, pady=1)


    def customer_query(self, framename):
        global entry_name
        entry_name = tk.Entry(framename, width=20, bg='white', fg='black', borderwidth=1)
        entry_name.grid(row=0, column=0)

    def customer_boxlist(self, framename):
        ''' customer boxlist'''
        frame_name = tk.LabelFrame(framename, text='Customer Name')
        # 新增滚动轴 scrollbar
        scroll_y = tk.Scrollbar(frame_name, orient=tk.VERTICAL)
        # 这里需要特别学习：exportselection=False
        # 保证了 两个 Listbox 点击一个时,不影响第二个。
        global listbox_customer
        listbox_customer = tk.Listbox(
            frame_name, height=10, width=20, yscrollcommand=scroll_y.set, exportselection=False)
        scroll_y.config(command=listbox_customer.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        frame_name.grid(row=1, column=0, padx=5, pady=5, columnspan=2)
        listbox_customer.pack()


    def show_list_cust(self, event):
        '''当点击 terminal 的时候显示客户名单'''
        listbox_customer.delete(0, tk.END)
        if listbox_subRegion.curselection() is None or len(listbox_subRegion.curselection()) == 0:
            SubRegion = None
        else:
            SubRegion = listbox_subRegion.get(listbox_subRegion.curselection()[0])
        if listbox_terminal.curselection() is None or len(listbox_terminal.curselection()) == 0:
            cur_terminal = None
        else:
            cur_no = listbox_terminal.curselection()
            cur_terminal = [listbox_terminal.get(i) for i in cur_no]
            # print(cur_no, '->', cur_terminal)
        if listbox_products.curselection() is None or len(listbox_products.curselection()) == 0:
            cur_product = None
        else:
            cur_no = listbox_products.curselection()
            cur_product = [listbox_products.get(i) for i in cur_no]
        if listbox_demandType.curselection() is None or len(listbox_demandType.curselection()) == 0:
            cur_FO = None
        else:
            cur_no = listbox_demandType.curselection()
            cur_FO = [listbox_demandType.get(i) for i in cur_no]
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
        # global custName_list
        custName_list = sorted(df_name_forecast[f_SubRegion & f_product & f_terminal & f_FO].index)
        # print('cust no: ', len(custName_list))
        for item in custName_list:
            listbox_customer.insert(tk.END, item)


    def show_list_terminal_product_FO(self, event):
        '''当点击 subregion 的时候显示 products & terminal & FO'''
        # global terminal_list, product_list, demandType_list
        # 1 terminal
        listbox_terminal.delete(0, tk.END)
        selected_subRegion = listbox_subRegion.get(tk.ANCHOR)
        terminal_list = sorted(list(df_name_forecast.loc[df_name_forecast.SubRegion ==
                                                         selected_subRegion, 'PrimaryTerminal'].unique()))
        for item in terminal_list:
            listbox_terminal.insert(tk.END, item)
        # 2 products
        listbox_products.delete(0, tk.END)
        product_list = df_name_forecast.loc[df_name_forecast.SubRegion ==
                                            selected_subRegion, 'ProductClass'].unique()
        product_list = [func.rank_product(i) for i in product_list]
        product_list = [i[0] for i in sorted(product_list, key=lambda x: x[1])]
        for item in product_list:
            listbox_products.insert(tk.END, item)
        # 3 Demand type
        listbox_demandType.delete(0, tk.END)
        demandType_list = list(df_name_forecast.loc[df_name_forecast.SubRegion ==
                                                    selected_subRegion, 'DemandType'].unique())
        # demandType_list = demandType_list
        for item in sorted(demandType_list):
            listbox_demandType.insert(tk.END, item)
        # 4 自动选择第一个
        listbox_terminal.select_set(0)
        listbox_products.select_set(0)
        listbox_demandType.select_set(0)
        # 显示 listbox_customer
        self.show_list_cust(event)


    def cust_btn_search(self):
        '''search for customer by shipto or name'''
        root = self.root
        info = entry_name.get().strip()
        # print(info)
        if info.isdigit():
            info = int(info)
            names = df_name_all[df_name_all.LocNum == info].index
        else:
            names = df_name_all[df_name_all.index.str.contains(info)].index
        if len(names) == 0:
            messagebox.showinfo(parent=root, title='Warning', message='Check your search!')
        else:
            listbox_customer.delete(0, tk.END)
            for item in sorted(names):
                listbox_customer.insert(tk.END, item)

    def send_feedback(self, event):

        root = self.root

        global save_pic
        save_pic = True
        pic_name = "./feedback.png"
        if os.path.isfile(pic_name):
            os.remove(pic_name)
        # event = None
        self.plot()
        print('testing')
        save_pic = False
        email_worker = send_email()
        result = combo_assess.get()
        reason = combo_reason.get()
        time.sleep(3)
        rounds = 0
        while not os.path.isfile(pic_name):
            time.sleep(2)
            rounds = rounds + 1
            if rounds > 5:
                messagebox.showinfo(parent=root, title='Warning', message='No Data To Send!')
                return
        message_subject, message_body, addressee = email_worker.getEmailData(result, reason)
        email_worker.outlook(addressee, message_subject, message_body)
        messagebox.showinfo(parent=root, title='Success', message='Email been sent!')

    def _detail_info_label(self, framename):
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
            lb_label = tk.Label(framename, text=label_text)
            lb_label.grid(row=i, column=0, padx=6, pady=pad_y)

            lb_value = tk.Label(framename, text="")
            lb_value.grid(row=i, column=1, padx=6, pady=pad_y)

            if label_text in ["__ MaxPayload", "forecast_hour_range"]:
                self.detail_labels[label_text] = lb_label
            self.detail_labels[key] = lb_value


    def frame_warning_label(self, framename):
        global t4_t6_value_label

        # 添加一个标签作为示例
        t4_t6_label = tk.Label(framename, text="T6-T4 recent 3-time average (h): ")
        t4_t6_label.grid(row=0, column=0, padx=6, pady=0)

        t4_t6_value_label = tk.Label(framename, text="")
        t4_t6_value_label.grid(row=0, column=1, padx=6, pady=0)


    def manual_input_label(self, framename):
        '''for schedulers manually input their estimation about hourly usage'''

        conn = self.conn
        pad_y = 0
        lb_cm = tk.Label(framename, text='CM Hourly')
        lb_cm.grid(row=0, column=0, padx=1, pady=pad_y)
        global box_cm, box_kg
        box_cm = tk.Entry(framename, width=10)
        box_cm.grid(row=0, column=1, padx=1, pady=pad_y)
        lb_kg = tk.Label(framename, text='KG Hourly')
        lb_kg.grid(row=1, column=0, padx=1, pady=pad_y)
        box_kg = tk.Entry(framename, width=10)
        box_kg.grid(row=1, column=1, padx=1, pady=pad_y)
        btn_calculate = tk.Button(framename, text='Calculate by Input', width=15,
                                  command=self.calculate_by_manual)
        btn_calculate.grid(row=2, column=0, pady=3, columnspan=2)
        btn_reset = tk.Button(framename, text='Reset', width=15,
                              command=self.reset_manual)
        btn_reset.grid(row=3, column=0, pady=3, columnspan=2)
        lb_assess = tk.Label(framename, text='Feedback: ')
        lb_assess.grid(row=4, column=0, padx=1, pady=pad_y)
        global combo_assess, combo_reason
        assess_options = ['', '预测准确', '预测误差小', '预测误差大']
        combo_assess = ttk.Combobox(framename, value=assess_options)
        # combo_assess.current(1)
        combo_assess.grid(row=4, column=1, padx=1, pady=pad_y)
        lb_reason = tk.Label(framename, text='Reason: ')
        lb_reason.grid(row=5, column=0, padx=1, pady=pad_y)
        reason_options = ['', '并联罐', '生产计划原因', '节日长假', '突发情况', '模型有改进空间']
        combo_reason = ttk.Combobox(framename, value=reason_options)
        combo_reason.grid(row=5, column=1, padx=1, pady=5)
        btn_email = tk.Button(framename, text='Send Email', width=15)
        btn_email.grid(row=6, column=0, pady=1, columnspan=2)
        btn_email.bind('<Button-1>', lambda event: threading.Thread(target=self.send_feedback,
                                                                    args=(event,)).start())
        lb_time1 = tk.Label(framename, text='Last Time: ')
        lb_time1.grid(row=7, column=0, padx=1, pady=pad_y)
        sql = 'select MAX(ReadingDate) from historyReading '
        lastTime = pd.read_sql(sql, conn).values.flatten()[0]
        lb_time2 = tk.Label(framename, text='{}'.format(lastTime))
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
                messagebox.showinfo(parent=root, title='Warning', message='No history Data To Show')
                return
        else:
            table_name = 'forecastReading'
            sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
            df = pd.read_sql(sql, conn)
            df = df[df.Forecasted_Reading.notna()].reset_index(drop=True)
            if len(df) == 0:
                messagebox.showinfo(parent=root, title='Warning', message='No forecast_data_refresh Data To Show')
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
        root = self.root
        cur = self.cur
        conn = self.conn

        input_value1 = box_kg.get()
        input_value2 = box_cm.get()
        if len(input_value1) > 0 and len(input_value2) > 0:
            messagebox.showinfo(parent=root, title='Warning', message='Cannot KM+CM')
            return
        if len(input_value1) > 0:
            try:
                input_value = float(input_value1)
            except ValueError:
                messagebox.showinfo(parent=root, title='Warning', message='Input Wrong')
        else:
            try:
                galsperinch = df_info.GalsPerInch.values[0]
                input_value = float(input_value2) * galsperinch
            except ValueError:
                messagebox.showinfo(parent=root, title='Warning', message='Input Wrong')
                return
        if input_value < 0 or input_value > 50000:
            messagebox.showinfo(parent=root, title='Warning', message='Input Wrong')
            return
        # print(input_value1, input_value2, type(input_value1), type(input_value2))
        custName = listbox_customer.get(tk.ANCHOR)
        if custName not in df_name_forecast.index:
            messagebox.showinfo(parent=root, title='Warning', message='No Data To Show.')
            return
        else:
            shipto = int(df_name_forecast.loc[custName].values[0])
        df = self.create_manual_forecast_data(shipto, input_value)
        table_name = 'manual_forecast'
        cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        conn.commit()
        df.to_sql(table_name, con=conn, if_exists='replace', index=False)
        global manual_plot
        manual_plot = True
        self.plot()
        manual_plot = False


    def reset_manual(self):
        box_kg.delete(0, 'end')
        box_cm.delete(0, 'end')
        event = None
        self.plot()


    def treeView_design(self, framename, width, height, row, column, y_scroll):
        '''增加 treeView'''
        myFrame = tk.Frame(framename, width=width, height=height)
        myFrame.pack_propagate(0)
        myFrame.grid(row=row, column=column, padx=10, pady=5)
        # treeview scrollbar
        if y_scroll:
            tree_scroll_y = tk.Scrollbar(myFrame, orient=tk.VERTICAL)
            tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
            myTree = ttk.Treeview(myFrame, yscrollcommand=tree_scroll_y.set, selectmode='extended')
            # configure the scrollbar
            tree_scroll_y.config(command=myTree.yview)
        else:
            myTree = ttk.Treeview(myFrame, selectmode='extended')
        # Add Style
        style = ttk.Style()
        # pick a theme
        style.theme_use('default')
        style.configure('Treeview', rowheight=25)
        # tree_scroll_x.config(command=myTree.xview)
        # striped row tags
        myTree.tag_configure('oddRow', background='white')
        myTree.tag_configure('evenRow', background='lightblue')
        # 点击属性
        # myTree.bind("<Double-1>", lambda event: OnDoubleClick(event, root))
        return myTree


    def clear_tree(self, treename):
        # 如只删除 treeview
        treename.delete(*treename.get_children())


    def treeview_data(self, shipto, treename, purpose):
        '''显示数据'''
        conn = self.conn
        if purpose == 'reading':
            df = self.data_manager.get_recent_reading(shipto)
            self.clear_tree(treename)
        else:
            df = self.data_manager.get_delivery_window(shipto)
            self.clear_tree(treename)
        # print(df)
        # set up new tree view
        treename['column'] = list(df.columns)
        # print(df)
        # 设置 column 的 属性, 主要是列宽
        for col in df.columns:
            if 'No' in col:
                treename.column(col, anchor=tk.CENTER, width=35)
            elif 'ReadingDate' in col:
                treename.column(col, anchor=tk.CENTER, width=120)
            elif 'Trailer_' in col:
                treename.column(col, anchor=tk.CENTER, width=100)
            elif 'LeaveTime' in col:
                treename.column(col, anchor=tk.CENTER, width=120)
            elif 'CheckInfo' in col:
                treename.column(col, anchor=tk.CENTER, width=120)
            elif 'title' in col:
                treename.column(col, anchor=tk.CENTER, width=80)
            elif col in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
                treename.column(col, anchor=tk.CENTER, width=40)
            else:
                treename.column(col, anchor=tk.CENTER, width=65)

        treename['show'] = 'headings'
        # look thru column list for headers
        global count
        count = 0
        for column in treename['column']:
            treename.heading(column, text=column, anchor=tk.CENTER)
        df_rows = df.to_numpy().tolist()
        # 填入内容
        for row in df_rows:
            if count % 2 == 0:
                treename.insert(parent='', index='end', values=row, tags=('evenRow',))
            else:
                treename.insert(parent='', index='end', values=row, tags=('oddRow',))
            count += 1
        treename.pack()


    def forecaster_run(self,path1):
        root = self.root
        # 建立 筛选区域
        # 补丁
        global plot_flag
        plot_flag = True

        global df_name_forecast, df_name_all
        df_name_forecast = self.data_manager.get_forecast_customer_from_sqlite()
        df_name_all = self.data_manager.get_all_customer_from_sqlite()
        # 建立 作图区域
        plot_frame = tk.LabelFrame(root, text='Plot')
        plot_frame.pack(fill='x', expand=True, padx=2, pady=1)

        # column 0: 筛选区域
        plot_frame.columnconfigure(0, weight=1)
        f_frame = tk.LabelFrame(plot_frame, text='Filter')
        f_frame.grid(row=0, column=0, padx=2, pady=1)
        self.subRegion_boxlist(f_frame)
        self.terminal_boxlist(f_frame)
        self.products_boxlist(f_frame)
        self.demandType_boxlist(f_frame)
        # 重新排版,建立 frame_input
        frame_input = tk.LabelFrame(plot_frame, text='input')
        frame_input.grid(row=1, column=0, padx=2, pady=5)
        self.input_framework(frame_input)

        # column 1：作图区域
        plot_frame.columnconfigure(1, weight=8)
        pic_frame = tk.LabelFrame(plot_frame)
        pic_frame.grid(row=0, column=1, rowspan=2, sticky=tk.E + tk.W + tk.N + tk.S)
        pic_frame.rowconfigure(0, weight=1)
        pic_frame.columnconfigure(0, weight=1)
        global fig, ax, ax_histy, canvas, toolbar, annot
        fig, ax, ax_histy, canvas, toolbar = self.get_plot_basic(pic_frame)

        annot = None


        canvas.mpl_connect("motion_notify_event", self.hover)

        # column 2: 新增 DTD and Cluster 的 Frame
        plot_frame.columnconfigure(2, weight=3)
        dtd_cluster_frame = tk.LabelFrame(plot_frame)
        dtd_cluster_frame.grid(row=0, column=2, rowspan=2, padx=2, pady=2, sticky="nsew")

        self.decorate_dtd_cluster_label(dtd_cluster_frame=dtd_cluster_frame)

        # 最大的frame：par_frame
        par_frame = tk.LabelFrame(root)
        par_frame.pack(fill='x', expand=True, padx=5, pady=1)

        for col in range(4):
            par_frame.columnconfigure(col, weight=1)

        cust_frame = self.info_cust_frame(par_frame)
        self.customer_query(cust_frame)
        global btn_query
        btn_query = tk.Button(cust_frame, text='Search', command=lambda: self.cust_btn_search())
        btn_query.grid(row=0, column=1, padx=2)
        self.customer_boxlist(cust_frame)
        global save_pic, manual_plot
        save_pic = False
        manual_plot = False
        global unitOfLength_dict
        unitOfLength_dict = {1: 'CM', 2: 'Inch', 3: 'M', 4: 'MM', 5: 'Percent', 6: 'Liters'}

        listbox_subRegion.bind("<<ListboxSelect>>", self.show_list_terminal_product_FO)
        listbox_terminal.bind("<<ListboxSelect>>", self.show_list_cust)
        listbox_products.bind("<<ListboxSelect>>", self.show_list_cust)
        listbox_demandType.bind("<<ListboxSelect>>", self.show_list_cust)

        listbox_customer.bind("<<ListboxSelect>>", lambda event: threading.Thread(
            target=self.plot).start())

        # 重新排版,建立 frame_detail
        frame_detail = tk.LabelFrame(par_frame, text='Detailed Info')
        frame_detail.grid(row=0, column=1, padx=10, pady=2)
        # 输入 起始日期
        self._detail_info_label(frame_detail)

        second_col_frame = tk.LabelFrame(par_frame)
        second_col_frame.grid(row=0, column=2, padx=2, pady=2)

        frame_warning = tk.LabelFrame(second_col_frame, text='Warning')
        frame_warning.grid(row=0, column=0, padx=2, pady=2)

        self.frame_warning_label(frame_warning)

        # 重新排版,建立 frame_detail
        frame_manual = tk.LabelFrame(second_col_frame, text='Manual Input')
        frame_manual.grid(row=1, column=0, padx=2, pady=2)
        # 输入 起始日期
        self.manual_input_label(frame_manual)
        # 新增两个 Treeview
        frame_tree = tk.LabelFrame(par_frame, text='Historical Readings')
        frame_tree.grid(row=0, column=3, padx=2, pady=1)
        # 增加历史液位记录
        global reading_tree, deliveryWindow_tree
        reading_tree = self.treeView_design(framename=frame_tree, width=380,
                                       height=120, row=0, column=0, y_scroll=True)
        deliveryWindow_tree = self.treeView_design(framename=frame_tree, width=380,
                                              height=120, row=1, column=0, y_scroll=False)

        global log_file
        log_file = os.path.join(path1, 'LB_Forecasting\\log.txt')
        func.log_connection(log_file, 'opened')


    def decorate_dtd_cluster_label(self, dtd_cluster_frame):
        # 上方 Frame：Terminal/Source DTD 模块
        frame_dtd = tk.LabelFrame(dtd_cluster_frame, text="Terminal/Source DTD")
        frame_dtd.pack(fill='both', expand=True, padx=5, pady=2)

        self.set_dtd_label(dtd_frame=frame_dtd)

        # 下方 Frame：临近客户模块
        frame_near_customer = tk.LabelFrame(dtd_cluster_frame, text="临近客户")
        frame_near_customer.pack(fill='both', expand=True, padx=5, pady=2)

        self.set_near_customer_label(near_customer_frame=frame_near_customer)


    def set_dtd_label(self, dtd_frame):
        global dtd_table

        columns = ["DT", "距离(km)", "时长(h)", "发车时间"]
        col_widths = [10, 20, 20, 100]

        dtd_table = ui_structure.SimpleTable(dtd_frame, columns=columns, col_widths=col_widths, height=5)
        dtd_table.frame.pack(fill="both", expand=True)


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
        dtd_table.insert_rows(rows)


    def set_near_customer_label(self, near_customer_frame):
        global near_customer_table

        columns = ["临近客户简称", "距离(km)", "DDER"]
        col_widths = [100, 20, 10]

        near_customer_table = ui_structure.SimpleTable(near_customer_frame, columns=columns, col_widths=col_widths,
                                                       height=4)
        near_customer_table.frame.pack(fill="both", expand=True)


    def update_near_customer_table(self, shipto_id: str):
        cursor = self.cur
        sql_line = '''
            SELECT ToLocNum, ToCustAcronym, distanceKM, DDER 
            FROM ClusterInfo
            WHERE LocNum={}
            ORDER BY DDER DESC
        '''.format(shipto_id)

        cursor.execute(sql_line)
        results = cursor.fetchall()

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

        near_customer_table.insert_rows(update_rows)
