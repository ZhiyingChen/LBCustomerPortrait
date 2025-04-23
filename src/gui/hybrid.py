import tkinter as tk
from tkinter import ttk
from ..forecast.forecastPlot8 import update_font, connect_sqlite, refresh_odbc_data, forecaster_run
from .new import NewInterface

class HybridApp(tk.Tk):
    def __init__(self):
        super().__init__()

        # 保持原有初始化逻辑
        self._init_legacy_environment()
        self._create_interface_container()
        self._setup_navigation()

        # 初始化新界面（延迟加载）
        self.new_frame = None

    def _init_legacy_environment(self):
        """继承原有主函数的初始化逻辑"""
        # 保持原有字体设置
        update_font()

        # 保持数据库连接
        self.db_name = 'AutoSchedule.sqlite'
        self.conn = connect_sqlite(self.db_name)
        self.cur = self.conn.cursor()
        refresh_odbc_data(self.conn, self.cur)

        # 保持窗口设置
        self.path1 = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling'
        self.wm_title("Air Products Forecasting Viz")
        self.iconbitmap('./csl.ico')

    def _create_interface_container(self):
        """创建界面容器"""
        # 主容器框架
        self.main_container = ttk.Frame(self)
        self.main_container.pack(fill='both', expand=True)

        # 预测界面容器（保持原有布局）
        self.legacy_container = ttk.Frame(self.main_container)
        self.legacy_container.pack(fill='both', expand=True)

        # 执行原有界面构建逻辑
        forecaster_run(self.legacy_container, self.path1, self.cur, self.conn)

    def _setup_navigation(self):
        """添加导航控制栏"""
        control_bar = ttk.Frame(self)
        control_bar.pack(side='top', fill='x', pady=5)

        ttk.Button(control_bar, text="预测界面",
                   command=self.show_legacy).pack(side='left', padx=10)
        # ttk.Button(control_bar, text="新界面",
        #            command=self.show_new_interface).pack(side='left', padx=10)

    def show_new_interface(self):
        """显示新界面"""
        if self.new_frame is None:
            self._lazy_init_new_interface()

        self.legacy_container.pack_forget()
        self.new_frame.pack(fill='both', expand=True)

    def show_legacy(self):
        """显示预测界面"""
        if self.new_frame:
            self.new_frame.pack_forget()
        self.legacy_container.pack(fill='both', expand=True)

    def _lazy_init_new_interface(self):
        """延迟初始化新界面"""
        self.new_frame = NewInterface(self.main_container)