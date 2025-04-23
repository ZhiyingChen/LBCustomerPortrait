from tkinter import ttk

class NewInterface(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self._build_modern_ui()
        self._setup_data_handling()

    def _build_modern_ui(self):
        """构建新界面组件"""
        header = ttk.Frame(self)
        header.pack(fill='x', pady=10)

        ttk.Label(header, text="新界面",
                  font=('Helvetica', 16, 'bold')).pack(side='left')

        # 添加你的新组件...

    def _setup_data_handling(self):
        """与旧界面共享数据"""
        # 通过master访问主应用实例
        self.shared_conn = self.master.master.conn
        self.shared_cur = self.master.master.cur