import tkinter as tk
from tkinter import ttk


class SimpleTable:
    def __init__(self, parent, columns, col_widths=None, height=10):
        self.root = parent
        self.frame = tk.Frame(parent)
        self.frame.pack(fill="both", expand=True)  # 关键点1: 父容器填充扩展

        self.tree = ttk.Treeview(
            self.frame,
            columns=columns,
            show="headings",
            height=height,
            selectmode='extended'
        )

        # 设置列头
        for i, col in enumerate(columns):
            width = col_widths[i] if col_widths and i < len(col_widths) else 100
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor='center')

        # 垂直滚动条
        scrollbar_y = ttk.Scrollbar(self.frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar_y.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        scrollbar_y.grid(row=0, column=1, sticky="ns")

        self.frame.grid_rowconfigure(0, weight=1)
        self.frame.grid_columnconfigure(0, weight=1)

        style = ttk.Style()
        # pick a theme
        style.theme_use('default')
        style.configure('Treeview', rowheight=25)

        self.tree.bind("<Double-1>", self.on_double_click)
        self.tree.bind("<Control-c>", self.copy_selected_to_clipboard)


    def insert_rows(self, rows):
        """ 插入多行数据，清空旧数据 """
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert("", "end", values=row)

    def clear(self):
        self.tree.delete(*self.tree.get_children())


    def on_double_click(self, event):
        item_id = self.tree.focus()
        if item_id:
            values = self.tree.item(item_id, "values")
            # 弹出一个简单的窗口显示内容
            popup = tk.Toplevel()
            popup.title("复制内容")
            text = tk.Text(popup, wrap="word", height=10, width=50)
            text.insert("1.0", '\t'.join(values))  # 用 tab 分隔列
            text.pack(padx=10, pady=10)
            text.focus()

    def copy_selected_to_clipboard(self, event=None):
        selected = self.tree.selection()
        if selected:
            values = self.tree.item(selected[0], "values")[0]
            self.root.clipboard_clear()
            self.root.clipboard_append(values)
            print("已复制到剪贴板：", values)




