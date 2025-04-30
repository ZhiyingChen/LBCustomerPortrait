import tkinter as tk
from tkinter import ttk


class SimpleTable:
    def __init__(self, parent, columns, col_widths=None, height=10):
        self.frame = tk.Frame(parent)

        self.tree = ttk.Treeview(
            self.frame,
            columns=columns,
            show="headings",
            height=height
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

    def insert_rows(self, rows):
        """ 插入多行数据，清空旧数据 """
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert("", "end", values=row)

    def clear(self):
        self.tree.delete(*self.tree.get_children())