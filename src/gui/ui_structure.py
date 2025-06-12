import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

class SimpleTable:
    def __init__(self, parent, columns, col_widths=None, height=10, col_stretch=None):
        self.root = parent
        self.frame = tk.Frame(parent)
        self.frame.pack(fill="both", expand=True)  # 关键点1: 父容器填充扩展

        self.tree = ttk.Treeview(
            self.frame,
            columns=columns,
            show="headings",
            height=height,
            selectmode='extended',
            yscrollcommand=lambda f, l: scrollbar_y.set(f, l)
        )

        if col_stretch is None:
            col_stretch = [True] * len(columns)

        # 设置列头
        for i, col in enumerate(columns):
            width = col_widths[i] if col_widths and i < len(col_widths) else 100
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor='center', stretch=col_stretch[i])

        # 垂直滚动条
        scrollbar_y = tk.Scrollbar(self.frame, orient="vertical", command=self.tree.yview)
        scrollbar_y.grid(row=0, column=1, sticky="ns")

        self.tree.grid(row=0, column=0, sticky="nsew")

        self.frame.grid_rowconfigure(0, weight=1)
        self.frame.grid_columnconfigure(0, weight=1)

        style = ttk.Style()
        # pick a theme
        style.theme_use('winnative')
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
        if not item_id:
            return
        values = self.tree.item(item_id, "values")
        col = self.tree.identify_column(event.x)
        col_index = int(col.replace("#", "")) - 1
        col_name = self.tree["columns"][col_index]
        values = self.tree.item(item_id, "values")
        value = values[col_index]
        # 可编辑列
        x, y, width, height = self.tree.bbox(item_id, col)
        entry = tk.Entry(self.tree)
        entry.place(x=x, y=y, width=width, height=height)
        entry.insert(0, value)
        entry.focus()

        def save_edit(event):
            messagebox.showerror(
                parent=self.root,
                title="错误",
                message="该列不允许编辑！"
            )
            return

        entry.bind("<Return>", save_edit)
        entry.bind("<FocusOut>", lambda e: entry.destroy())

    def copy_selected_to_clipboard(self, event=None):
        selected = self.tree.selection()
        if selected:
            values = self.tree.item(selected[0], "values")[0]
            self.root.clipboard_clear()
            self.root.clipboard_append(values)
            print("已复制到剪贴板：", values)


    def select(self):
        selected = self.tree.selection()
        if not selected:
            return
        custName = self.tree.item(selected[0], "values")[0]
        return custName

