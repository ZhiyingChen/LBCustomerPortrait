import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

class SimpleTable:
    def __init__(self, parent, columns, col_widths=None, height=10, col_stretch=None, show_header=True):
        self.root = parent
        self.frame = tk.Frame(parent)
        self.frame.pack(fill="both", expand=True)  # 父容器填充扩展

        self.tree = ttk.Treeview(
            self.frame,
            columns=columns,
            show="headings" if show_header else "tree",
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
        self.tree.bind("<Motion>", self.on_motion)  # 绑定鼠标移动事件
        self.tree.bind("<Leave>", self.on_leave)  # 绑定鼠标离开事件
        # 添加右键菜单
        self.menu = tk.Menu(self.tree, tearoff=0)
        self.menu.add_command(label="复制整张表格", command=self.copy_all_to_clipboard)

        # 绑定右键事件
        self.tree.bind("<Button-3>", self.show_context_menu)

        self.tooltip = None  # 初始化 tooltip

    def insert_rows(self, rows, make_red=False, align='center'):
        """ 插入多行数据，清空旧数据 """
        self.tree.delete(*self.tree.get_children())

        if make_red:
            # 配置一个名为 'red' 的标签样式
            self.tree.tag_configure('red', foreground='red')

        # 调整列的对齐方式
        for col in self.tree["columns"]:
            self.tree.column(col, anchor=align)

        # 配置标签样式
        for row in rows:
            tags = []
            if make_red:
                tags.append('red')

            self.tree.insert("", "end", values=row, tags=tuple(tags))

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

    def on_motion(self, event):
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None

        item_id = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)
        if item_id and col:
            col_index = int(col.replace("#", "")) - 1
            values = self.tree.item(item_id, "values")
            if col_index < len(values):
                value = values[col_index]
                if len(value) > 17:  # 如果内容较长，显示 tooltip
                    self.tooltip = tk.Toplevel(self.tree)
                    self.tooltip.withdraw()
                    self.tooltip.overrideredirect(True)
                    label = tk.Label(self.tooltip, text=value, background="yellow", relief='solid', borderwidth=1)
                    label.pack()
                    self.tooltip.geometry(f"+{event.x_root-len(value) * 8}+{event.y_root+10}")
                    self.tooltip.deiconify()

    def on_leave(self, event):
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None

    def copy_all_to_clipboard(self):
        all_items = self.tree.get_children()
        if not all_items:
            return

        # 获取列名
        headers = self.tree["columns"]
        data = [headers]

        # 获取所有行数据
        for item in all_items:
            row = self.tree.item(item, "values")
            data.append(row)

        # 拼接为制表符分隔的字符串
        text = "\n".join(["\t".join(map(str, row)) for row in data])

        # 复制到剪贴板
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        print("整张表格已复制到剪贴板")

    def show_context_menu(self, event):
        try:
            self.menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.menu.grab_release()
