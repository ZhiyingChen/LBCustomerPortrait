import tkinter as tk
from tkinter import ttk

class OrderPopupUI:
    def __init__(self, root):
        self.window = tk.Toplevel(root)
        self.window.title("订单和行程界面")
        self.window.geometry("1400x800")

        # 中间推荐显示标签
        self.recommendation_var = tk.StringVar(value="当前选中客户：")

        # 主体区域
        self.main_frame = tk.Frame(self.window)
        self.main_frame.pack(fill='both', expand=True, padx=10, pady=5)

        # 左侧：Working FO List 和 OO List
        self.left_frame = tk.Frame(self.main_frame)
        self.left_frame.pack(side='left', fill='y')

        columns = ["ShipTo", "客户简称", "产品", "From", "To", "KG", "备注", "安排Trip"]
        self.working_tree = self._create_table(
            self.left_frame, title="Working FO List", columns=columns,
            editable_cols=["From", "To", "KG", "备注"], deletable=True, add_so_button=True
        )
        self.oo_tree = self._create_table(
            self.left_frame, title="OO List", columns=columns
        )

        # 中间：Single Ship To Trip Recommendation
        self.center_frame = tk.Frame(self.main_frame)
        self.center_frame.pack(side='left', fill='both', expand=True)
        tk.Label(self.center_frame, text="Single ShipTo Trip Recommendation").pack()
        self.recommendation_label = tk.Label(
            self.center_frame, textvariable=self.recommendation_var,
            fg="blue", font=("Arial", 12)
        )
        self.recommendation_label.pack(pady=10)

        # 右侧：Trip Draft（占位）
        self.right_frame = tk.Frame(self.main_frame)
        self.right_frame.pack(side='right', fill='both', expand=True)
        tk.Label(self.right_frame, text="Total Trip Draft").pack()

    def _create_table(self, parent, title, columns, editable_cols=None, deletable=False, add_so_button=False):
        frame = tk.LabelFrame(parent, text=title)
        frame.pack(fill='both', expand=True, pady=5)

        # 垂直容器：表格 + 滚动条
        table_container = tk.Frame(frame)
        table_container.pack(fill='both', expand=True)

        tree = ttk.Treeview(table_container, columns=columns, show="headings", height=6)
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=60, anchor='center')
        tree.pack(side='left', fill='both', expand=True)

        # 滚动条
        scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')

        # 双击事件：显示客户简称或编辑
        tree.bind("<Double-1>", lambda e, t=tree: self._on_double_click(e, t, editable_cols))

        # 操作按钮区域
        if deletable or add_so_button:
            btn_frame = tk.Frame(frame)
            btn_frame.pack(pady=5)

            if deletable:
                btn_del = tk.Button(btn_frame, text="删除选中行", command=lambda: self._delete_selected(tree))
                btn_del.pack(side='left', padx=5)

            if add_so_button:
                btn_clear = tk.Button(btn_frame, text="一键在LBShell建立SO订单", command=lambda: self._clear_all_rows(tree))
                btn_clear.pack(side='left', padx=5)

        # 示例数据
        for i in range(3):
            tree.insert("", "end", values=(f"{i+1}", f"客户{i+1}", "产品A", "HK", "SH", "100", "无", "否"))

        return tree

    def _on_double_click(self, event, tree, editable_cols):
        item_id = tree.focus()
        if not item_id:
            return
        col = tree.identify_column(event.x)
        col_index = int(col.replace("#", "")) - 1
        col_name = tree["columns"][col_index]
        value = tree.item(item_id, "values")[col_index]

        # 显示客户简称
        if col_name == "客户简称":
            self.recommendation_var.set(f"当前选中客户：{value}")
            return

        # 可编辑列
        if editable_cols and col_name in editable_cols:
            x, y, width, height = tree.bbox(item_id, col)
            entry = tk.Entry(tree)
            entry.place(x=x, y=y, width=width, height=height)
            entry.insert(0, value)
            entry.focus()

            def save_edit(event):
                new_value = entry.get()
                values = list(tree.item(item_id, "values"))
                values[col_index] = new_value
                tree.item(item_id, values=values)
                entry.destroy()

            entry.bind("<Return>", save_edit)
            entry.bind("<FocusOut>", lambda e: entry.destroy())

    def _delete_selected(self, tree):
        selected = tree.selection()
        for item in selected:
            tree.delete(item)

    def _clear_all_rows(self, tree):
        for item in tree.get_children():
            tree.delete(item)
