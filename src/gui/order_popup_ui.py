import time
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import pandas as pd
import logging
import datetime
import os
from .lb_order_data_manager import LBOrderDataManager
from .lb_data_manager import LBDataManager
from .. import domain_object as do
from ..utils import enums, constant


class OrderPopupUI:
    def __init__(
            self,
            root,
            order_data_manager: LBOrderDataManager,
            data_manager: LBDataManager
    ):
        self.closed = False
        self.order_data_manager = order_data_manager
        self.data_manager = data_manager

        self.window = tk.Toplevel(root)
        self.window.title("订单和行程界面")
        self.window.geometry("1200x600")
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)


        # 上方框架
        top_frame = tk.Frame(self.window)
        top_frame.pack(side='top', fill='x', padx=10, pady=5)

        # 按钮容器
        button_container = tk.Frame(top_frame)
        button_container.pack(side='right', fill='x')


        # 一键清除已经创建的订单按钮
        btn_clear_so = tk.Button(button_container, text="一键清除订单",
                                 command=self._clear_all,
                                 bg='#ADD8E6',fg="black", relief="raised", font=("Arial", 10))
        btn_clear_so.pack(side='left', padx=5, pady=5)

        btn_copy_table = tk.Button(button_container, text="复制表格",
                                 command=self.copy_all_to_clipboard,
                                 bg="#009A49",fg="white", relief="raised", font=("Arial", 10))
        btn_copy_table.pack(side='left', padx=5, pady=5)

        # 主体区域
        self.main_frame = tk.Frame(self.window)
        self.main_frame.pack(fill='both', expand=True, padx=10, pady=5)

        # 左侧：Working FO List 和 OO List
        self._create_working_tree()


    # region 创建初始界面
    def _create_working_tree(self):

        self.working_tree = self._create_table(
            self.main_frame, title="Working FO List",
            editable_cols=["From", "To", "KG", "备注"]
        )
        self.working_tree.bind("<Button-3>", lambda e, t=self.working_tree: self._on_right_click(e, t))
        self.working_tree.bind("<Motion>", self.on_motion)  # 绑定鼠标移动事件
        self.working_tree.bind("<Leave>", self.on_leave)  # 绑定鼠标离开事件

        self.tooltip = None  # 初始化 tooltip

        for shipto, fo in self.order_data_manager.forecast_order_dict.items():
            self.add_order_display_in_working_tree(order=fo)



    def _create_table(
            self,
            parent,
            title,
            editable_cols=None
    ):
        columns = ["订单", "类型", "DT","产品", "ShipTo", "客户简称",  "订单从", "订单到", "KG", "备注", "目标充装", "最佳充装", "断气"]
        widths = [80, 20, 30, 30, 60, 80,  110, 110, 40, 80, 110, 110, 110]
        frame = tk.LabelFrame(parent, text=title)
        frame.pack(fill='both', expand=True, pady=5)

        # 垂直容器：表格 + 滚动条
        table_container = tk.Frame(frame)
        table_container.pack(fill='both', expand=True)

        tree = ttk.Treeview(table_container, columns=columns, show="headings", height=6)
        for i, col in enumerate(columns):
            tree.heading(col, text=col)
            tree.column(col, width=widths[i], anchor='center')
        tree.pack(side='left', fill='both', expand=True)

        # 滚动条
        scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')

        # 双击事件：显示客户简称或编辑
        tree.bind("<Double-1>", lambda e, t=tree: self._on_double_click(e, t, editable_cols))

        return tree

    # endregion

    # region 事件处理
    def _on_right_click(self, event, tree):
        if event.num != 3:  # 右键点击
            return

        if not tree.selection():
            return

        confirm = messagebox.askyesno(
            title="提示",
            message="确认删除选中的行吗？",
            parent=self.window
        )
        if confirm:
            self._delete_selected(tree=tree)

    def _on_double_click(self, event, tree, editable_cols):
        item_id = tree.focus()
        if not item_id:
            return

        col = tree.identify_column(event.x)
        col_index = int(col.replace("#", "")) - 1
        col_name = tree["columns"][col_index]
        values = tree.item(item_id, "values")
        value = values[col_index]


        # 可编辑列
        x, y, width, height = tree.bbox(item_id, col)
        entry = tk.Entry(tree)
        entry.place(x=x, y=y, width=width, height=height)
        entry.insert(0, value)
        entry.focus()


        def save_edit(event):
            if not (editable_cols and col_name in editable_cols):
                messagebox.showerror(
                    parent=self.window,
                    title="错误",
                    message="该列不允许编辑！"
                )
                return
            new_value = entry.get()
            values = list(tree.item(item_id, "values"))
            order_id = values[0]
            order = self.order_data_manager.forecast_order_dict[order_id]
            # 校验
            if col_name in ["From", "To"]:
                try:
                    new_value = pd.to_datetime(new_value)
                except ValueError:
                    messagebox.showerror(
                        title="错误",
                        message="时间格式不正确，应该为 %Y/%m/%d %H:%M 格式，请重新输入！",
                        parent=self.window
                    )
                    return
                if (col_name == "From" and new_value >= order.to_time or
                        col_name == "To" and new_value <= order.from_time):
                    messagebox.showerror(
                        title="错误",
                        message="时间范围不正确，应该在订单开始和结束时间之间！",
                        parent=self.window
                    )
                    return


            elif col_name == "KG":
                try:
                    new_value = int(new_value)
                except ValueError:
                    messagebox.showerror(
                        title="错误",
                        message="KG格式不正确，应该为整数，请重新输入！",
                        parent=self.window
                    )
                    return
                max_drop_kg = self.data_manager.get_max_payload_value_by_ship2(ship2=order.shipto)
                if new_value <= 0 or new_value > max_drop_kg:
                    messagebox.showerror(
                        title="错误",
                        message="KG应该大于0且小于等于最大配送量{}！".format(max_drop_kg),
                        parent=self.window
                    )
                    return

            # 1. 更新缓存中该ShipTo的FO订单的信息
            setattr(order, constant.ORDER_ATTR_MAP[col_name], new_value)

            # 2. FOList里面删除原来的行，换成最新修改的一行
            self.order_data_manager.update_forecast_order_in_fo_list(
                order=order
            )
            if col_name in ["From", "To"]:
                new_value = new_value.strftime("%Y/%m/%d %H:%M")

            values[col_index] = new_value
            tree.item(item_id, values=values)
            entry.destroy()

        entry.bind("<Return>", save_edit)
        entry.bind("<FocusOut>", lambda e: entry.destroy())

    def delete_order(self, order_id, tree, item):
        #   FOList里面删除原来的行
        self.order_data_manager.delete_forecast_order_from_fo_list(
            order_id=order_id
        )
        #   界面里面删除本行
        tree.delete(item)

        #   删除缓存中该ShipTo的FO订单的信息
        del self.order_data_manager.forecast_order_dict[order_id]

    def _delete_selected(self, tree):
        selected = tree.selection()
        for item in selected:
            order_id = tree.item(item, "values")[0]
            self.delete_order(order_id, tree, item)


    def _clear_all(self):
        """
         点击“清空所有有SO号的行”按钮，清空所有有SO号的行
        """
        confirm = messagebox.askyesno(
            title="提示",
            message="确认清空所有有的行吗？",
            parent=self.window
        )
        if not confirm:
            return

        for item in self.working_tree.get_children():
            values = list(self.working_tree.item(item, "values"))
            order_id = values[0]
            self.delete_order(order_id, self.working_tree, item)


    def copy_all_to_clipboard(self):
        all_items = self.working_tree.get_children()
        if not all_items:
            return

        # 获取列名
        headers = self.working_tree["columns"]
        data = [headers]

        # 获取所有行数据
        for item in all_items:
            row = self.working_tree.item(item, "values")
            data.append(row)

        # 拼接为制表符分隔的字符串
        text = "\n".join(["\t".join(map(str, row)) for row in data])

        # 复制到剪贴板
        self.window.clipboard_clear()
        self.window.clipboard_append(text)
        messagebox.showinfo(
            title="提示",
            message="已复制到剪贴板！",
        )


    def _on_close(self):
        self.closed = True
        self.window.destroy()

    def add_order_display_in_working_tree(
            self, order: do.Order
    ):
        data = [
            order.order_id,
            order.order_type,
            order.corporate_idn,
            order.product,
            order.shipto,
            order.cust_name,
            order.from_time.strftime("%Y/%m/%d %H:%M"),
            order.to_time.strftime("%Y/%m/%d %H:%M"),
            int(order.drop_kg),
            order.comments,
            order.target_date.strftime("%Y/%m/%d %H:%M") if isinstance(order.target_date, datetime.datetime) and pd.notnull(
                order.target_date) else "",
            order.risk_date.strftime("%Y/%m/%d %H:%M") if isinstance(order.risk_date, datetime.datetime) and pd.notnull(
                order.risk_date) else "",
            order.run_out_date.strftime("%Y/%m/%d %H:%M") if isinstance(order.run_out_date, datetime.datetime) and pd.notnull(
                order.run_out_date) else "",
        ]
        self.working_tree.insert("", "end", values=tuple(data))

    def on_motion(self, event):
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None

        item_id = self.working_tree.identify_row(event.y)
        col = self.working_tree.identify_column(event.x)
        if item_id and col:
            col_index = int(col.replace("#", "")) - 1
            values = self.working_tree.item(item_id, "values")
            if col_index < len(values):
                value = values[col_index]
                if len(value) > 10:  # 如果内容较长，显示 tooltip
                    self.tooltip = tk.Toplevel(self.working_tree)
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