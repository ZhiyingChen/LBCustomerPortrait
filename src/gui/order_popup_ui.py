import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import pandas as pd
from .lb_order_data_manager import LBOrderDataManager
from .. import domain_object as do
from ..utils import enums, constant


class OrderPopupUI:
    def __init__(
            self,
            root,
            order_data_manager: LBOrderDataManager
    ):
        self.closed = False
        self.order_data_manager = order_data_manager

        self.window = tk.Toplevel(root)
        self.window.title("订单和行程界面")
        self.window.geometry("1400x800")
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        # 中间推荐显示标签
        self.recommendation_var = tk.StringVar(value="当前选中客户：")

        # 主体区域
        self.main_frame = tk.Frame(self.window)
        self.main_frame.pack(fill='both', expand=True, padx=10, pady=5)

        # 左侧：Working FO List 和 OO List
        self.left_frame = tk.Frame(self.main_frame)
        self.left_frame.pack(side='left', fill='y')

        self._create_working_tree()
        self._create_oo_tree()

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

    # region 创建初始界面
    def _create_working_tree(self):
        insert_data = []
        for shipto, fo in self.order_data_manager.forecast_order_dict.items():
            data = [
                fo.shipto,
                fo.cust_name,
                fo.product,
                fo.from_time.strftime("%Y/%m/%d %H:%M"),
                fo.to_time.strftime("%Y/%m/%d %H:%M"),
                int(fo.drop_kg),
                fo.comments,
                "是" if fo.is_in_trip() else "否"
            ]
            insert_data.append(data)
        self.working_tree = self._create_table(
            self.left_frame, title="Working FO List",
            editable_cols=["From", "To", "KG", "备注"], add_so_button=True,
            insert_data=insert_data
        )
        self.working_tree.bind("<Button-3>", lambda e, t=self.working_tree: self._on_right_click(e, t))

    def _create_oo_tree(self):
        insert_data = []
        for shipto, oo in self.order_data_manager.order_only_dict.items():
            data = [
                oo.shipto,
                oo.cust_name,
                oo.product,
                oo.from_time.strftime("%Y/%m/%d %H:%M"),
                oo.to_time.strftime("%Y/%m/%d %H:%M"),
                int(oo.drop_kg),
                oo.comments,
                 "是" if oo.is_in_trip() else "否"
            ]
            insert_data.append(data)

        self.oo_tree = self._create_table(
            self.left_frame, title="OO List", insert_data=insert_data
        )

    def _create_table(
            self,
            parent,
            title,
            editable_cols=None,
            add_so_button=False,
            insert_data=None
    ):
        columns = ["ShipTo", "客户简称", "产品", "From", "To", "KG", "备注", "InTrip"]
        widths = [70, 80, 40, 110, 110, 60, 80, 40]
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

        # 操作按钮区域
        if add_so_button:
            btn_frame = tk.Frame(frame)
            btn_frame.pack(pady=5)

            if add_so_button:
                btn_clear = tk.Button(btn_frame, text="一键在LBShell建立SO订单",
                                      command=lambda: self._send_data_to_lb_shell(tree))
                btn_clear.pack(side='left', padx=5)

        # 示例数据
        if insert_data:
            for data in insert_data:
                tree.insert("", "end", values=tuple(data))

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
            parent = self.window
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
                shipto = values[0]
                order = self.order_data_manager.forecast_order_dict[shipto]
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
                        new_value = float(new_value)
                    except ValueError:
                        messagebox.showerror(
                            title="错误",
                            message="KG格式不正确，应该为数值类型，请重新输入！",
                            parent=self.window
                        )
                        return


                # 1. 更新缓存中该ShipTo的FO订单的信息
                setattr(order, constant.ORDER_ATTR_MAP[col_name], new_value)

                # 2. FOList里面删除原来的行，换成最新修改的一行
                self.order_data_manager.update_forecast_order_in_fo_list(
                    order=order
                )
                # 3. FORecordList 增加一行 EditType 为Modify 的信息
                self.order_data_manager.insert_order_record_in_fo_record_list(
                    order=order,
                    edit_type=enums.EditType.Modify
                )

                values[col_index] = new_value
                tree.item(item_id, values=values)
                entry.destroy()

            entry.bind("<Return>", save_edit)
            entry.bind("<FocusOut>", lambda e: entry.destroy())

    def _delete_selected(self, tree):
        selected = tree.selection()
        for item in selected:
            shipto = tree.item(item, "values")[0]
            #   FOList里面删除原来的行
            self.order_data_manager.delete_forecast_order_from_fo_list(
                shipto=shipto
            )

            #   FORecordList 增加一行 EditType 为 Delete 的信息
            self.order_data_manager.insert_order_record_in_fo_record_list(
                order=self.order_data_manager.forecast_order_dict[shipto],
                edit_type=enums.EditType.Delete
            )
            #   界面里面删除本行
            tree.delete(item)

            #   删除缓存中该ShipTo的FO订单的信息
            del self.order_data_manager.forecast_order_dict[shipto]

    def _send_data_to_lb_shell(self, tree):
        # todo: 执行RPA功能

        # todo: 将FOList和RecordList全部上传至SharepointList

        # 清空FOList和RecordList, 清空缓存中的所有FO订单信息
        self.order_data_manager.remove_all_forecast_orders()

        # 把FO界面上的信息清空
        self._clear_all_rows(tree)


    def _clear_all_rows(self, tree):
        for item in tree.get_children():
            tree.delete(item)


    def _on_close(self):
        self.closed = True
        self.window.destroy()

    def add_order_display_in_working_tree(
            self, order: do.Order
    ):
        data = [
            order.shipto,
            order.cust_name,
            order.product,
            order.from_time.strftime("%Y/%m/%d %H:%M"),
            order.to_time.strftime("%Y/%m/%d %H:%M"),
            int(order.drop_kg),
            order.comments,
            "是" if order.is_in_trip() else "否"
        ]
        self.working_tree.insert("", "end", values=tuple(data))
    # endregion

