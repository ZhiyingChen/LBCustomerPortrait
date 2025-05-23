import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import pandas as pd
import logging
import datetime
import os
from .lb_order_data_manager import LBOrderDataManager
from .. import domain_object as do
from ..utils import enums, constant
from ..rpa.main import BuildOrder
from ..utils import functions as func
from ..utils.email_report import outlook_sender

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
        self.window.geometry("1000x600")
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        # 上方框架
        top_frame = tk.Frame(self.window)
        top_frame.pack(side='top', fill='x', padx=10, pady=5)

        # 上次修改时间标签
        self.last_modified_label = tk.Label(top_frame, text=f"上次修改时间: ", anchor='w', fg='blue')
        self.last_modified_label.pack(side='left', fill='x', expand=True)
        self.update_last_modified_time()
        # 创建按钮
        btn_clear = tk.Button(top_frame, text="一键在LBShell建立SO订单",
                              command=lambda: self._send_data_to_lb_shell(self.working_tree))
        btn_clear.pack(side='right', padx=5)

        # 主体区域
        self.main_frame = tk.Frame(self.window)
        self.main_frame.pack(fill='both', expand=True, padx=10, pady=5)

        # 左侧：Working FO List 和 OO List
        self._create_working_tree()

    def _run_rpa(self):
        valid_order_list = {
            k: v for k, v in self.order_data_manager.forecast_order_dict.items()
            if v.is_in_trip_draft and not v.has_valid_so_number
        }
        """
            对于已经有SO号的订单，或者不在行程草稿中的订单，不进行RPA操作 
        """
        if len(valid_order_list) == 0:
            messagebox.showerror(
                title="错误",
                message="没有需要建立SO订单的有效订单数据，请先添加！",
                parent=self.window
            )
            return

        rpa_order_list = []
        for o_id, order in valid_order_list.items():
            rpa_order_list.append(
                {
                    'OrderId': order.order_id,
                    'LocNum': order.shipto,
                    'from': order.from_time.strftime('%d/%m/%y %H:%M'),
                    'to': order.to_time.strftime('%d/%m/%y %H:%M'),
                    'kg': str(int(order.drop_kg)),
                    'comment': order.comments,
                    'sonumber': ''
                }
            )

        pic_dir = r'\\shangnt\lbshell\PUAPI\PU_program\automation\rpa_pic'
        lbshell_exe_name = "LbShell32.exe"
        lb_shell_path = r'C:\Program Files (x86)'  # 需要替换为你的LBshell所在c盘folder,大部分无需替换。

        result_rpa_order_list = BuildOrder().get_sonumber(
            LBversion='cn',
            path_pic=pic_dir,
            file_name=lbshell_exe_name,
            search_path=lb_shell_path,
            data_list=rpa_order_list
        )

        return result_rpa_order_list

    def _fake_run_rpa(self):
        if len(self.order_data_manager.forecast_order_dict) == 0:
            messagebox.showerror(
                title="错误",
                message="没有订单数据，请先添加订单！",
                parent=self.window
            )
            return

        rpa_order_list = []
        for o_id, order in self.order_data_manager.forecast_order_dict.items():
            if order.has_valid_so_number or not order.is_in_trip_draft:
                """
                对于已经有SO号的订单，或者不在行程草稿中的订单，不进行RPA操作 
                """
                continue
            rpa_order_list.append(
                {
                    'OrderId': order.order_id,
                    'LocNum': order.shipto,
                    'from': order.from_time.strftime('%d/%m/%y %H:%M'),
                    'to': order.to_time.strftime('%d/%m/%y %H:%M'),
                    'kg': str(int(order.drop_kg)),
                    'comment': order.comments,
                    'sonumber': 'SO123456'
                }
            )
        return rpa_order_list



    def update_rpa_result_info(self, result_rpa_order_list):
        if not isinstance(result_rpa_order_list, list) or len(result_rpa_order_list) == 0:
            return

        for order_info in result_rpa_order_list:
            # 完成RPA之后，更新FO缓存中的SONUMBER
            order_id = order_info['OrderId']
            so_number = order_info['sonumber']
            order = self.order_data_manager.forecast_order_dict[order_id]
            order.complete_so_number(so_number=so_number)
            # 更新 FOList 和 FORecordList 中的SONUMBER
            if not order.has_valid_so_number:
                continue
            self.order_data_manager.update_so_number_in_fo_list(order_id=order_id, so_number=order.so_number)
            self.order_data_manager.update_so_number_in_fo_record_list(order_id=order_id, so_number=order.so_number)



    def _send_result_to_email(self, result_rpa_order_list):
        user_name = func.get_user_name()

        # 输出到excel做几路
        result_df = pd.DataFrame(result_rpa_order_list)

        # Add timestamp column
        result_df['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        result_df['是否成功'] = result_df['sonumber'].apply(
            lambda x: '成功' if isinstance(x, str) and x.startswith('SO') else '失败')

        # Define the output file path
        result_file_path = './output/rpa_result.xlsx'

        # Check if the file exists
        if os.path.exists(result_file_path):
            # Append to existing file
            with pd.ExcelWriter(result_file_path, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                startrow = writer.sheets['Sheet1'].max_row
                if startrow == 0:
                    result_df.to_excel(writer, index=False, sheet_name='Sheet1', startrow=startrow)
                else:
                    result_df.to_excel(writer, index=False, sheet_name='Sheet1', startrow=startrow, header=False)

        else:
            # Create a new file
            result_df.to_excel(result_file_path, index=False, engine='openpyxl')


        emailer = '{}@airproducts.com;chenz32@airproducts.com;zhaol12@airproducts.com'.format(user_name)

        success_df = result_df[result_df['是否成功'] == '成功']
        fail_df = result_df[result_df['是否成功'] == '失败']

        total_number = len(result_df)
        success_number = len(success_df)
        fail_number = len(fail_df)

        now = datetime.datetime.now()

        message_subject = "LBShell RPA 操作结果 {}: 失败{}条".format(now.strftime("%Y-%m-%d %H:%M"), fail_number)


        message_body = (
            "以下是RPA操作结果表格，总共{}条订单：\n\n "
            "其中， sonumber 列为RPA操作生成的订单号，如果该订单号不以SO开头，则表示RPA操作失败。\n\n "
            "失败订单{}条，失败订单如下：\n\n "
            "{}  \n\n"
            "成功订单{}条，成功订单如下：\n\n "
            "{}  \n\n".format(
                total_number,
            fail_number,
            fail_df.to_html(index=False),
            success_number,
            success_df.to_html(index=False)
        )
        )

        outlook_sender(sender=emailer, addressee=emailer, message_subject=message_subject, message_body=message_body)

    # region 创建初始界面
    def _create_working_tree(self):
        insert_data = []
        for shipto, fo in self.order_data_manager.forecast_order_dict.items():
            data = [
                fo.order_id,
                fo.shipto,
                fo.cust_name,
                fo.product,
                fo.from_time.strftime("%Y/%m/%d %H:%M"),
                fo.to_time.strftime("%Y/%m/%d %H:%M"),
                int(fo.drop_kg),
                fo.comments,
                "1" if fo.is_in_trip_draft else "",
                fo.so_number
            ]
            insert_data.append(data)
        self.working_tree = self._create_table(
            self.main_frame, title="Working FO List",
            editable_cols=["From", "To", "KG", "备注", "行程草稿？"],
            insert_data=insert_data
        )
        self.working_tree.bind("<Button-3>", lambda e, t=self.working_tree: self._on_right_click(e, t))

    def _create_table(
            self,
            parent,
            title,
            editable_cols=None,
            insert_data=None
    ):
        columns = ["临时Id", "ShipTo", "客户简称", "产品", "From", "To", "KG", "备注", "行程草稿？", "SO号"]
        widths = [100, 70, 80, 40, 110, 110, 60, 80, 30, 100]
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
                if new_value <= 0:
                    messagebox.showerror(
                        title="错误",
                        message="KG应该大于0！",
                        parent=self.window
                    )
                    return
            elif col_name == "行程草稿？":
                if str(new_value) not in ["1", ""]:
                    messagebox.showerror(
                        title="错误",
                        message="行程草稿？应该为 '1' 或 '' ！",
                        parent=self.window
                    )
                    return
                if new_value == "1":
                    new_value = 1
                else:
                    new_value = 0

            # 1. 更新缓存中该ShipTo的FO订单的信息
            setattr(order, constant.ORDER_ATTR_MAP[col_name], new_value)

            # 2. FOList里面删除原来的行，换成最新修改的一行
            self.order_data_manager.update_forecast_order_in_fo_list(
                order=order
            )
            if col_name != "行程草稿？":
                # 3. FORecordList 增加一行 EditType 为Modify 的信息
                self.order_data_manager.insert_order_record_in_fo_record_list(
                    order=order,
                    edit_type=enums.EditType.Modify
                )
            if col_name in ["From", "To"]:
                new_value = new_value.strftime("%Y/%m/%d %H:%M")
            if col_name == "行程草稿？":
                new_value = "1" if new_value == 1 else ""
            values[col_index] = new_value
            tree.item(item_id, values=values)
            entry.destroy()
            self.update_last_modified_time()


        entry.bind("<Return>", save_edit)
        entry.bind("<FocusOut>", lambda e: entry.destroy())

    def _delete_selected(self, tree):
        selected = tree.selection()
        for item in selected:
            order_id = tree.item(item, "values")[0]
            #   FOList里面删除原来的行
            self.order_data_manager.delete_forecast_order_from_fo_list(
                order_id=order_id
            )

            #   FORecordList 增加一行 EditType 为 Delete 的信息
            self.order_data_manager.insert_order_record_in_fo_record_list(
                order=self.order_data_manager.forecast_order_dict[order_id],
                edit_type=enums.EditType.Delete
            )
            #   界面里面删除本行
            tree.delete(item)

            #   删除缓存中该ShipTo的FO订单的信息
            del self.order_data_manager.forecast_order_dict[order_id]

        self.update_last_modified_time()

    def update_last_modified_time(self):
        last_modified_time = self.order_data_manager.get_last_modified_time()
        self.last_modified_label.config(text=f"上次修改时间：{last_modified_time}")

    def _send_data_to_lb_shell(self, tree):
        confirm = messagebox.askyesno(
            title="提示",
            message="确认一键在LBShell建立SO订单吗？"
                    "\n（只有勾选行程草稿的订单且没有SO号的订单会被处理）\n"
                    "如果确认，则在建完所有订单前，请勿使用鼠标，"
                    "请耐心等待。",
            parent=self.window
        )
        if not confirm:
            return

        # 对于没有SONUMBER且勾选行程草稿的订单执行RPA功能，完成之后更新缓存中的SONUMBER
        rpa_result_lt = self._run_rpa()
        # rpa_result_lt = self._fake_run_rpa()
        self.update_rpa_result_info(rpa_result_lt)

        # 把更新后的SONUMBER 展示在界面
        self._update_so_number_in_working_tree()

        self._send_result_to_email(rpa_result_lt)

    def _update_so_number_in_working_tree(self):
        """
         把更新后的SONUMBER展示在界面
        """
        for item in self.working_tree.get_children():
            values = list(self.working_tree.item(item, "values"))
            order_id = values[0]
            order = self.order_data_manager.forecast_order_dict[order_id]
            so_number = order.so_number
            values[-1] = so_number
            self.working_tree.item(item, values=values)



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
            order.order_id,
            order.shipto,
            order.cust_name,
            order.product,
            order.from_time.strftime("%Y/%m/%d %H:%M"),
            order.to_time.strftime("%Y/%m/%d %H:%M"),
            int(order.drop_kg),
            order.comments
        ]
        self.working_tree.insert("", "end", values=tuple(data))
        self.update_last_modified_time()
