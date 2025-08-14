import tkinter as tk
from tkinter import messagebox
import pandas as pd
import datetime
from tksheet import Sheet
from .lb_order_data_manager import LBOrderDataManager
from .lb_data_manager import LBDataManager
from .. import domain_object as do
from ..utils import enums, constant


class OrderPopupUI:
    def __init__(self, root, order_data_manager: LBOrderDataManager, data_manager: LBDataManager):
        self.closed = False
        self.order_data_manager = order_data_manager
        self.data_manager = data_manager

        self.window = tk.Toplevel(root)
        self.window.title("订单和行程界面")
        self.window.geometry("1200x600")
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        top_frame = tk.Frame(self.window)
        top_frame.pack(side='top', fill='x', padx=10, pady=5)

        button_container = tk.Frame(top_frame)
        button_container.pack(side='right', fill='x')

        btn_clear_so = tk.Button(button_container, text="一键清除订单",
                                 command=self._clear_all,
                                 bg='#ADD8E6', fg="black", relief="raised", font=("Arial", 10))
        btn_clear_so.pack(side='left', padx=5, pady=5)

        btn_copy_table = tk.Button(button_container, text="复制表格",
                                   command=self.copy_all_to_clipboard,
                                   bg="#009A49", fg="white", relief="raised", font=("Arial", 10))
        btn_copy_table.pack(side='left', padx=5, pady=5)

        self.main_frame = tk.Frame(self.window)
        self.main_frame.pack(fill='both', expand=True, padx=10, pady=5)

        self._create_working_sheet()

        btn_hide_column = tk.Button(button_container, text="隐藏列",
                                    command=self._hide_columns,
                                    bg="#FFA07A", fg="black", relief="raised", font=("Arial", 10))
        btn_hide_column.pack(side='left', padx=5, pady=5)

        btn_show_column = tk.Button(button_container, text="显示列",
                                    command=self._show_columns,
                                    bg="#90EE90", fg="black", relief="raised", font=("Arial", 10))
        btn_show_column.pack(side='left', padx=5, pady=5)
        self.hidden_column_indices = []

    # region 创建工作表
    def _create_working_sheet(self):
        columns = ["订单", "类型", "DT", "产品", "ShipTo", "客户简称", "订单从", "订单到",
                   "吨", "备注", "目标充装", "最佳充装", "断气"]
        self.sheet = Sheet(self.main_frame,
                           headers=columns,
                           show_x_scrollbar=True,
                           show_y_scrollbar=True)
        self.sheet.enable_bindings((
            "single_select", "row_select", "column_select", "drag_select",
            "column_drag_and_drop", "row_drag_and_drop",
            "right_click_popup_menu", "rc_select",  "copy", "edit_cell"))
        self.sheet.pack(fill='both', expand=True)

        self.sheet.extra_bindings("end_edit_cell", func=self._on_cell_edit)
        self.sheet.popup_menu_add_command("删除选中的行", func=self._delete_selected_row)

        for shipto, fo in self.order_data_manager.forecast_order_dict.items():
            self.add_order_display_in_working_sheet(order=fo)
    # endregion

    # region 事件处理

    def _hide_columns(self):
        headers = self.sheet.headers()
        hidden_col_names = [headers[i] for i in self.hidden_column_indices]
        selected = self._ask_multiple_columns([v for v in headers if v not in hidden_col_names], "选择要隐藏的列")
        if selected:
            indices = [headers.index(name) for name in selected]
            self.sheet.hide_columns(indices)
            self.hidden_column_indices.extend(indices)

    def _show_columns(self):
        headers = self.sheet.headers()
        if not self.hidden_column_indices:
            messagebox.showinfo(title="提示", message="没有隐藏的列")
            return
        hidden_names = [headers[i] for i in self.hidden_column_indices]
        selected = self._ask_multiple_columns(hidden_names, "选择要显示的列")
        if selected:
            indices = [headers.index(name) for name in selected]
            self.sheet.show_columns(indices)
            self.hidden_column_indices = [i for i in self.hidden_column_indices if i not in indices]

    def _ask_multiple_columns(self, options, title):
        popup = tk.Toplevel(self.window)
        popup.title(title)
        popup.geometry("300x600")
        popup.transient(self.window)
        popup.grab_set()

        selected_vars = []
        for opt in options:
            var = tk.BooleanVar()
            chk = tk.Checkbutton(popup, text=opt, variable=var)
            chk.pack(anchor='w')
            selected_vars.append((opt, var))

        result = []

        def confirm():
            for name, var in selected_vars:
                if var.get():
                    result.append(name)
            popup.destroy()

        btn = tk.Button(popup, text="确认", command=confirm)
        btn.pack(pady=10)

        popup.wait_window()
        return result

    def _on_cell_edit(self, event):
        row, column, value = event["row"], event["column"], event["value"]
        col_index = int(column)
        col_name = self.sheet.headers()[col_index]

        order_id = self.sheet.get_cell_data(row, 0)
        order = self.order_data_manager.forecast_order_dict.get(order_id)
        if not order:
            return

        try:
            if col_name in ["订单从", "订单到"]:
                new_value = pd.to_datetime(value)
                if pd.isnull(new_value) or not isinstance(new_value, datetime.datetime):
                    raise ValueError("时间格式不正确")
                if (col_name == "订单从" and new_value >= order.to_time) or \
                        (col_name == "订单到" and new_value <= order.from_time):
                    raise ValueError("时间范围不正确")
                setattr(order, constant.ORDER_ATTR_MAP[col_name], new_value)
                value = new_value.strftime("%Y/%m/%d %H:%M")

            elif col_name == "吨":
                new_value = float(value)
                max_drop_kg = self.data_manager.get_max_payload_value_by_ship2(order.shipto) / 1000
                if not (0 < new_value <= max_drop_kg):
                    raise ValueError("吨应该大于0且小于等于最大配送量")
                setattr(order, constant.ORDER_ATTR_MAP[col_name], new_value * 1000)

            elif col_name == "备注":
                setattr(order, constant.ORDER_ATTR_MAP[col_name], value)

            else:
                raise ValueError(f"{col_name}列不支持编辑")

            self.order_data_manager.update_forecast_order_in_fo_list(order)
            self.sheet.set_cell_data(row, column, value)

        except Exception as e:
            original_value = getattr(order, constant.ORDER_ATTR_MAP.get(col_name, ""), value)
            if isinstance(original_value, datetime.datetime):
                original_value = original_value.strftime("%Y/%m/%d %H:%M")
            self.sheet.set_cell_data(row, column, original_value)
            messagebox.showerror(title="错误", message=str(e), parent=self.window)

    def _clear_all(self):
        confirm = messagebox.askyesno(title="提示", message="确认清空所有有的行吗？", parent=self.window)
        if not confirm:
            return
        for row_index in reversed(range(self.sheet.get_total_rows())):
            order_id = self.sheet.get_cell_data(row_index, 0)
            self.delete_order(order_id, row_index)

    def copy_all_to_clipboard(self):
        headers = self.sheet.headers()
        col_num = len(headers)
        data = [headers]
        for row_index in range(self.sheet.get_total_rows()):
            row = [self.sheet.get_cell_data(row_index, col_index) for col_index in range(col_num)]
            data.append(row)
        text = "\n".join(["\t".join(map(str, row)) for row in data])
        self.window.clipboard_clear()
        self.window.clipboard_append(text)
        messagebox.showinfo(title="提示", message="已复制到剪贴板！")

    def _delete_selected_row(self, event=None):
        selected_row = self.sheet.get_selected_rows()
        if not selected_row:
            return
        order_col = self.sheet.headers().index("订单")
        selected_order_lt = [
            (self.sheet.get_cell_data(row_index, order_col), row_index)
            for row_index in selected_row
        ]
        confirm = messagebox.askyesno(title="确认删除", message="确认删除选中行订单吗？", parent=self.window)
        if confirm:
            for order_id, row_index in selected_order_lt:
                self.delete_order(order_id, row_index)

    def _on_close(self):
        self.closed = True
        self.window.destroy()
    # endregion

    # region 订单相关操作
    def delete_order(self, order_id, row_index):
        self.order_data_manager.delete_forecast_order_from_fo_list(order_id=order_id)
        del self.order_data_manager.forecast_order_dict[order_id]
        self.sheet.delete_row(row_index)

    def add_order_display_in_working_sheet(self, order: do.Order):
        # 需要根据当前的header顺序去自动调节
        headers = self.sheet.headers()

        data = []
        for header in headers:
            value = getattr(order, constant.ORDER_ATTR_MAP.get(header, ""), "")
            if isinstance(value, datetime.datetime):
                value = value.strftime("%Y/%m/%d %H:%M")
            data.append(
                value
            )

        self.sheet.insert_row(data)
    # endregion

