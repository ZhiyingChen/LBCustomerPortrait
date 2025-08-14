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
            "arrowkeys", "right_click_popup_menu", "rc_select",
            "rc_insert_row", "rc_delete_row", "rc_insert_column", "rc_delete_column",
            "copy", "cut", "paste", "delete", "undo", "edit_cell"))
        self.sheet.pack(fill='both', expand=True)

        self.sheet.extra_bindings("end_edit_cell", func=self._on_cell_edit)

        for shipto, fo in self.order_data_manager.forecast_order_dict.items():
            self.add_order_display_in_working_sheet(order=fo)

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

    def delete_order(self, order_id, row_index):
        self.order_data_manager.delete_forecast_order_from_fo_list(order_id=order_id)
        del self.order_data_manager.forecast_order_dict[order_id]
        self.sheet.delete_row(row_index)

    def copy_all_to_clipboard(self):
        data = [self.sheet.headers]
        for row_index in range(self.sheet.get_total_rows()):
            row = [self.sheet.get_cell_data(row_index, col_index) for col_index in range(len(self.sheet.headers))]
            data.append(row)
        text = "\n".join(["\t".join(map(str, row)) for row in data])
        self.window.clipboard_clear()
        self.window.clipboard_append(text)
        messagebox.showinfo(title="提示", message="已复制到剪贴板！")

    def _on_close(self):
        self.closed = True
        self.window.destroy()

    def add_order_display_in_working_sheet(self, order: do.Order):
        data = [
            order.order_id,
            order.order_type,
            order.corporate_idn,
            order.product,
            order.shipto,
            order.cust_name,
            order.from_time.strftime("%Y/%m/%d %H:%M"),
            order.to_time.strftime("%Y/%m/%d %H:%M"),
            round(order.drop_kg / 1000, 1),
            order.comments,
            order.target_date.strftime("%Y/%m/%d %H:%M") if isinstance(order.target_date, datetime.datetime) and pd.notnull(order.target_date) else "",
            order.risk_date.strftime("%Y/%m/%d %H:%M") if isinstance(order.risk_date, datetime.datetime) and pd.notnull(order.risk_date) else "",
            order.run_out_date.strftime("%Y/%m/%d %H:%M") if isinstance(order.run_out_date, datetime.datetime) and pd.notnull(order.run_out_date) else "",
        ]
        self.sheet.insert_row(data)

