import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import pandas as pd
import datetime
from tksheet import Sheet
from tkcalendar import DateEntry
from typing import List, Dict
from .lb_order_data_manager import LBOrderDataManager
from .lb_data_manager import LBDataManager
from .. import domain_object as do
from ..utils import enums, constant
from ..utils.field import FOTableHeader as foh


class OrderPopupUI:
    def __init__(self, root, order_data_manager: LBOrderDataManager, data_manager: LBDataManager):
        self.closed = False
        self.order_data_manager = order_data_manager
        self.data_manager = data_manager

        self.window = tk.Toplevel(root)
        self.window.title("订单和行程界面")
        self.window.geometry("1200x600")
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        self._decorate_top_frame()

        self._create_working_sheet()

    # region 创建操作区域
    def _decorate_top_frame(self):
        top_frame = tk.Frame(self.window)
        top_frame.pack(side='top', fill='x', padx=10, pady=5)

        # ========== 1. 筛选模块 ==========
        filter_frame = tk.LabelFrame(top_frame, text="筛选")
        filter_frame.pack(side='left', padx=10)

        # 在 filter_frame 中添加
        tk.Label(filter_frame, text="开始日期").grid(row=0, column=0, padx=5)
        self.start_date_var = tk.StringVar()
        self.start_date_picker = DateEntry(filter_frame, textvariable=self.start_date_var, date_pattern="yyyy-mm-dd")
        self.start_date_picker.grid(row=0, column=1, padx=5)

        tk.Label(filter_frame, text="结束日期").grid(row=0, column=2, padx=5)
        self.end_date_var = tk.StringVar()
        self.end_date_picker = DateEntry(filter_frame, textvariable=self.end_date_var, date_pattern="yyyy-mm-dd")
        self.end_date_picker.grid(row=0, column=3, padx=5)

        self.filter_vars = {
            foh.corporate_id: tk.StringVar(value="全部"),
            foh.product: tk.StringVar(value="全部"),
        }

        for idx, label_text in enumerate([foh.corporate_id, foh.product]):
            tk.Label(filter_frame, text=label_text).grid(row=1, column=idx * 2, padx=5)
            combo = ttk.Combobox(filter_frame, textvariable=self.filter_vars[label_text],
                                 state="readonly", width=10)
            combo['values'] = ["全部"]
            combo.grid(row=1, column=idx * 2 + 1, padx=5)
            setattr(self, f"{label_text}_combo", combo)


        # 设置默认值
        today = datetime.date.today()
        start_default = today + datetime.timedelta(days=1)
        end_default = start_default + datetime.timedelta(days=2)
        self.start_date_var.set(start_default.strftime("%Y-%m-%d"))
        self.end_date_var.set(end_default.strftime("%Y-%m-%d"))

        btn_apply_filter = tk.Button(filter_frame, text="应用筛选",
                                     command=self._apply_dropdown_filter,
                                     bg="#FFD700", fg="black", relief="raised", font=("Arial", 10))
        btn_apply_filter.grid(row=0, column=6, padx=10)

        btn_clear_filter = tk.Button(filter_frame, text="清除筛选",
                                     command=self._clear_filter,
                                     bg="#D3D3D3", fg="black", relief="raised", font=("Arial", 10))
        btn_clear_filter.grid(row=1, column=6, padx=5)

        # ========== 2. 隐藏模块 ==========
        hide_frame = tk.LabelFrame(top_frame, text="列操作")
        hide_frame.pack(side='left', padx=10)

        btn_hide_column = tk.Button(hide_frame, text="隐藏列",
                                    command=self._hide_columns,
                                    bg="#FFA07A", fg="black", relief="raised", font=("Arial", 10))
        btn_hide_column.pack(side='left', padx=5, pady=5)

        btn_show_column = tk.Button(hide_frame, text="显示列",
                                    command=self._show_columns,
                                    bg="#90EE90", fg="black", relief="raised", font=("Arial", 10))
        btn_show_column.pack(side='left', padx=5, pady=5)
        self.hidden_column_indices = []

        # ========== 3. 功能模块 ==========
        func_frame = tk.LabelFrame(top_frame, text="功能")
        func_frame.pack(side='right', padx=10)

        btn_clear_so = tk.Button(func_frame, text="一键清除订单",
                                 command=self._clear_all,
                                 bg='#ADD8E6', fg="black", relief="raised", font=("Arial", 10))
        btn_clear_so.pack(side='left', padx=5, pady=5)

        btn_copy_table = tk.Button(func_frame, text="复制当前表格",
                                   command=self.copy_all_to_clipboard,
                                   bg="#009A49", fg="white", relief="raised", font=("Arial", 10))
        btn_copy_table.pack(side='left', padx=5, pady=5)

        btn_refresh_oo = tk.Button(func_frame, text="刷新O类型订单数据",
                                    command=self.refresh_oo_order,
                                    bg="#FFC0CB", fg="black", relief="raised", font=("Arial", 10))
        btn_refresh_oo.pack(side='left', padx=5, pady=5)

        # 主工作区
        self.main_frame = tk.Frame(self.window)
        self.main_frame.pack(fill='both', expand=True, padx=10, pady=5)

    # endregion

    # region 创建工作表
    def _create_working_sheet(self):
        columns = [foh.order_id, foh.order_type, foh.corporate_id, foh.product, foh.shipto, foh.cust_name, foh.order_from, foh.order_to,
                   foh.ton, foh.comment, foh.target_date, foh.risk_date, foh.run_out_date]
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
        self.sheet.popup_menu_add_command("复制选中行的计划表形式", func=self._copy_selected_rows_by_plan_table)

        all_orders = list(self.order_data_manager.forecast_order_dict.values()) + list(self.order_data_manager.order_order_dict.values())
        self.add_order_display_in_working_sheet(order_lt=all_orders)

        self._update_filter_options()

    def _update_filter_options(self):
        """
        基于全量订单数据更新筛选下拉选项，而不是当前表格。
        """
        headers = self.sheet.headers()
        all_rows = self._get_all_rows_from_source()  # 全量数据

        for col_name in [foh.corporate_id, foh.product]:
            if col_name not in headers:
                continue

            col_index = headers.index(col_name)
            values = {str(row[col_index]) for row in all_rows if row[col_index]}  # 去重且排除空值
            options = ["全部"] + sorted(values)

            prev_selected = self.filter_vars[col_name].get()
            combo_attr = f"{col_name}_combo"

            if hasattr(self, combo_attr):
                combo = getattr(self, combo_attr)
                combo['values'] = options
                if prev_selected in options:
                    self.filter_vars[col_name].set(prev_selected)
                else:
                    self.filter_vars[col_name].set("全部")
                    combo.current(0)

    # endregion

    # region 事件处理

    def _order_to_row(self, order_lt: List[do.Order]):
        headers = self.sheet.headers()
        rows = []
        for order in order_lt:
            row = []
            for header in headers:
                attr = constant.ORDER_ATTR_MAP.get(header, "")
                value = getattr(order, attr, "")
                if header == foh.ton:
                    value = round(value / 1000, 1)
                if isinstance(value, datetime.datetime) and not pd.isnull(value):
                    value = value.strftime("%Y/%m/%d %H:%M")
                elif pd.isnull(value):
                    value = ""
                row.append(value)
            rows.append(row)
        return rows

    def _get_all_rows_from_source(self):
        # 每次都从数据源构建，不依赖当前 sheet 中的可见数据
        rows = []
        forecast_rows = self._order_to_row(order_lt=list(self.order_data_manager.forecast_order_dict.values()))
        order_rows = self._order_to_row(order_lt=list(self.order_data_manager.order_order_dict.values()))
        rows.extend(forecast_rows)
        rows.extend(order_rows)
        return rows

    def _apply_dropdown_filter(self):
        headers = self.sheet.headers()
        criteria = {}
        for col_name in [foh.corporate_id, foh.product]:
            selected = self.filter_vars[col_name].get()
            if selected != "全部":
                criteria[col_name] = selected
        # 获取日期范围
        try:
            start_date = datetime.datetime.strptime(self.start_date_var.get(), "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(self.end_date_var.get(), "%Y-%m-%d").date()
            if end_date < start_date:
                messagebox.showerror("错误", "结束日期不能早于开始日期")
                return
        except ValueError:
            messagebox.showerror("错误", "日期格式不正确")
            return

        all_rows = self._get_all_rows_from_source()
        col_index_map = {name: idx for idx, name in enumerate(headers)}

        filtered_rows = []
        for row in all_rows:
            match = True
            # DT、产品筛选
            for col_name, expected in criteria.items():
                idx = col_index_map[col_name]
                if str(row[idx]) != expected:
                    match = False
                    break
            if not match:
                continue

            # 日期筛选：订单到
            order_to_idx = col_index_map[foh.order_to]
            order_to_str = row[order_to_idx]
            if order_to_str:
                try:
                    order_to_date = datetime.datetime.strptime(order_to_str.split(" ")[0], "%Y/%m/%d").date()
                    if not (start_date <= order_to_date <= end_date):
                        continue
                except ValueError:
                    continue
            else:
                continue
            filtered_rows.append(row)

        self.sheet.set_sheet_data(filtered_rows)

    def _clear_filter(self):
        for k, var in self.filter_vars.items():
            var.set("全部")
            combo_attr = f"{k}_combo"
            if hasattr(self, combo_attr):
                getattr(self, combo_attr).current(0)

        today = datetime.date.today()
        start_default = today + datetime.timedelta(days=1)
        end_default = start_default + datetime.timedelta(days=2)
        self.start_date_var.set(start_default.strftime("%Y-%m-%d"))
        self.end_date_var.set(end_default.strftime("%Y-%m-%d"))

        self.sheet.set_sheet_data(self._get_all_rows_from_source())
        self._update_filter_options()

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
        # 弹窗居中
        self.window.update_idletasks()
        width, height = 320, 500
        x = self.window.winfo_x() + (self.window.winfo_width() - width) // 2
        y = self.window.winfo_y() + (self.window.winfo_height() - height) // 3

        popup = tk.Toplevel(self.window)
        popup.title(title)
        popup.geometry(f"{width}x{height}+{x}+{y}")
        popup.transient(self.window)
        popup.grab_set()

        # 标题
        tk.Label(popup, text="请选择列：", font=("Arial", 12, "bold")).pack(pady=10)

        # 滚动区域
        frame_container = tk.Frame(popup)
        frame_container.pack(fill="both", expand=True, padx=10)

        canvas = tk.Canvas(frame_container, borderwidth=0)
        scrollbar = tk.Scrollbar(frame_container, orient="vertical", command=canvas.yview)
        scroll_frame = tk.Frame(canvas)

        scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # 复选框
        selected_vars = []
        for opt in options:
            var = tk.BooleanVar()
            chk = ttk.Checkbutton(scroll_frame, text=opt, variable=var)
            chk.pack(anchor="w", pady=2)
            selected_vars.append((opt, var))

        # 按钮区域
        btn_frame = tk.Frame(popup)
        btn_frame.pack(pady=10)

        def select_all():
            for _, var in selected_vars:
                var.set(True)

        def deselect_all():
            for _, var in selected_vars:
                var.set(False)

        def confirm():
            result[:] = [name for name, var in selected_vars if var.get()]
            popup.destroy()

        result = []
        ttk.Button(btn_frame, text="全选", command=select_all).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="全不选", command=deselect_all).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="确认", command=confirm).pack(side="left", padx=5)

        popup.wait_window()
        return result

    def _on_cell_edit(self, event):
        row, column, value = event["row"], event["column"], event["value"]

        col_name = self.sheet.headers()[column]

        order_id_col = self.sheet.headers().index(foh.order_id)
        order_type_col = self.sheet.headers().index(foh.order_type)

        order_id = self.sheet.get_cell_data(row, order_id_col)
        order_type = self.sheet.get_cell_data(row, order_type_col)

        if order_type == enums.OrderType.FO:
            order = self.order_data_manager.forecast_order_dict.get(order_id)
        else:
            order = self.order_data_manager.order_order_dict.get(order_id)

        if not order:
            return

        try:
            if col_name in [foh.order_from, foh.order_to]:
                new_value = pd.to_datetime(value)
                if pd.isnull(new_value) or not isinstance(new_value, datetime.datetime):
                    raise ValueError("时间格式不正确")
                if (col_name == foh.order_from and new_value >= order.to_time) or \
                        (col_name == foh.order_to and new_value <= order.from_time):
                    raise ValueError("时间范围不正确")
                setattr(order, constant.ORDER_ATTR_MAP[col_name], new_value)
                value = new_value.strftime("%Y/%m/%d %H:%M")

            elif col_name == foh.ton:
                new_value = float(value)
                max_drop_kg = self.data_manager.get_max_payload_value_by_ship2(order.shipto) / 1000
                if not (0 < new_value <= max_drop_kg):
                    raise ValueError("吨应该大于0且小于等于最大配送量")
                setattr(order, constant.ORDER_ATTR_MAP[col_name], new_value * 1000)

            elif col_name == foh.comment:
                setattr(order, constant.ORDER_ATTR_MAP[col_name], value)

            else:
                raise ValueError(f"{col_name}列不支持编辑")

            self.order_data_manager.update_order_in_list(order)
            self.sheet.set_cell_data(row, column, value)

        except Exception as e:
            original_value = getattr(order, constant.ORDER_ATTR_MAP.get(col_name, ""), value)
            if isinstance(original_value, datetime.datetime) and not pd.isnull(original_value):
                original_value = original_value.strftime("%Y/%m/%d %H:%M")
            self.sheet.set_cell_data(row, column, original_value)
            messagebox.showerror(title="错误", message=str(e), parent=self.window)

    def _clear_all(self):
        confirm = messagebox.askyesno(title="提示", message="确认清空所有有的行吗？", parent=self.window)
        if not confirm:
            return
        for row_index in reversed(range(self.sheet.get_total_rows())):
            order_id_col = self.sheet.headers().index(foh.order_id)
            order_type_col = self.sheet.headers().index(foh.order_type)

            order_id = self.sheet.get_cell_data(row_index, order_id_col)
            order_type = self.sheet.get_cell_data(row_index, order_type_col)

            self.delete_order(order_id, order_type, row_index)
        self._update_filter_options()

    def copy_all_to_clipboard(self):
        total_rows = self.sheet.get_total_rows()
        if total_rows == 0:
            messagebox.showinfo(title="提示", message="没有行可复制！")
            return

        rows = [i for i in range(total_rows)]

        # 弹窗选择复制格式
        choice = self._ask_copy_format()
        if choice is None:
            return  # 用户取消

        if choice == "表格":
            self.copy_order_detail_text(rows=rows)
            messagebox.showinfo(title="提示", message="表格形式已复制到剪贴板！")
        elif choice == "计划表":
            self.copy_order_simple_text(rows=rows)
            messagebox.showinfo(title="提示", message="计划表形式已复制到剪贴板！")

    def _ask_copy_format(self):
        """
        弹出一个选择框，返回 '表格' 或 '计划表'，取消返回 None
        """
        # 把popup放到中间位置
        self.window.update_idletasks()
        x = self.window.winfo_x() + (self.window.winfo_width() - 300) // 2
        y = self.window.winfo_y() + (self.window.winfo_height() - 150) // 2

        popup = tk.Toplevel(self.window)
        popup.title("选择复制格式")
        popup.geometry("300x150+{}+{}".format(x, y))

        popup.transient(self.window)
        popup.grab_set()

        tk.Label(popup, text="请选择复制的格式：", font=("Arial", 12)).pack(pady=20)

        choice_var = tk.StringVar(value="")

        def select(choice):
            choice_var.set(choice)
            popup.destroy()

        btn_frame = tk.Frame(popup)
        btn_frame.pack(pady=10)

        tk.Button(btn_frame, text="表格形式", width=10, command=lambda: select("表格")).pack(side="left", padx=10)
        tk.Button(btn_frame, text="计划表形式", width=10, command=lambda: select("计划表")).pack(side="left", padx=10)

        popup.wait_window()
        return choice_var.get() if choice_var.get() else None

    def _delete_selected_row(self, event=None):
        selected_row = self.sheet.get_selected_rows()
        if not selected_row:
            return
        order_id_col = self.sheet.headers().index(foh.order_id)
        order_type_col = self.sheet.headers().index(foh.order_type)
        selected_order_lt = sorted([
            (self.sheet.get_cell_data(row_index, order_id_col), self.sheet.get_cell_data(row_index, order_type_col), row_index)
            for row_index in selected_row
        ], key=lambda x: x[2], reverse=True)
        confirm = messagebox.askyesno(title="确认删除", message="确认删除选中行订单吗？", parent=self.window)
        if confirm:
            for order_id, order_type, row_index in selected_order_lt:
                self.delete_order(order_id, order_type, row_index)
            self._update_filter_options()

    def _copy_selected_rows_by_plan_table(self, event=None):
        selected_rows = self.sheet.get_selected_rows()
        if not selected_rows:
            messagebox.showinfo(title="提示", message="没有选中的行可复制！")
            return

        self.copy_order_simple_text(rows=selected_rows)


    def copy_order_simple_text(self, rows):
        headers = self.sheet.headers()

        order_simple_lt = []
        for row_index in rows:
            cust_name_col = headers.index(foh.cust_name)
            from_time_col = headers.index(foh.order_from)
            to_time_col = headers.index(foh.order_to)
            drop_ton_col = headers.index(foh.ton)
            comment_col = headers.index(foh.comment)

            cust_name = self.sheet.get_cell_data(row_index, cust_name_col)
            from_time = pd.to_datetime(self.sheet.get_cell_data(row_index, from_time_col))
            to_time = pd.to_datetime(self.sheet.get_cell_data(row_index, to_time_col))
            drop_ton = self.sheet.get_cell_data(row_index, drop_ton_col)
            comment = self.sheet.get_cell_data(row_index, comment_col)

            if comment:
                comment = '，{}'.format(comment)

            simple_order_string = '{}({}号{}点-{}号{}点，{}吨{})'.format(
                cust_name,
                from_time.strftime('%d'),
                from_time.strftime('%H'),
                to_time.strftime('%d'),
                to_time.strftime('%H'),
                drop_ton,
                comment
            )

            order_simple_lt.append(simple_order_string)

        text = "\n".join(order_simple_lt)
        self.window.clipboard_clear()
        self.window.clipboard_append(text)
        messagebox.showinfo(title="提示", message="选中行的计划表形式已复制到剪贴板！")

    def copy_order_detail_text(self, rows):
        headers = self.sheet.headers()
        col_num = len(headers)
        data = [headers]
        for row_index in rows:
            row = [self.sheet.get_cell_data(row_index, col_index) for col_index in range(col_num)]
            data.append(row)
        text = "\n".join(["\t".join(map(str, row)) for row in data])
        self.window.clipboard_clear()
        self.window.clipboard_append(text)
        messagebox.showinfo(title="提示", message="整张表格已复制到剪贴板！")

    def _on_close(self):
        self.closed = True
        self.window.destroy()
    # endregion

    # region 订单相关操作
    def delete_order(self, order_id, order_type, row_index):
        self.sheet.delete_row(row_index)

        if order_type == enums.OrderType.FO:
            del self.order_data_manager.forecast_order_dict[order_id]
        elif order_type == enums.OrderType.OO:
            del self.order_data_manager.order_order_dict[order_id]
        self.order_data_manager.delete_order_from_list(order_id=order_id, order_type=order_type)


    def add_order_display_in_working_sheet(self, order_lt:List[do.Order]):
        data = self._order_to_row(order_lt=order_lt)
        self.sheet.insert_rows(data)

    def refresh_oo_order(self):
        try:
            self.order_data_manager.refresh_oo_list()

            # 把sheet里面order_type=OO的行删除
            headers = self.sheet.headers()
            for row_index in reversed(range(self.sheet.get_total_rows())):
                order_type_col = headers.index(foh.order_type)
                order_type = self.sheet.get_cell_data(row_index, order_type_col)

                if order_type != enums.OrderType.OO:
                    continue
                self.sheet.delete_row(row_index)

            # 把新的OO订单插入sheet
            self.add_order_display_in_working_sheet(order_lt=list(self.order_data_manager.order_order_dict.values()))
            self._update_filter_options()
            messagebox.showinfo(title="提示", message="OO订单刷新成功！", parent=self.window)
        except Exception as e:
            messagebox.showerror(title="错误", message='刷新OO数据失败：' + str(e), parent=self.window)

    # endregion

