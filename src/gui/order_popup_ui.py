import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import pandas as pd
import datetime
from tksheet import Sheet
from tkcalendar import DateEntry
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

        # 主工作区
        self.main_frame = tk.Frame(self.window)
        self.main_frame.pack(fill='both', expand=True, padx=10, pady=5)

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

        for shipto, fo in self.order_data_manager.forecast_order_dict.items():
            self.add_order_display_in_working_sheet(order=fo)

        self._update_filter_options()

    def _update_filter_options(self):
        """
        用当前 sheet 数据生成筛选选项。
        同时兼容两种情形：
        1) 新：ttk.Combobox，属性名为 {col_name}_combo
        2) 旧：OptionMenu，属性名为 {col_name}_dropdown（便于逐步迁移）
        """
        headers = self.sheet.headers()
        for col_name in [foh.corporate_id, foh.product]:
            if col_name not in headers:
                continue

            col_index = headers.index(col_name)

            # 收集去重后的值
            values = set()
            for row_index in range(self.sheet.get_total_rows()):
                val = self.sheet.get_cell_data(row_index, col_index)
                values.add(str(val))

            options = ["全部"] + sorted(values)

            # 记录之前用户选中，尽量保留
            prev_selected = self.filter_vars[col_name].get()

            combo_attr = f"{col_name}_combo"
            dropdown_attr = f"{col_name}_dropdown"

            if hasattr(self, combo_attr):  # 新：Combobox
                combo = getattr(self, combo_attr)
                combo['values'] = options
                # 恢复或回退到"全部"
                if prev_selected in options:
                    self.filter_vars[col_name].set(prev_selected)
                    # 如果是 readonly，显示就跟着变量走；如果想强制定位可以 combo.set(prev_selected)
                else:
                    self.filter_vars[col_name].set("全部")
                    combo.current(0)

            elif hasattr(self, dropdown_attr):  # 旧：OptionMenu（过渡兼容，尽快移除）
                menu = getattr(self, dropdown_attr)["menu"]
                menu.delete(0, "end")
                for opt in options:
                    menu.add_command(label=opt, command=lambda v=opt, k=col_name: self.filter_vars[k].set(v))
                # 同样恢复或回退
                if prev_selected in options:
                    self.filter_vars[col_name].set(prev_selected)
                else:
                    self.filter_vars[col_name].set("全部")
            else:
                # 既没有 combo 也没有 dropdown；不抛错，以免影响运行
                pass

    # endregion

    # region 事件处理

    def _order_to_row(self, order: do.Order):
        headers = self.sheet.headers()
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
        return row

    def _get_all_rows_from_source(self):
        # 每次都从数据源构建，不依赖当前 sheet 中的可见数据
        rows = []
        for order in self.order_data_manager.forecast_order_dict.values():
            rows.append(self._order_to_row(order))
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

        # 把popup放到中间位置
        self.window.update_idletasks()
        x = self.window.winfo_x() + (self.window.winfo_width() - 300) // 2
        y = self.window.winfo_y() + (self.window.winfo_height() - 500) // 3

        popup = tk.Toplevel(self.window)
        popup.title(title)
        popup.geometry("300x500+{}+{}".format(x, y))
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

            self.order_data_manager.update_forecast_order_in_fo_list(order)
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
            order_id = self.sheet.get_cell_data(row_index, 0)
            self.delete_order(order_id, row_index)

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
        order_col = self.sheet.headers().index(foh.order_id)
        selected_order_lt = [
            (self.sheet.get_cell_data(row_index, order_col), row_index)
            for row_index in selected_row
        ]
        confirm = messagebox.askyesno(title="确认删除", message="确认删除选中行订单吗？", parent=self.window)
        if confirm:
            for order_id, row_index in selected_order_lt:
                self.delete_order(order_id, row_index)

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
    def delete_order(self, order_id, row_index):
        self.order_data_manager.delete_forecast_order_from_fo_list(order_id=order_id)
        del self.order_data_manager.forecast_order_dict[order_id]
        self.sheet.delete_row(row_index)

    def add_order_display_in_working_sheet(self, order: do.Order):
        data = self._order_to_row(order=order)
        self.sheet.insert_row(data)
    # endregion

