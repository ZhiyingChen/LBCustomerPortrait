import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import datetime as dt
import re
from typing import List, Optional, Tuple
from tkcalendar import DateEntry
from tksheet import Sheet

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

    # -------------------------
    # 顶部操作区域（筛选/列操作/功能）
    # -------------------------
    def _decorate_top_frame(self):
        top_frame = tk.Frame(self.window)
        top_frame.pack(side='top', fill='x', padx=10, pady=5)

        # 1) 筛选模块
        filter_frame = tk.LabelFrame(top_frame, text="筛选")
        filter_frame.pack(side='left', padx=10)

        tk.Label(filter_frame, text="开始日期").grid(row=0, column=0, padx=5)
        self.start_date_var = tk.StringVar()
        self.start_date_picker = DateEntry(filter_frame, textvariable=self.start_date_var,
                                           date_pattern="yyyy-mm-dd", width=12)
        self.start_date_picker.grid(row=0, column=1, padx=5)

        tk.Label(filter_frame, text="结束日期").grid(row=0, column=2, padx=5)
        self.end_date_var = tk.StringVar()
        self.end_date_picker = DateEntry(filter_frame, textvariable=self.end_date_var,
                                         date_pattern="yyyy-mm-dd", width=12)
        self.end_date_picker.grid(row=0, column=3, padx=5)

        self.filter_vars = {
            foh.corporate_id: tk.StringVar(value="全部"),
            foh.product: tk.StringVar(value="全部"),
        }
        for idx, label_text in enumerate([foh.corporate_id, foh.product]):
            tk.Label(filter_frame, text=label_text).grid(row=1, column=idx * 2, padx=5)
            combo = ttk.Combobox(filter_frame, textvariable=self.filter_vars[label_text],
                                 state="readonly", width=12)
            combo['values'] = ["全部"]
            combo.grid(row=1, column=idx * 2 + 1, padx=5)
            setattr(self, f"{label_text}_combo", combo)

        # 默认日期：明日 0 点（显示 yyyy-mm-dd），结束 = 开始 + 2 天
        today = dt.date.today()
        start_default = today + dt.timedelta(days=1)
        end_default = start_default + dt.timedelta(days=2)
        self.start_date_var.set(start_default.strftime("%Y-%m-%d"))
        self.end_date_var.set(end_default.strftime("%Y-%m-%d"))

        tk.Button(filter_frame, text="应用筛选", command=self._apply_dropdown_filter,
                  bg="#FFD700", relief="raised", font=("Arial", 10)).grid(row=0, column=6, padx=10)
        tk.Button(filter_frame, text="清除筛选", command=self._clear_filter,
                  bg="#D3D3D3", relief="raised", font=("Arial", 10)).grid(row=1, column=6, padx=5)

        # 2) 列操作
        hide_frame = tk.LabelFrame(top_frame, text="列操作")
        hide_frame.pack(side='left', padx=10)
        tk.Button(hide_frame, text="隐藏列", command=self._hide_columns,
                  bg="#FFA07A", relief="raised", font=("Arial", 10)).pack(side='left', padx=5, pady=5)
        tk.Button(hide_frame, text="显示列", command=self._show_columns,
                  bg="#90EE90", relief="raised", font=("Arial", 10)).pack(side='left', padx=5, pady=5)
        self.hidden_column_indices: List[int] = []

        # 3) 功能模块
        func_frame = tk.LabelFrame(top_frame, text="功能")
        func_frame.pack(side='right', padx=10)
        tk.Button(func_frame, text="一键清除\n所有订单", command=self._clear_all,
                  bg='#ADD8E6', relief="raised", font=("Arial", 10)).pack(side='left', padx=5, pady=5)
        tk.Button(func_frame, text="复制\n当前表格", command=self.copy_all_to_clipboard,
                  bg="#009A49", fg="white", relief="raised", font=("Arial", 10)).pack(side='left', padx=5, pady=5)
        tk.Button(func_frame, text="刷新O类型\n订单数据", command=self.refresh_oo_order,
                  bg="#FFC0CB", relief="raised", font=("Arial", 10)).pack(side='left', padx=5, pady=5)

        # 主工作区
        self.main_frame = tk.Frame(self.window)
        self.main_frame.pack(fill='both', expand=True, padx=10, pady=5)

    # -------------------------
    # 创建工作表
    # -------------------------
    def _create_working_sheet(self):
        self.base_headers = [
            foh.order_id, foh.order_type, foh.corporate_id, foh.product,
            foh.shipto, foh.cust_name, foh.order_from, foh.order_to,
            foh.ton, foh.comment, foh.target_date, foh.risk_date, foh.run_out_date
        ]
        self.sheet = Sheet(self.main_frame,
                           headers=self.base_headers[:],
                           show_x_scrollbar=True,
                           show_y_scrollbar=True)
        self.sheet.enable_bindings((
            "single_select", "row_select", "column_select", "drag_select",
            # 为避免表头箭头映射混乱，建议禁用列拖拽；如需开启，需要同步更新 base_headers
            # "column_drag_and_drop",
            "row_drag_and_drop",
            "right_click_popup_menu", "rc_select", "copy", "edit_cell"
        ))
        self.sheet.pack(fill='both', expand=True)

        self.sheet.extra_bindings("end_edit_cell", func=self._on_cell_edit)
        self.sheet.popup_menu_add_command("删除选中的行", func=self._delete_selected_row)
        self.sheet.popup_menu_add_command("复制选中行的计划表形式", func=self._copy_selected_rows_by_plan_table)

        # 初始渲染（一次性填充，避免 insert_rows 造成空白行）
        all_rows = self._get_all_rows_from_source()
        self._render_rows(all_rows)

        # 基于全量数据更新下拉候选
        self._update_filter_options()

    # -------------------------
    # 工具与渲染
    # -------------------------
    def _idx(self, header_name: str) -> int:
        """用 base_headers 获取列索引，避免因显示箭头导致 headers() 失配"""
        return self.base_headers.index(header_name)

    def _safe_to_str(self, v) -> str:
        return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)

    def _parse_display_dt(self, s: str) -> Optional[dt.datetime]:
        """从 'YYYY/MM/DD HH:MM' 字符串解析 datetime；失败返回 None"""
        if not s:
            return None
        try:
            return dt.datetime.strptime(s.strip(), "%Y/%m/%d %H:%M")
        except Exception:
            return None

    def _order_to_rows(self, order_lt: List[do.Order]) -> List[List]:
        rows: List[List] = []
        for order in order_lt:
            row = []
            for header in self.base_headers:
                attr = constant.ORDER_ATTR_MAP.get(header, "")
                value = getattr(order, attr, "")
                if header == foh.ton:
                    try:
                        value = round(value / 1000, 1)
                    except Exception:
                        pass
                if isinstance(value, dt.datetime) and not pd.isnull(value):
                    value = value.strftime("%Y/%m/%d %H:%M")
                elif not isinstance(value, str) and pd.isnull(value):
                    value = ""
                row.append(value)
            rows.append(row)
        return rows

    def _get_all_rows_from_source(self) -> List[List]:
        fo_rows = self._order_to_rows(list(self.order_data_manager.forecast_order_dict.values()))
        oo_rows = self._order_to_rows(list(self.order_data_manager.order_order_dict.values()))
        return fo_rows + oo_rows

    def _render_rows(self, rows: List[List]):
        """统一渲染入口：设置数据 -> 应用现有排序 -> 刷新箭头表头"""
        self.sheet.set_sheet_data(rows)


    # -------------------------
    # 筛选（基于全量数据）
    # -------------------------
    def _update_filter_options(self):
        """始终基于全量数据生成下拉候选，不受当前表格过滤影响"""
        all_rows = self._get_all_rows_from_source()

        for col_name in [foh.corporate_id, foh.product]:
            col_index = self._idx(col_name)
            values = {self._safe_to_str(row[col_index]) for row in all_rows if self._safe_to_str(row[col_index])}
            options = ["全部"] + sorted(values)

            prev_selected = self.filter_vars[col_name].get()
            combo_attr = f"{col_name}_combo"

            if hasattr(self, combo_attr):
                combo = getattr(self, combo_attr)
                combo['values'] = options
                if prev_selected in options:
                    self.filter_vars[col_name].set(prev_selected)
                    combo.set(prev_selected)
                else:
                    self.filter_vars[col_name].set("全部")
                    combo.current(0)

    def _apply_dropdown_filter(self):
        # 1) 解析日期区间
        try:
            start_date = dt.datetime.strptime(self.start_date_var.get(), "%Y-%m-%d").date()
            end_date = dt.datetime.strptime(self.end_date_var.get(), "%Y-%m-%d").date()
            if end_date < start_date:
                messagebox.showerror("错误", "结束日期不能早于开始日期")
                return
        except ValueError:
            messagebox.showerror("错误", "日期格式不正确")
            return

        # 2) 解析下拉条件
        criteria = {}
        for col_name in [foh.corporate_id, foh.product]:
            selected = self.filter_vars[col_name].get()
            if selected != "全部":
                criteria[col_name] = selected

        # 3) 从全量数据开始过滤
        all_rows = self._get_all_rows_from_source()

        order_to_idx = self._idx(foh.order_to)
        filtered_rows = []
        for row in all_rows:
            # 2.1 DT/产品精确匹配
            matched = True
            for k, v in criteria.items():
                if self._safe_to_str(row[self._idx(k)]) != v:
                    matched = False
                    break
            if not matched:
                continue

            # 2.2 日期过滤（订单到 的“日期部分”在 [start_date, end_date]）
            date_str = self._safe_to_str(row[order_to_idx])
            dt_obj = self._parse_display_dt(date_str)
            if not dt_obj:
                continue
            if not (start_date <= dt_obj.date() <= end_date):
                continue

            filtered_rows.append(row)

        # 4) 渲染
        self._render_rows(filtered_rows)

    def _clear_filter(self):
        # 下拉重置
        for k, var in self.filter_vars.items():
            var.set("全部")
            combo_attr = f"{k}_combo"
            if hasattr(self, combo_attr):
                getattr(self, combo_attr).current(0)

        # 日期重置
        today = dt.date.today()
        start_default = today + dt.timedelta(days=1)
        end_default = start_default + dt.timedelta(days=2)
        self.start_date_var.set(start_default.strftime("%Y-%m-%d"))
        self.end_date_var.set(end_default.strftime("%Y-%m-%d"))

        # 渲染全量
        self._render_rows(self._get_all_rows_from_source())
        self._update_filter_options()

    # -------------------------
    # 列隐藏/显示
    # -------------------------
    def _hide_columns(self):
        headers_display = self.sheet.headers()  # 显示用（可能包含箭头）
        # 通过 base_headers 来给用户选择，避免箭头影响
        available_names = [h for i, h in enumerate(self.base_headers) if i not in self.hidden_column_indices]
        selected = self._ask_multiple_columns(available_names, "选择要隐藏的列")
        if selected:
            indices = [self._idx(name) for name in selected]
            self.sheet.hide_columns(indices)
            # 维护唯一性
            for idx in indices:
                if idx not in self.hidden_column_indices:
                    self.hidden_column_indices.append(idx)

    def _show_columns(self):
        if not self.hidden_column_indices:
            messagebox.showinfo(title="提示", message="没有隐藏的列")
            return
        hidden_names = [self.base_headers[i] for i in self.hidden_column_indices]
        selected = self._ask_multiple_columns(hidden_names, "选择要显示的列")
        if selected:
            indices = [self._idx(name) for name in selected]
            self.sheet.show_columns(indices)
            # 从隐藏集合中移除
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

        def _on_cfg(_):
            canvas.configure(scrollregion=canvas.bbox("all"))

        scroll_frame.bind("<Configure>", _on_cfg)
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

        result: List[str] = []

        def select_all():
            for _, var in selected_vars:
                var.set(True)

        def deselect_all():
            for _, var in selected_vars:
                var.set(False)

        def confirm():
            result[:] = [name for name, var in selected_vars if var.get()]
            popup.destroy()

        ttk.Button(btn_frame, text="全选", command=select_all).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="全不选", command=deselect_all).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="确认", command=confirm).pack(side="left", padx=5)

        popup.wait_window()
        return result

    # -------------------------
    # 单元格编辑
    # -------------------------
    def _on_cell_edit(self, event):
        row, column, value = event["row"], event["column"], event["value"]
        col_name = self.base_headers[column]  # 用 base_headers 对齐真实列名

        order_id_col = self._idx(foh.order_id)
        order_type_col = self._idx(foh.order_type)

        order_id = self.sheet.get_cell_data(row, order_id_col)
        order_type = self.sheet.get_cell_data(row, order_type_col)



        try:
            if order_type == enums.OrderType.OO:
                order = self.order_data_manager.order_order_dict.get(order_id)
                raise ValueError("OO 订单不允许修改任何属性！")

            order = self.order_data_manager.forecast_order_dict.get(order_id)

            if col_name in [foh.order_from, foh.order_to]:
                new_value = pd.to_datetime(value)
                if pd.isnull(new_value) or not isinstance(new_value, dt.datetime):
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

            # 同步数据列表
            self.order_data_manager.update_order_in_list(order)
            # 更新 UI
            self.sheet.set_cell_data(row, column, value)

        except Exception as e:
            original_value = getattr(order, constant.ORDER_ATTR_MAP.get(col_name, ""), value)
            if isinstance(original_value, dt.datetime) and not pd.isnull(original_value):
                original_value = original_value.strftime("%Y/%m/%d %H:%M")
            self.sheet.set_cell_data(row, column, original_value)
            messagebox.showerror(title="错误", message=str(e), parent=self.window)

    # -------------------------
    # 删除/撤销删除/清空
    # -------------------------
    def _clear_all(self):
        confirm = messagebox.askyesno(title="提示", message="确认清空所有行吗？", parent=self.window)
        if not confirm:
            return

        order_id_col = self._idx(foh.order_id)
        order_type_col = self._idx(foh.order_type)

        # 收集全部行（从大到小删除）
        to_delete = []
        for row_index in range(self.sheet.get_total_rows()):
            order_id = self.sheet.get_cell_data(row_index, order_id_col)
            order_type = self.sheet.get_cell_data(row_index, order_type_col)
            to_delete.append((order_id, order_type, row_index))

        for order_id, order_type, row_index in sorted(to_delete, key=lambda x: x[2], reverse=True):
            self.delete_order(order_id, order_type, row_index)

        self._update_filter_options()

    def _delete_selected_row(self, event=None):
        selected_row = self.sheet.get_selected_rows()
        if not selected_row:
            return

        order_id_col = self._idx(foh.order_id)
        order_type_col = self._idx(foh.order_type)
        selected_order_lt = sorted([
            (self.sheet.get_cell_data(row_index, order_id_col),
             self.sheet.get_cell_data(row_index, order_type_col),
             row_index)
            for row_index in selected_row
        ], key=lambda x: x[2], reverse=True)

        confirm = messagebox.askyesno(title="确认删除", message="确认删除选中行订单吗？", parent=self.window)
        if confirm:
            for order_id, order_type, row_index in selected_order_lt:
                self.delete_order(order_id, order_type, row_index)
            self._update_filter_options()

    def delete_order(self, order_id, order_type, row_index):
        # 删除 UI 行
        self.sheet.delete_row(row_index)

        # 删除数据字典，避免 KeyError
        if order_type == enums.OrderType.FO:
            self.order_data_manager.forecast_order_dict.pop(order_id, None)
        elif order_type == enums.OrderType.OO:
            self.order_data_manager.order_order_dict.pop(order_id, None)

        # 同步更新列表
        self.order_data_manager.delete_order_from_list(order_id=order_id, order_type=order_type)

    # -------------------------
    # 复制
    # -------------------------
    def copy_all_to_clipboard(self):
        total_rows = self.sheet.get_total_rows()
        if total_rows == 0:
            messagebox.showinfo(title="提示", message="没有行可复制！")
            return

        rows = list(range(total_rows))

        # 弹窗选择复制格式
        choice = self._ask_copy_format()
        if not choice:
            return  # 用户取消

        if choice == "表格":
            self.copy_order_detail_text(rows=rows)
            messagebox.showinfo(title="提示", message="表格形式已复制到剪贴板！", parent=self.window)
        elif choice == "计划表":
            self.copy_order_simple_text(rows=rows)
            messagebox.showinfo(title="提示", message="计划表形式已复制到剪贴板！", parent=self.window)

    def _ask_copy_format(self):
        # 居中弹窗
        self.window.update_idletasks()
        x = self.window.winfo_x() + (self.window.winfo_width() - 300) // 2
        y = self.window.winfo_y() + (self.window.winfo_height() - 150) // 2

        popup = tk.Toplevel(self.window)
        popup.title("选择复制格式")
        popup.geometry(f"300x150+{x}+{y}")
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

    def _copy_selected_rows_by_plan_table(self, event=None):
        selected_rows = self.sheet.get_selected_rows()
        if not selected_rows:
            messagebox.showinfo(title="提示", message="没有选中的行可复制！", parent=self.window)
            return
        self.copy_order_simple_text(rows=selected_rows)

    def copy_order_simple_text(self, rows: List[int]):
        headers = self.base_headers  # 保证字段名一致
        cust_name_col = self._idx(foh.cust_name)
        from_time_col = self._idx(foh.order_from)
        to_time_col = self._idx(foh.order_to)
        drop_ton_col = self._idx(foh.ton)
        comment_col = self._idx(foh.comment)

        order_simple_lt = []
        for row_index in rows:
            cust_name = self.sheet.get_cell_data(row_index, cust_name_col)
            from_time = pd.to_datetime(self.sheet.get_cell_data(row_index, from_time_col))
            to_time = pd.to_datetime(self.sheet.get_cell_data(row_index, to_time_col))
            drop_ton = self.sheet.get_cell_data(row_index, drop_ton_col)
            comment = self.sheet.get_cell_data(row_index, comment_col) or ""

            comment_suffix = f"，{comment}" if comment else ""
            simple_order_string = "{}({}号{}点-{}号{}点，{}吨{})".format(
                cust_name,
                from_time.strftime('%d'),
                from_time.strftime('%H'),
                to_time.strftime('%d'),
                to_time.strftime('%H'),
                drop_ton,
                comment_suffix
            )
            order_simple_lt.append(simple_order_string)

        text = "\n".join(order_simple_lt)
        self.window.clipboard_clear()
        self.window.clipboard_append(text)

    def copy_order_detail_text(self, rows: List[int]):
        headers = self.sheet.headers()  # 当前显示的表头（可能带箭头）
        # 将表头中的箭头去除，保持导出整洁
        clean_headers = [re.sub(r"\s*[↑↓]\d+$", "", h) for h in headers]
        col_num = len(headers)

        data = [clean_headers]
        for row_index in rows:
            row = [self._safe_to_str(self.sheet.get_cell_data(row_index, col_index)) for col_index in range(col_num)]
            data.append(row)

        text = "\n".join(["\t".join(map(str, row)) for row in data])
        self.window.clipboard_clear()
        self.window.clipboard_append(text)

    # -------------------------
    # 新增 FO 订单
    # -------------------------
    def add_order_to_ui(self, order: do.Order):
        """将新创建的 FO 订单追加到当前表格中"""
        try:
            # 获取当前表格数据
            current_rows = self.sheet.get_sheet_data()
            # 将新订单转换为行格式
            new_rows = self._order_to_rows(order_lt=[order])
            # 合并数据
            current_rows.extend(new_rows)
            # 渲染合并后的数据
            self._render_rows(current_rows)
            # 更新筛选下拉框
            self._update_filter_options()
        except Exception as e:
            messagebox.showerror(title="错误", message=f"添加订单失败：{e}", parent=self.window)


    # -------------------------
    # 刷新 OO 订单
    # -------------------------
    def refresh_oo_order(self):
        try:
            self.order_data_manager.refresh_oo_list()

            # 删除 UI 中 order_type = OO 的行
            order_type_col = self._idx(foh.order_type)
            for row_index in reversed(range(self.sheet.get_total_rows())):
                order_type = self.sheet.get_cell_data(row_index, order_type_col)
                if order_type == enums.OrderType.OO:
                    self.sheet.delete_row(row_index)

            # 插入新的 OO 行
            oo_rows = self._order_to_rows(list(self.order_data_manager.order_order_dict.values()))
            # 在现有数据末尾追加
            current = self.sheet.get_sheet_data()
            current.extend(oo_rows)
            self._render_rows(current)

            self._update_filter_options()
            messagebox.showinfo(title="提示", message="OO订单刷新成功！", parent=self.window)
        except Exception as e:
            messagebox.showerror(title="错误", message='刷新OO数据失败：' + str(e), parent=self.window)

    # -------------------------
    # 关闭
    # -------------------------
    def _on_close(self):
        self.closed = True
        self.window.destroy()
