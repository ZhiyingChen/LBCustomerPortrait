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
    # 创建工作表（修改：左右并排）
    # -------------------------
    def _create_working_sheet(self):
        # === 容器：左右两栏 ===
        self.left_frame = tk.Frame(self.main_frame)
        self.left_frame.pack(side="left", fill="both", expand=True)
        self.right_frame = tk.Frame(self.main_frame, width=1050)  # 右侧宽度可按需
        self.right_frame.pack(side="left", fill="both", expand=False)

        # === 左：原 sheet（保持不变） ===
        self.base_headers = [
            foh.order_id, foh.order_type, foh.corporate_id, foh.product,
            foh.shipto, foh.cust_name, foh.order_from, foh.order_to,
            foh.ton, foh.comment, foh.target_date, foh.risk_date, foh.run_out_date
        ]
        # 左：原 sheet（保持功能不变，但关闭内部纵向滚动条）
        self.sheet = Sheet(self.left_frame,
                           headers=self.base_headers[:],
                           show_x_scrollbar=True,
                           show_y_scrollbar=False)  # 关闭内部纵向滚动条

        self.sheet.enable_bindings((
            "single_select", "row_select", "column_select", "drag_select",
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

        # === 右：甘特图 sheet（新增） ===
        self._init_gantt_sheet()

        # 窗口大小变化时自动调整列宽
        self.window.bind("<Configure>", lambda e: self._auto_adjust_column_widths())

        # 基于全量数据更新下拉候选
        self._update_filter_options()

        # === 同步绑定（新增） ===
        self._bind_cross_sheet_sync()

    # -------------------------
    # 甘特图：初始化（新增）
    # -------------------------
    def _init_gantt_sheet(self) -> None:
        """创建右侧的甘特图 sheet（只读），并填充与左侧相同的行数"""
        # 甘特时间起点：明天 00:00，范围 48 小时
        today = dt.date.today()
        start = dt.datetime.combine(today + dt.timedelta(days=1), dt.time(0, 0))
        self.gantt_start_dt: dt.datetime = start
        self.gantt_hours: List[dt.datetime] = [start + dt.timedelta(hours=i) for i in range(48)]
        headers = [h.strftime("%d %H") for h in self.gantt_hours]

        # ---- 容器：右侧包含 甘特sheet + 外置竖向滚动条（在最右侧） ----
        self.gantt_container = tk.Frame(self.right_frame)
        self.gantt_container.pack(fill="both", expand=True)

        # 右侧甘特 sheet：关闭纵向滚动条，保留横向滚动条
        self.gantt_sheet = Sheet(
            self.gantt_container,
            headers=headers,
            show_x_scrollbar=True,
            show_y_scrollbar=False,  # 关闭内部纵向滚动条
        )
        self.gantt_sheet.enable_bindings(("single_select", "row_select", "drag_select"))
        self.gantt_sheet.set_options(edit_cell_enabled=False)
        self.gantt_sheet.pack(side="left", fill="both", expand=True)

        # === 外置纵向滚动条：放在最右端 ===
        self._vbar = tk.Scrollbar(self.gantt_container, orient="vertical",
                                  command=self._on_shared_vscroll)
        self._vbar.pack(side="right", fill="y")

        # 以左侧当前行数，填充空白行（文本先为空，随后渲染颜色与小时数字）
        row_count = self.sheet.get_total_rows()
        self.gantt_sheet.set_sheet_data([[""] * len(headers) for _ in range(row_count)])

        # 初次渲染 + 同步滚动条位置
        self._render_gantt_rows_from_left()
        self._update_shared_vbar()

    # -------------------------
    # 甘特图：根据左侧行重绘（新增）
    # -------------------------
    def _render_gantt_rows_from_left(self) -> None:
        """读取左侧 sheet 当前行顺序+数据，生成右侧甘特行内容与涂色"""
        rows = self.sheet.get_sheet_data()
        # 调整右侧行数与左侧一致
        left_n = len(rows)
        right_n = self.gantt_sheet.get_total_rows()
        if right_n < left_n:
            self.gantt_sheet.insert_rows(left_n - right_n)
        elif right_n > left_n:
            for r in range(right_n - 1, left_n - 1, -1):
                self.gantt_sheet.delete_row(r)

        # 逐行绘制
        for r_idx, row in enumerate(rows):
            self._render_one_gantt_row(r_idx, row)

        self.gantt_sheet.redraw()

    def _render_one_gantt_row(self, r_idx: int, row: List) -> None:
        """将一行订单数据渲染到甘特图：底色 & 小时数字"""

        # —— 提取字段（按你的列头映射）
        def parse_dt_cell(col_name: str) -> Optional[dt.datetime]:
            s = self._safe_to_str(row[self._idx(col_name)])
            if not s:
                return None
            # 左表存储格式已保证是 "YYYY/MM/DD HH:MM"
            try:
                return dt.datetime.strptime(s, "%Y/%m/%d %H:%M")
            except Exception:
                # 若用户刚编辑，tksheet 可能临时是 pd.to_datetime 的字符串形式
                try:
                    return pd.to_datetime(s).to_pydatetime()
                except Exception:
                    return None

        from_dt = parse_dt_cell(foh.order_from)
        to_dt = parse_dt_cell(foh.order_to)
        target_dt = parse_dt_cell(foh.target_date)  # 目标充装
        best_dt = parse_dt_cell(foh.risk_date)  # 最佳充装
        outage_dt = parse_dt_cell(foh.run_out_date)  # 断气

        # —— 预清空该行（文字、颜色）
        col_n = len(self.gantt_hours)
        self.gantt_sheet.set_row_data(r_idx, [""] * col_n, redraw=False)
        # 清色：覆盖涂白
        for c in range(col_n):
            self.gantt_sheet.highlight_cells(row=r_idx, column=c, bg="#FFFFFF", fg="#000000", redraw=False)

        # —— 工具：换算“包含该小时”的列索引
        def hour_idx(dt_val: Optional[dt.datetime]) -> Optional[int]:
            if not dt_val:
                return None
            delta = dt_val - self.gantt_start_dt
            h = int(delta.total_seconds() // 3600)
            if 0 <= h < col_n:
                return h
            return None

        from_h = hour_idx(from_dt)
        to_h = hour_idx(to_dt)
        target_h = hour_idx(target_dt)
        best_h = hour_idx(best_dt)
        outage_h = hour_idx(outage_dt)

        # —— 颜色逻辑
        GREEN = "#90EE90"  # 亮绿
        YELLOW = "#FFD966"  # 浅黄
        RED = "#FFA6A6"  # 浅红
        WHITE = "#FFFFFF"

        def paint(seg_from: int, seg_to: int, color: str):
            """闭区间 [seg_from, seg_to] 上色（做边界裁剪）"""
            if seg_from is None or seg_to is None:
                return
            a, b = max(0, seg_from), min(col_n - 1, seg_to)
            if a > b:
                return
            for c in range(a, b + 1):
                self.gantt_sheet.highlight_cells(row=r_idx, column=c, bg=color, fg="#000000", redraw=False)

        all_three = (target_h is not None and best_h is not None and outage_h is not None)

        if all_three:
            # 逻辑1
            # 绿：[target(含), best(前1)]
            paint(target_h, best_h - 1, GREEN)
            # 黄：[best(含), outage(前1)]
            paint(best_h, outage_h - 1, YELLOW)
            # 红：[outage(含), 末尾]
            paint(outage_h, col_n - 1, RED)
        else:
            # 逻辑2
            if from_h is not None:
                # 绿：[from(含), to(前1)] —— 注：避免重叠冲突
                end_g = (to_h - 1) if (to_h is not None) else (col_n - 1)
                paint(from_h, end_g, GREEN)
            if to_h is not None:
                # 红：[to(含), 末尾]
                paint(to_h, col_n - 1, RED)

        # —— 标注小时数字（只在 from/to 的那一列）
        def put_hour(h_idx: Optional[int], dt_val: Optional[dt.datetime]):
            if h_idx is not None and dt_val is not None:
                txt = dt_val.strftime("%H")
                self.gantt_sheet.set_cell_data(r_idx, h_idx, txt, redraw=False)

        put_hour(from_h, from_dt)
        put_hour(to_h, to_dt)

    def _on_shared_vscroll(self, *args):
        """外置竖向滚动条驱动两个 Sheet 同步滚动"""
        try:
            if not args:
                return
            op = args[0]
            if op == "moveto":
                # args: ("moveto", fraction)
                frac = float(args[1])
                self.sheet.yview_moveto(frac)
                self.gantt_sheet.yview_moveto(frac)
            elif op == "scroll":
                # args: ("scroll", number, what), 例如 ("scroll", 1, "units") / ("scroll", -1, "pages")
                n = int(args[1])
                what = args[2]
                # 用“计算 fraction + moveto”的方式滚动
                self._scroll_by(self.sheet, n, what)
                self._scroll_by(self.gantt_sheet, n, what)
        finally:
            self._update_shared_vbar()

    def _y_scroll_units(self, n: int):
        """按单位行滚动（上下键/滚轮），n 正负分别代表下/上"""
        self._scroll_by(self.sheet, n, "units")
        self._scroll_by(self.gantt_sheet, n, "units")
        self._update_shared_vbar()

    def _y_scroll_pages(self, n: int):
        """按页滚动（PageUp/PageDown），n 正负分别代表下/上"""
        self._scroll_by(self.sheet, n, "pages")
        self._scroll_by(self.gantt_sheet, n, "pages")
        self._update_shared_vbar()

    def _scroll_by(self, sh, n: int, what: str = "units"):
        """
        用 yview() -> (first,last) + total_rows 来估算新的滚动位置，再 yview_moveto
        - sh: tksheet.Sheet
        - n: 正负方向
        - what: "units" | "pages"
        """
        try:
            first, last = sh.yview()  # 0~1
        except Exception:
            # 某些版本可能返回异常，兜底为顶部
            first, last = 0.0, 0.0

        total = getattr(sh, "get_total_rows", lambda: 0)()
        if total <= 0:
            sh.yview_moveto(0.0)
            return

        # 估算可视行数（至少为 1）
        visible_rows = max(1, int(round((last - first) * total))) if (last - first) > 0 else max(1, int(total * 0.1))
        # 当前 top 行
        top_row = int(round(first * total))

        if what == "units":
            top_row += n
        else:  # "pages"
            top_row += n * visible_rows

        # 边界裁剪：保证最后一页也能满屏（top_row + visible_rows <= total）
        top_row = max(0, min(top_row, max(0, total - visible_rows)))
        new_frac = 0.0 if total == 0 else top_row / float(total)
        sh.yview_moveto(new_frac)

    def _update_shared_vbar(self):
        """以右侧甘特为准，回写外置滚动条的位置区间"""
        try:
            first, last = self.gantt_sheet.yview()  # 期望返回 (first, last) 浮点数
            self._vbar.set(first, last)
        except Exception:
            # 某些版本若不支持取 yview 范围，忽略即可
            pass

    # -------------------------
    # 跨表同步（新增）
    # -------------------------
    def _bind_cross_sheet_sync(self) -> None:
        """绑定左->右的选择同步，以及共享纵向滚动条的滚动同步"""

        # —— 行选择同步（左驱右）
        def on_row_select(_=None):
            rows = self.sheet.get_selected_rows()
            self.gantt_sheet.deselect("all")
            for r in rows:
                self.gantt_sheet.select_row(r, redraw=False)
            self.gantt_sheet.redraw()

        self.sheet.extra_bindings("row_select", func=on_row_select)

        # —— 鼠标滚轮（Windows/macOS）
        def _on_mouse_wheel(event):
            # event.delta > 0 表示向上；按行滚动一步
            step = -1 if getattr(event, "delta", 0) > 0 else 1
            self._y_scroll_units(step)
            return "break"

        self.sheet.bind("<MouseWheel>", _on_mouse_wheel)
        self.gantt_sheet.bind("<MouseWheel>", _on_mouse_wheel)

        # —— 鼠标滚轮（Linux）
        self.sheet.bind("<Button-4>", lambda e: (self._y_scroll_units(-1), "break"))
        self.sheet.bind("<Button-5>", lambda e: (self._y_scroll_units(+1), "break"))
        self.gantt_sheet.bind("<Button-4>", lambda e: (self._y_scroll_units(-1), "break"))
        self.gantt_sheet.bind("<Button-5>", lambda e: (self._y_scroll_units(+1), "break"))

        # —— 键盘纵向滚动
        self.sheet.bind("<Up>", lambda e: self._y_scroll_units(-1))
        self.sheet.bind("<Down>", lambda e: self._y_scroll_units(+1))
        self.sheet.bind("<Prior>", lambda e: self._y_scroll_pages(-1))  # PageUp
        self.sheet.bind("<Next>", lambda e: self._y_scroll_pages(+1))  # PageDown

        self.gantt_sheet.bind("<Up>", lambda e: self._y_scroll_units(-1))
        self.gantt_sheet.bind("<Down>", lambda e: self._y_scroll_units(+1))
        self.gantt_sheet.bind("<Prior>", lambda e: self._y_scroll_pages(-1))
        self.gantt_sheet.bind("<Next>", lambda e: self._y_scroll_pages(+1))

        # —— 行拖拽后重绘右侧并刷新滚动条区间
        self.sheet.extra_bindings("row_drag_and_drop", func=lambda e: (
            self._render_gantt_rows_from_left(), self._update_shared_vbar()
        ))

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
        # === 新增：右侧甘特图按左侧当前数据重绘 ===
        if hasattr(self, "gantt_sheet"):
            self._render_gantt_rows_from_left()

    def _auto_adjust_column_widths(self):
        """根据窗口宽度自动调整左右两表列宽"""
        try:
            total_width = self.window.winfo_width()
            if total_width <= 0:
                return
            # 特殊列：类型、DT、产品、吨 -> 小宽度（两个中文字符 ≈ 40px）
            col_width_dict = {
                foh.order_id: 100,
                foh.order_type: 30,
                foh.corporate_id: 40,
                foh.product: 40,
                foh.shipto: 70,
                foh.cust_name: 140,
                foh.order_from : 120,
                foh.order_to : 120,
                foh.ton: 40,
                foh.comment: 120,
                foh.target_date: 120,
                foh.risk_date: 120,
                foh.run_out_date: 120,
            }

            for idx, col_name in enumerate(self.base_headers):
                self.sheet.column_width(column=idx, width=col_width_dict[col_name])

            # 右侧甘特图列宽（固定宽度，两个中文字符 ≈ 40px）
            gantt_col_width = 40
            for c in range(len(self.gantt_hours)):
                self.gantt_sheet.column_width(column=c, width=gantt_col_width)

        except Exception as e:
            print("调整列宽失败：", e)

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

    def _apply_dropdown_filter(self) -> None:
        """根据日期和下拉条件筛选订单"""
        try:
            start_date = dt.datetime.strptime(self.start_date_var.get(), "%Y-%m-%d").date()
            end_date = dt.datetime.strptime(self.end_date_var.get(), "%Y-%m-%d").date()
            if end_date < start_date:
                raise ValueError("结束日期不能早于开始日期")
        except ValueError:
            messagebox.showerror("错误", "日期格式不正确", parent=self.window)
            return

        # 下拉条件
        criteria = {k: v.get() for k, v in self.filter_vars.items() if v.get() != "全部"}

        # 过滤逻辑
        filtered = []
        for row in self._get_all_rows_from_source():
            if any(self._safe_to_str(row[self._idx(k)]) != val for k, val in criteria.items()):
                continue
            dt_obj = self._parse_display_dt(self._safe_to_str(row[self._idx(foh.order_to)]))
            if dt_obj and start_date <= dt_obj.date() <= end_date:
                filtered.append(row)

        self._render_rows(filtered)

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
        row, col, new_val = event["row"], event["column"], event["value"]
        col_name = self.base_headers[col]
        order_id = self.sheet.get_cell_data(row, self._idx(foh.order_id))
        order_type = self.sheet.get_cell_data(row, self._idx(foh.order_type))

        # 禁止修改 OO 订单
        if order_type == enums.OrderType.OO:
            messagebox.showerror("错误", "OO 订单不允许修改任何属性！", parent=self.window)
            self._restore_cell(row, col, order_id, order_type, col_name)
            return

        order = self.order_data_manager.forecast_order_dict.get(order_id)
        if not order:
            messagebox.showerror("错误", "未找到对应订单！", parent=self.window)
            return

        try:
            attr = constant.ORDER_ATTR_MAP.get(col_name)
            if col_name in [foh.order_from, foh.order_to]:
                new_dt = pd.to_datetime(new_val)
                if pd.isnull(new_dt):
                    raise ValueError("时间格式不正确")
                if col_name == foh.order_from and new_dt >= order.to_time:
                    raise ValueError("开始时间不能晚于结束时间")
                if col_name == foh.order_to and new_dt <= order.from_time:
                    raise ValueError("结束时间不能早于开始时间")
                setattr(order, attr, new_dt)
                new_val = new_dt.strftime("%Y/%m/%d %H:%M")

            elif col_name == foh.ton:
                ton = float(new_val)
                max_ton = self.data_manager.get_max_payload_value_by_ship2(order.shipto) / 1000
                if not (0 < ton <= max_ton):
                    raise ValueError(f"吨应在 0 和 {max_ton} 之间")
                setattr(order, attr, ton * 1000)

            elif col_name == foh.comment:
                setattr(order, attr, new_val)

            else:
                raise ValueError(f"{col_name} 列不支持编辑")

            # 更新数据和 UI
            self.order_data_manager.update_order_in_list(order)
            self.sheet.set_cell_data(row, col, new_val)

            # === 新增：只重绘该行甘特 ===
            if hasattr(self, "gantt_sheet"):
                row_data = self.sheet.get_row_data(row)
                self._render_one_gantt_row(row, row_data)
                self.gantt_sheet.redraw()

        except Exception as e:
            self._restore_cell(row, col, order_id, order_type, col_name)
            messagebox.showerror("错误", str(e), parent=self.window)

    def _restore_cell(self, row, col, order_id, order_type, col_name):
        """恢复单元格原始值"""
        if order_type == enums.OrderType.OO:
            order = self.order_data_manager.order_order_dict.get(order_id)
        else:
            order = self.order_data_manager.forecast_order_dict.get(order_id)
        if not order:
            return
        original = getattr(order, constant.ORDER_ATTR_MAP.get(col_name, ""), "")
        if isinstance(original, dt.datetime) and not pd.isnull(original):
            original = original.strftime("%Y/%m/%d %H:%M")
        if col_name == foh.ton:
            original = round(original / 1000, 1)
        self.sheet.set_cell_data(row, col, original)

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

        # 同步删除甘特行
        if hasattr(self, "gantt_sheet"):
            try:
                self.gantt_sheet.delete_row(row_index)
            except Exception:
                # 回退到全表重绘，保证一致性
                self._render_gantt_rows_from_left()

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

            # 同步甘特
            if hasattr(self, "gantt_sheet"):
                self._render_gantt_rows_from_left()

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
