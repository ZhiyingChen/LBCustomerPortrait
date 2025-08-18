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
        self.start_date_var.set(start_default.strftime("%y-%m-%d"))
        self.end_date_var.set(end_default.strftime("%y-%m-%d"))

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
        # ✅ 新增：排序对话框按钮
        tk.Button(func_frame, text="排\n序", command=self._open_sort_dialog,
                  bg="#EEE8AA", relief="raised", font=("Arial", 10)).pack(side='left', padx=5, pady=5)

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
        self.right_frame.pack(side="left", fill="both", expand=True)

        self._init_width_model()
        self._bind_column_width_events()
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
        self.window.bind("<Configure>", lambda e: self._adjust_frame_widths())
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
                return dt.datetime.strptime(s, "%y/%m/%d %H:%M")
            except Exception:
                # 若用户刚编辑，tksheet 可能临时是 pd.to_datetime 的字符串形式
                try:
                    return pd.to_datetime(s, format="%y/%m/%d %H:%M").to_pydatetime()
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

        all_three = (target_dt is not None and best_dt is not None and outage_dt is not None)

        if all_three:
            # 逻辑1

            # 前处理：找出最晚的时间
            if outage_dt < self.gantt_start_dt:
                outage_h = 0
            elif best_dt < self.gantt_start_dt:
                best_h = 0
            elif target_dt < self.gantt_start_dt:
                target_h = 0

            if target_h is not None:
                # 绿：[target(含), best(前1)]
                if best_h is None:
                    best_h = col_n
                paint(target_h, best_h - 1, GREEN)

            if best_h is not None:
                # 黄：[best(含), outage(前1)]
                if outage_h is None:
                    outage_h = col_n
                paint(best_h, outage_h - 1, YELLOW)
            if outage_h is not None:
                # 红：[outage(含), 末尾]
                paint(outage_h, col_n - 1, RED)
        else:
            # 逻辑2
            if to_dt < self.gantt_start_dt:
                to_dt = - 1

            if from_h is not None:
                # 绿：[from(含), to] —— 注：避免重叠冲突
                end_g = to_h if (to_h is not None) else (col_n - 1)
                paint(from_h, end_g, GREEN)
            if to_h is not None and to_h + 1 < col_n -1:
                # 红：[to+1, 末尾]
                paint(min(to_h + 1, col_n - 1), col_n - 1, RED)

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
        """
        1) 左->右 行选择同步
        2) 纵向滚动同步（鼠标/键盘/外置滚动条）
        3) 行拖拽完成 -> 等待 idle 后按左表“当前 UI 顺序”重绘右侧甘特
        4) 轮询兜底（跨版本/事件不触发时仍确保同步）
        """

        # --------------------------
        # 调试/兜底配置
        # --------------------------
        DEBUG = False  # 打开后打印触发日志与当前顺序
        SAFE_POLL = True  # 兜底轮询，确保无论如何都能同步
        POLL_MS = 250  # 轮询间隔（毫秒）

        # 保存上次“左表顺序的指纹”，用于判断是否变化
        self._last_left_digest = None

        # 计算当前左表“顺序指纹”（优先用订单ID列；没有就退化为整行内容）
        def _left_order_digest():
            try:
                oid_col = self._idx(foh.order_id)
                return tuple(self.sheet.get_cell_data(i, oid_col)
                             for i in range(self.sheet.get_total_rows()))
            except Exception:
                # 退化方案：比较整行值（更重但更稳）
                return tuple(
                    tuple(self.sheet.get_row_data(i))
                    for i in range(self.sheet.get_total_rows())
                )

        # 统一的“按左表当前 UI 顺序重绘右侧甘特”的函数
        def _sync_from_left(reason: str = "manual"):
            # 关键点：**逐行**按当前显示顺序读取
            rows = [self.sheet.get_row_data(i) for i in range(self.sheet.get_total_rows())]

            # 调整右侧行数
            left_n = len(rows)
            right_n = self.gantt_sheet.get_total_rows()
            if right_n < left_n:
                self.gantt_sheet.insert_rows(left_n - right_n)
            elif right_n > left_n:
                for r in range(right_n - 1, left_n - 1, -1):
                    self.gantt_sheet.delete_row(r)

            # 逐行重绘
            for r, row in enumerate(rows):
                self._render_one_gantt_row(r, row)
            self.gantt_sheet.redraw()

            # 同步选中状态
            sel = self.sheet.get_selected_rows()
            self.gantt_sheet.deselect("all")
            for r in sel:
                self.gantt_sheet.select_row(r, redraw=False)
            self.gantt_sheet.redraw()

            # 刷新共享滚动条
            self._update_shared_vbar()

            # 刷新当前顺序指纹
            self._last_left_digest = _left_order_digest()

            if DEBUG:
                print(f"[DEBUG] gantt synced ({reason}), rows={left_n}")

        # --------------------------
        # 1) 行选择同步（左驱右）
        # --------------------------
        def on_row_select(_=None):
            sel = self.sheet.get_selected_rows()
            self.gantt_sheet.deselect("all")
            for r in sel:
                self.gantt_sheet.select_row(r, redraw=False)
            self.gantt_sheet.redraw()
            if DEBUG:
                print(f"[DEBUG] on_row_select -> {sel}")

        self.sheet.extra_bindings("row_select", func=on_row_select)

        # --------------------------
        # 2) 纵向滚动同步（鼠标/键盘）
        # --------------------------
        def _on_mouse_wheel(event):
            step = -1 if getattr(event, "delta", 0) > 0 else 1
            self._y_scroll_units(step)
            # 异步刷新外置滚动条显示区间
            self.window.after_idle(self._update_shared_vbar)
            return "break"

        # Windows/macOS
        self.sheet.bind("<MouseWheel>", _on_mouse_wheel)
        self.gantt_sheet.bind("<MouseWheel>", _on_mouse_wheel)
        # Linux
        self.sheet.bind("<Button-4>",
                        lambda e: (self._y_scroll_units(-1), self.window.after_idle(self._update_shared_vbar), "break"))
        self.sheet.bind("<Button-5>",
                        lambda e: (self._y_scroll_units(+1), self.window.after_idle(self._update_shared_vbar), "break"))
        self.gantt_sheet.bind("<Button-4>", lambda e: (
        self._y_scroll_units(-1), self.window.after_idle(self._update_shared_vbar), "break"))
        self.gantt_sheet.bind("<Button-5>", lambda e: (
        self._y_scroll_units(+1), self.window.after_idle(self._update_shared_vbar), "break"))

        # 键盘
        self.sheet.bind("<Up>", lambda e: (self._y_scroll_units(-1), self.window.after_idle(self._update_shared_vbar)))
        self.sheet.bind("<Down>",
                        lambda e: (self._y_scroll_units(+1), self.window.after_idle(self._update_shared_vbar)))
        self.sheet.bind("<Prior>", lambda e: (
        self._y_scroll_pages(-1), self.window.after_idle(self._update_shared_vbar)))  # PageUp
        self.sheet.bind("<Next>", lambda e: (
        self._y_scroll_pages(+1), self.window.after_idle(self._update_shared_vbar)))  # PageDown

        self.gantt_sheet.bind("<Up>",
                              lambda e: (self._y_scroll_units(-1), self.window.after_idle(self._update_shared_vbar)))
        self.gantt_sheet.bind("<Down>",
                              lambda e: (self._y_scroll_units(+1), self.window.after_idle(self._update_shared_vbar)))
        self.gantt_sheet.bind("<Prior>",
                              lambda e: (self._y_scroll_pages(-1), self.window.after_idle(self._update_shared_vbar)))
        self.gantt_sheet.bind("<Next>",
                              lambda e: (self._y_scroll_pages(+1), self.window.after_idle(self._update_shared_vbar)))

        # --------------------------
        # 3) 行拖拽完成 -> 同步甘特（多事件名兼容）
        # --------------------------
        def on_row_drag_end(event=None):
            # 有些版本拖拽后需要 redraw 一次才能保证 UI/数据一致
            try:
                self.sheet.redraw()
            except Exception:
                pass
            # 等待 tksheet 完成内部重排后再读取数据
            self.window.after_idle(lambda: _sync_from_left("row_drag_end"))

            if DEBUG:
                try:
                    oid_col = self._idx(foh.order_id)
                    left_ids = [self.sheet.get_cell_data(i, oid_col) for i in range(self.sheet.get_total_rows())]
                    print(f"[DEBUG] after_drag order ids -> {left_ids}")
                except Exception as ex:
                    print(f"[DEBUG] after_drag: cannot dump ids: {ex}")

        # 尝试绑定多个可能的事件名（不同版本 tksheet 命名不同）
        def _try_bind_extra(name: str):
            try:
                self.sheet.extra_bindings(name, func=on_row_drag_end)
                if DEBUG:
                    print(f"[DEBUG] bound extra: {name}")
            except Exception:
                if DEBUG:
                    print(f"[DEBUG] extra not available: {name}")

        for evt_name in ("end_move_rows", "row_drag_and_drop", "end_row_move", "move_rows", "row_move"):
            _try_bind_extra(evt_name)

        # 一些版本提供虚拟事件（如果支持就顺手绑定）
        try:
            self.sheet.bind("<<SheetModified>>",
                            lambda e: self.window.after_idle(lambda: _sync_from_left("<<SheetModified>>")))
            if DEBUG:
                print("[DEBUG] bound virtual <<SheetModified>>")
        except Exception:
            pass

        # --------------------------
        # 4) 轮询兜底（事件不触发也能同步）
        # --------------------------
        if SAFE_POLL:
            def _poll_changes():
                try:
                    cur = _left_order_digest()
                    if self._last_left_digest is None:
                        # 首次记录
                        self._last_left_digest = cur
                    elif cur != self._last_left_digest:
                        _sync_from_left("poll")
                    else:
                        # 保持外置滚动条状态更新的稳定性
                        self._update_shared_vbar()
                finally:
                    if not getattr(self, "closed", False):
                        self.window.after(POLL_MS, _poll_changes)

            # 延迟启动，待窗口稳定后
            self.window.after(300, _poll_changes)

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
            return dt.datetime.strptime(s.strip(), "%y/%m/%d %H:%M")
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
                    value = value.strftime("%y/%m/%d %H:%M")
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
        self._auto_adjust_column_widths()
        # === 新增：右侧甘特图按左侧当前数据重绘 ===
        if hasattr(self, "gantt_sheet"):
            self._render_gantt_rows_from_left()

    def _adjust_frame_widths(self):
        total_width = self.window.winfo_width()
        half_width = total_width // 2
        self.left_frame.config(width=half_width)
        self.right_frame.config(width=half_width)

    def _auto_adjust_column_widths(self, event=None):
        """
        以“当前UI可见列”作为单一真相来源，应用列宽模型（用户覆盖 > 默认）。
        在窗口尺寸变化、数据重绘、隐藏/显示列之后都可以调用。
        """
        try:
            # 延迟到 idle，避免与 tksheet 内部刷新冲突
            self.window.after_idle(self._reapply_column_widths)
        except Exception as e:
            print("调整列宽失败：", e)

    # === 工具：安全按基础列名取值（不会受隐藏列影响） ===
    def _row_value(self, row_index: int, header_name: str):
        """始终用全宽行数据 + base_headers 索引取值，避免隐藏列导致的显示列错位"""
        row = self.sheet.get_row_data(row_index)  # 全列宽
        return row[self._idx(header_name)]

    # === 工具：把“显示列索引”翻译成“基础列名/基础列索引” ===
    def _display_col_to_base_col(self, display_col: int):
        """
        输入：当前 UI 下的显示列索引（如 end_edit_cell 事件里的 column）
        输出：(基础列名, 基础列索引)
        说明：会清理排序箭头后缀，比如 '订单开始↑1' -> '订单开始'
        """
        hdrs = self.sheet.headers()  # 当前显示的表头（可能带箭头、且已去除隐藏列）
        if display_col < 0 or display_col >= len(hdrs):
            raise IndexError("display_col 超出范围")
        import re as _re
        clean_name = _re.sub(r"\s*[↑↓]\d+$", "", hdrs[display_col])
        return clean_name, self._idx(clean_name)

    # ========= 列宽模型 =========
    def _init_width_model(self):
        # 默认宽度
        self._default_col_widths = {
            foh.order_id: 100,
            foh.order_type: 30,
            foh.corporate_id: 40,
            foh.product: 40,
            foh.shipto: 70,
            foh.cust_name: 140,
            foh.order_from: 110,
            foh.order_to: 110,
            foh.ton: 40,
            foh.comment: 120,
            foh.target_date: 110,
            foh.risk_date: 110,
            foh.run_out_date: 110,
        }
        self._user_col_widths = {}  # {列名(无箭头): 宽度}

    def _clean_header_text(self, h: str) -> str:
        # 去掉排序箭头等后缀，保持与 base_headers 的名字一致
        return re.sub(r"\s*[↑↓]\d+$", "", h or "")

    def _current_display_headers(self) -> List[str]:
        # 以“当前UI显示的头部”作为列顺序与集合的唯一来源
        return [self._clean_header_text(h) for h in (self.sheet.headers() or []) if h not in self.hidden_column_indices]

    def _get_target_width(self, col_name: str) -> int:
        # 用户覆盖优先，否则用默认
        return int(self._user_col_widths.get(col_name, self._default_col_widths.get(col_name, 80)))

    def _reapply_column_widths(self):
        """按当前可见列顺序，应用目标宽度（用户覆盖 > 默认）"""
        try:
            headers_display = self._current_display_headers()
            for idx, col_name in enumerate(headers_display):
                if idx in self.hidden_column_indices:
                    continue
                self.sheet.column_width(column=idx, width=self._get_target_width(col_name))
            # 甘特图固定
            if hasattr(self, "gantt_sheet"):
                gantt_col_width = 40
                for c in range(len(getattr(self, "gantt_hours", []))):
                    self.gantt_sheet.column_width(column=c, width=gantt_col_width)
            self.sheet.redraw()
        except Exception as e:
            print("应用列宽失败：", e)

    def _capture_current_widths(self):
        """
        读取当前可见列的宽度，存储到 _user_col_widths（用于记住用户拖动后的宽度）。
        """
        try:
            headers_display = self._current_display_headers()
            for idx, col_name in enumerate(headers_display):
                # tksheet 的 column_width(column=idx) 返回该列当前宽度
                w = self.sheet.column_width(column=idx)
                if isinstance(w, (int, float)) and w > 0:
                    self._user_col_widths[col_name] = int(w)
        except Exception as e:
            print("读取列宽失败：", e)

    def _bind_column_width_events(self):
        """
        监听列宽变化事件，自动记忆用户宽度。
        不同版本 tksheet 事件名不同，做兼容尝试。
        """

        def on_width_change(_=None):
            # 结束调整后记录，并再次应用（确保一致）
            self._capture_current_widths()
            # 延迟到 idle 再应用，避免被内部刷新覆盖
            self.window.after_idle(self._reapply_column_widths)

        for evt in ("end_resize_columns", "end_change_columns_width", "column_width_resize"):
            try:
                self.sheet.extra_bindings(evt, func=on_width_change)
                # 绑定到一个事件即可；如果想多重兜底，也可以全绑定
            except Exception:
                pass

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
            start_date = dt.datetime.strptime(self.start_date_var.get(), "%y-%m-%d").date()
            end_date = dt.datetime.strptime(self.end_date_var.get(), "%y-%m-%d").date()
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
        self.start_date_var.set(start_default.strftime("%y-%m-%d"))
        self.end_date_var.set(end_default.strftime("%y-%m-%d"))

        # 渲染全量
        self._render_rows(self._get_all_rows_from_source())
        self._update_filter_options()


    # -------------------------
    # 列隐藏/显示
    # -------------------------
    def _hide_columns(self):
        available_names = [h for i, h in enumerate(self.base_headers) if i not in self.hidden_column_indices]
        selected = self._ask_multiple_columns(available_names, "选择要隐藏的列")
        if selected:
            indices = [self._idx(name) for name in selected]
            self.sheet.hide_columns(indices)
            for idx in indices:
                if idx not in self.hidden_column_indices:
                    self.hidden_column_indices.append(idx)

            # 记录一次当前（隐藏前后）宽度，再延迟应用
            self._capture_current_widths()
            self.window.after_idle(self._auto_adjust_column_widths)  # 用统一入口
            self.sheet.redraw()

    def _show_columns(self):
        if not self.hidden_column_indices:
            messagebox.showinfo(title="提示", message="没有隐藏的列")
            return
        hidden_names = [self.base_headers[i] for i in self.hidden_column_indices]
        selected = self._ask_multiple_columns(hidden_names, "选择要显示的列")
        if selected:
            indices = [self._idx(name) for name in selected]
            self.sheet.show_columns(indices)
            self.hidden_column_indices = [i for i in self.hidden_column_indices if i not in indices]

            # 同样：记录 -> 延迟应用 -> 重绘
            self._capture_current_widths()
            self.window.after_idle(self._auto_adjust_column_widths)
            self.sheet.redraw()

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
        row, display_col, new_val = event["row"], event["column"], event["value"]

        # 将“显示列索引”映射为“基础列名/基础列索引”
        col_name, base_col = self._display_col_to_base_col(display_col)

        # 用全宽行数据 + 基础列索引获取其他列值（不受隐藏列影响）
        row_data = self.sheet.get_row_data(row)
        order_id = row_data[self._idx(foh.order_id)]
        order_type = row_data[self._idx(foh.order_type)]

        # 禁止修改 OO 订单
        if order_type == enums.OrderType.OO:
            messagebox.showerror("错误", "OO 订单不允许修改任何属性！", parent=self.window)
            self._restore_cell(row, display_col, order_id, order_type, col_name)
            return

        order = self.order_data_manager.forecast_order_dict.get(order_id)
        if not order:
            messagebox.showerror("错误", "未找到对应订单！", parent=self.window)
            return
        try:
            attr = constant.ORDER_ATTR_MAP.get(col_name)
            if col_name in [foh.order_from, foh.order_to]:
                new_dt = pd.to_datetime(new_val, format="%y/%m/%d %H:%M")
                if pd.isnull(new_dt):
                    raise ValueError("时间格式不正确")
                if col_name == foh.order_from and new_dt >= order.to_time:
                    raise ValueError("开始时间不能晚于结束时间")
                if col_name == foh.order_to and new_dt <= order.from_time:
                    raise ValueError("结束时间不能早于开始时间")
                setattr(order, attr, new_dt)
                new_val = new_dt.strftime("%y/%m/%d %H:%M")

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

            # 更新数据和 UI（注意：set_cell_data 用显示列索引）
            self.order_data_manager.update_order_in_list(order)
            self.sheet.set_cell_data(row, display_col, new_val)

            # 仅重绘该行甘特
            if hasattr(self, "gantt_sheet"):
                fresh_row_data = self.sheet.get_row_data(row)  # 拿最新的全宽行
                self._render_one_gantt_row(row, fresh_row_data)
                self.gantt_sheet.redraw()

        except Exception as e:
            self._restore_cell(row, display_col, order_id, order_type, col_name)
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
            original = original.strftime("%y/%m/%d %H:%M")
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

        to_delete = []
        for row_index in range(self.sheet.get_total_rows()):
            row_data = self.sheet.get_row_data(row_index)  # 全列宽
            order_id = row_data[self._idx(foh.order_id)]
            order_type = row_data[self._idx(foh.order_type)]
            to_delete.append((order_id, order_type, row_index))

        # 从底部删，避免索引位移
        for order_id, order_type, row_index in sorted(to_delete, key=lambda x: x[2], reverse=True):
            self.delete_order(order_id, order_type, row_index)

        self._update_filter_options()

    def _delete_selected_row(self, event=None):
        selected_row = self.sheet.get_selected_rows()
        if not selected_row:
            return

        selected_order_lt = sorted([
            (
                self.sheet.get_row_data(row_index)[self._idx(foh.order_id)],
                self.sheet.get_row_data(row_index)[self._idx(foh.order_type)],
                row_index
            )
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
            from_time = pd.to_datetime(self.sheet.get_cell_data(row_index, from_time_col), format="%y/%m/%d %H:%M")
            to_time = pd.to_datetime(self.sheet.get_cell_data(row_index, to_time_col), format="%y/%m/%d %H:%M")
            drop_ton = self.sheet.get_cell_data(row_index, drop_ton_col)
            comment = self.sheet.get_cell_data(row_index, comment_col) or ""

            comment_suffix = f"，{comment}" if comment else ""
            simple_order_string = "{}({}号{}点-{}{}点，{}吨{})".format(
                cust_name,
                from_time.strftime('%d'),
                from_time.strftime('%H'),
                '{}号'.format(to_time.strftime('%d')) if from_time.date() != to_time.date() else '',
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

    def _open_sort_dialog(self):
        """打开“多列排序”对话框：选择列、顺序，并可添加多层级排序。"""
        # 如果没有行，给出提示
        if self.sheet.get_total_rows() == 0:
            messagebox.showinfo(title="提示", message="当前没有数据可排序。", parent=self.window)
            return

        # 弹窗居中
        self.window.update_idletasks()
        width, height = 500, 420
        x = self.window.winfo_x() + (self.window.winfo_width() - width) // 2
        y = self.window.winfo_y() + (self.window.winfo_height() - height) // 3

        dlg = tk.Toplevel(self.window)
        dlg.title("多列排序")
        dlg.geometry(f"{width}x{height}+{x}+{y}")
        dlg.transient(self.window)
        dlg.grab_set()

        # 候选列：使用你的 base_headers（也可只用“当前可见”列）
        # 若你只想让“可见列”可选，可以用：
        # headers_display = [h for i,h in enumerate(self.base_headers) if i not in self.hidden_column_indices]
        headers_all = self.base_headers[:]

        # 当前条件列表（每个元素：{'col': 列名, 'dir': 'asc'|'desc'})
        levels: List[dict] = []

        # 上方：选择一个条件（列 + 方向）
        pick_frame = tk.LabelFrame(dlg, text="新增排序条件")
        pick_frame.pack(fill="x", padx=10, pady=10)

        tk.Label(pick_frame, text="列名：").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        col_var = tk.StringVar(value=headers_all[0])
        col_cb = ttk.Combobox(pick_frame, textvariable=col_var, state="readonly", width=18, values=headers_all)
        col_cb.grid(row=0, column=1, padx=5, pady=5, sticky="w")

        tk.Label(pick_frame, text="顺序：").grid(row=0, column=2, padx=5, pady=5, sticky="e")
        dir_var = tk.StringVar(value="升序")
        dir_cb = ttk.Combobox(pick_frame, textvariable=dir_var, state="readonly", width=8, values=["升序", "降序"])
        dir_cb.grid(row=0, column=3, padx=5, pady=5, sticky="w")

        def add_level():
            c = col_var.get()
            d = "asc" if dir_var.get() == "升序" else "desc"
            if not c:
                return
            levels.append({"col": c, "dir": d})
            refresh_list()

        tk.Button(pick_frame, text="添加", command=add_level, width=10).grid(row=0, column=4, padx=8, pady=5)

        # 中部：已选的排序层级（Listbox + 操作按钮）
        list_frame = tk.LabelFrame(dlg, text="排序优先级（上->下）")
        list_frame.pack(fill="both", expand=True, padx=10, pady=5)

        lb = tk.Listbox(list_frame, height=10)
        lb.pack(side="left", fill="both", expand=True, padx=(10, 0), pady=10)

        sb = tk.Scrollbar(list_frame, orient="vertical", command=lb.yview)
        sb.pack(side="left", fill="y", padx=(0, 10), pady=10)
        lb.config(yscrollcommand=sb.set)

        btns_frame = tk.Frame(list_frame)
        btns_frame.pack(side="left", fill="y", padx=10, pady=10)

        def refresh_list():
            lb.delete(0, tk.END)
            for i, it in enumerate(levels, start=1):
                zh_dir = "升序" if it["dir"] == "asc" else "降序"
                lb.insert(tk.END, f"{i}. {it['col']}（{zh_dir}）")

        def get_sel_index() -> Optional[int]:
            sel = lb.curselection()
            return sel[0] if sel else None

        def move_up():
            i = get_sel_index()
            if i is None or i <= 0:
                return
            levels[i - 1], levels[i] = levels[i], levels[i - 1]
            refresh_list()
            lb.selection_set(i - 1)

        def move_down():
            i = get_sel_index()
            if i is None or i >= len(levels) - 1:
                return
            levels[i + 1], levels[i] = levels[i], levels[i + 1]
            refresh_list()
            lb.selection_set(i + 1)

        def remove_sel():
            i = get_sel_index()
            if i is None:
                return
            levels.pop(i)
            refresh_list()

        def clear_all():
            levels.clear()
            refresh_list()

        tk.Button(btns_frame, text="上移", width=8, command=move_up).pack(pady=4)
        tk.Button(btns_frame, text="下移", width=8, command=move_down).pack(pady=4)
        tk.Button(btns_frame, text="删除", width=8, command=remove_sel).pack(pady=4)
        tk.Button(btns_frame, text="清空", width=8, command=clear_all).pack(pady=4)

        # 底部：确认/取消
        action_frame = tk.Frame(dlg)
        action_frame.pack(fill="x", padx=10, pady=10)

        def on_ok():
            if not levels:
                messagebox.showinfo("提示", "请至少添加一个排序条件。", parent=dlg)
                return
            # 转换为 (col_index, 'asc'/'desc')
            sort_specs: List[Tuple[int, str]] = []
            for it in levels:
                try:
                    sort_specs.append((self._idx(it["col"]), it["dir"]))
                except ValueError:
                    messagebox.showerror("错误", f"未知列：{it['col']}", parent=dlg)
                    return
            dlg.destroy()
            self._apply_multi_sort(sort_specs)

        def on_cancel():
            dlg.destroy()

        ttk.Button(action_frame, text="确定", command=on_ok).pack(side="right", padx=5)
        ttk.Button(action_frame, text="取消", command=on_cancel).pack(side="right", padx=5)

    def _apply_multi_sort(self, sort_specs: List[Tuple[int, str]]):
        """
        多列排序：sort_specs 形如 [(col_idx, 'asc'|'desc'), ...]，优先级按列表顺序。
        使用稳定排序，从“次优先级”到“最高优先级”依次排序。
        """
        total = self.sheet.get_total_rows()
        if total <= 0:
            return

        # 保存选中行（通过订单号恢复）
        try:
            oid_col = self._idx(foh.order_id)
        except Exception:
            oid_col = None
        selected_ids = set()
        if oid_col is not None:
            try:
                for r in self.sheet.get_selected_rows():
                    selected_ids.add(self.sheet.get_cell_data(r, oid_col))
            except Exception:
                pass
        # 读取当前 UI 顺序（逐行，最稳）
        rows = [self.sheet.get_row_data(i) for i in range(total)]

        # 按类型定义 key（升序方向）；降序使用 reverse=True
        time_cols = {foh.order_from, foh.order_to, foh.target_date, foh.risk_date, foh.run_out_date}
        num_cols = {foh.ton}

        def parse_dt_cell(s: str) -> Optional[dt.datetime]:
            if not s:
                return None
            try:
                return dt.datetime.strptime(s.strip(), "%y/%m/%d %H:%M")
            except Exception:
                return None

        def key_for(col_idx: int):
            col_name = self.base_headers[col_idx]
            if col_name in time_cols:
                def _k(row):
                    v = row[col_idx]
                    dv = parse_dt_cell(str(v))
                    # (是否为空, 实际值) —— 空值永远靠后
                    return (dv is None, dv or dt.datetime.min)

                return _k
            elif col_name in num_cols:
                def _k(row):
                    v = row[col_idx]
                    try:
                        return (False, float(v))
                    except Exception:
                        return (True, float("inf"))

                return _k
            else:
                def _k(row):
                    v = row[col_idx]
                    s = "" if v is None else str(v)
                    return (False, s.casefold())

                return _k

        # 稳定排序：从“最后一个条件”到“第一个条件”依次 sort
        for col_idx, direction in reversed(sort_specs):
            rows.sort(key=key_for(col_idx), reverse=(direction == "desc"))

        # 渲染
        self._render_rows(rows)

        # 恢复隐藏列（如需要）
        if getattr(self, "hidden_column_indices", None):
            try:
                self.sheet.hide_columns(self.hidden_column_indices)
                self.sheet.redraw()
            except Exception:
                pass
        # 恢复选中
        if oid_col is not None and selected_ids:
            try:
                self.sheet.deselect("all")
                for i, row in enumerate(rows):
                    if row[oid_col] in selected_ids:
                        self.sheet.select_row(i, redraw=False)
                self.sheet.redraw()
            except Exception:
                pass
        # 可选：保存当前排序规格（便于刷新/过滤后重用）
        self._current_sort_specs = sort_specs
        self.window.after_idle(self._auto_adjust_column_widths)

    def _reapply_saved_sort_if_any(self):
        specs = getattr(self, "_current_sort_specs", None)
        if specs:
            # 避免再次 _render_rows 引起的递归，这里直接重排当前数据再 set
            rows = [self.sheet.get_row_data(i) for i in range(self.sheet.get_total_rows())]

            time_cols = {foh.order_from, foh.order_to, foh.target_date, foh.risk_date, foh.run_out_date}
            num_cols = {foh.ton}

            def parse_dt_cell(s: str) -> Optional[dt.datetime]:
                if not s:
                    return None
                try:
                    return dt.datetime.strptime(s.strip(), "%y/%m/%d %H:%M")
                except Exception:
                    return None

            def key_for(col_idx: int):
                col_name = self.base_headers[col_idx]
                if col_name in time_cols:
                    return lambda row: ((parse_dt_cell(str(row[col_idx])) is None),
                                        parse_dt_cell(str(row[col_idx])) or dt.datetime.min)
                elif col_name in num_cols:
                    def _kn(row):
                        try:
                            return (False, float(row[col_idx]))
                        except Exception:
                            return (True, float("inf"))

                    return _kn
                else:
                    return lambda row: (False, ("" if row[col_idx] is None else str(row[col_idx])).casefold())

            for col_idx, direction in reversed(specs):
                rows.sort(key=key_for(col_idx), reverse=(direction == "desc"))

            # 直接 set，不再递归调用 _render_rows
            self.sheet.set_sheet_data(rows)
            if hasattr(self, "gantt_sheet"):
                self._render_gantt_rows_from_left()
            if getattr(self, "hidden_column_indices", None):
                self.sheet.hide_columns(self.hidden_column_indices)
            self.sheet.redraw()
            self.window.after_idle(self._auto_adjust_column_widths)

    # -------------------------
    # 刷新 OO 订单
    # -------------------------
    def refresh_oo_order(self):
        try:
            self.order_data_manager.refresh_oo_list()

            # 1) 删除 UI 中 order_type = OO 的行 —— 用“全宽行 + 基础索引”判定
            order_type_col = self._idx(foh.order_type)
            for row_index in reversed(range(self.sheet.get_total_rows())):
                row_data = self.sheet.get_row_data(row_index)  # ✅ 全列宽
                order_type_val = row_data[order_type_col]
                if order_type_val == enums.OrderType.OO:
                    self.sheet.delete_row(row_index)

            # 2) 取剩余 UI 顺序的“全宽数据”
            current_full_rows = [self.sheet.get_row_data(i) for i in range(self.sheet.get_total_rows())]

            # 3) 生成新的 OO 行（全列宽）
            oo_rows = self._order_to_rows(list(self.order_data_manager.order_order_dict.values()))

            # 4) 合并并渲染（列宽一致，不会吞列）
            current_full_rows.extend(oo_rows)
            self._render_rows(current_full_rows)

            # 5) 排序 + 下拉刷新
            self._reapply_saved_sort_if_any()
            self._update_filter_options()
            self.window.after_idle(self._auto_adjust_column_widths)

            messagebox.showinfo(title="提示", message="OO订单刷新成功！", parent=self.window)
        except Exception as e:
            messagebox.showerror(title="错误", message='刷新OO数据失败：' + str(e), parent=self.window)

    # -------------------------
    # 关闭
    # -------------------------
    def _on_close(self):
        self.closed = True
        self.window.destroy()
