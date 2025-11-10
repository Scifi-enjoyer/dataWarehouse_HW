import io
import os
import sqlite3
import threading
import tkinter as tk
from contextlib import redirect_stdout
from datetime import datetime
from tkinter import messagebox, ttk
from tkinter.scrolledtext import ScrolledText

import config
from database.create import create_database_schema
from index import run_full_etl_and_analysis_job
from src.analyzer import (
    analyze_high_consumption,
    analyze_waste,
    run_all_analyses,
)


class SmartHomeDashboard(tk.Tk):
    """Dashboard giám sát ETL + phân tích dữ liệu Smart Home."""

    def __init__(self):
        super().__init__()
        self.title("Smart Home DW Monitor")
        self.geometry("1100x720")

        self._current_worker = None
        self.recommendations = []
        self.metric_vars = {
            "total_records": tk.StringVar(value="0"),
            "today_records": tk.StringVar(value="0"),
            "last_record": tk.StringVar(value="Chưa có dữ liệu"),
            "last_power": tk.StringVar(value="--"),
            "last_energy": tk.StringVar(value="--"),
        }
        self.status_var = tk.StringVar(value="Sẵn sàng.")

        self._build_ui()
        self.refresh_metrics()
        self._append_log("Ứng dụng sẵn sàng. Nhấn nút để chạy ETL hoặc phân tích.", "success")

    # ------------------------------------------------------------------
    # UI BUILDERS
    # ------------------------------------------------------------------
    def _build_ui(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Card.TFrame", background="#f8fafc", relief="flat")
        style.configure("Card.TLabel", background="#f8fafc", font=("Segoe UI", 10))
        style.configure("Value.TLabel", background="#f8fafc", font=("Segoe UI", 20, "bold"))
        style.configure("Header.TLabel", font=("Segoe UI", 18, "bold"), foreground="#0f172a")
        style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"), padding=6)

        self.columnconfigure(0, weight=1)

        # Header
        header = ttk.Frame(self, padding=(20, 15))
        header.grid(row=0, column=0, sticky="ew")
        ttk.Label(header, text="Smart Home Data Warehouse Monitor", style="Header.TLabel").pack(anchor="w")
        ttk.Label(
            header,
            text="Theo dõi ETL, phân tích và theo dõi log theo thời gian thực.",
            font=("Segoe UI", 11),
            foreground="#475569",
        ).pack(anchor="w", pady=(4, 0))

        # Metrics
        metrics_frame = ttk.Frame(self, padding=(20, 0))
        metrics_frame.grid(row=1, column=0, sticky="ew")
        metrics_frame.columnconfigure((0, 1, 2), weight=1)

        cards = [
            ("Tổng bản ghi", "total_records"),
            ("Bản ghi hôm nay", "today_records"),
            ("Thời gian bản ghi mới nhất", "last_record"),
            ("Power gần nhất (W)", "last_power"),
            ("Energy gần nhất (Wh)", "last_energy"),
        ]

        for idx, (label, key) in enumerate(cards):
            card = ttk.Frame(metrics_frame, style="Card.TFrame", padding=12)
            card.grid(row=idx // 3, column=idx % 3, padx=6, pady=6, sticky="nsew")
            ttk.Label(card, text=label, style="Card.TLabel").pack(anchor="w")
            ttk.Label(card, textvariable=self.metric_vars[key], style="Value.TLabel").pack(anchor="w", pady=(6, 0))

        # Console log + actions
        info_actions = ttk.Frame(self, padding=(20, 10))
        info_actions.grid(row=2, column=0, sticky="nsew")
        info_actions.columnconfigure(0, weight=3)
        info_actions.columnconfigure(1, weight=1)
        info_actions.rowconfigure(0, weight=1)

        log_frame = ttk.LabelFrame(info_actions, text="Console log", padding=12)
        log_frame.grid(row=0, column=0, padx=(0, 10), sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = ScrolledText(log_frame, height=16, font=("Consolas", 10))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        self.log_text.configure(state="disabled")
        self.log_text.tag_config("info", foreground="#0f172a")
        self.log_text.tag_config("success", foreground="#059669")
        self.log_text.tag_config("warning", foreground="#d97706")
        self.log_text.tag_config("error", foreground="#dc2626")

        actions_frame = ttk.LabelFrame(info_actions, text="Tác vụ nhanh", padding=12)
        actions_frame.grid(row=0, column=1, sticky="nsew")
        actions_frame.columnconfigure((0, 1), weight=1)

        self.action_buttons = []
        buttons = [
            ("Chạy ETL ngay", lambda: self._run_action("ETL", run_full_etl_and_analysis_job)),
            ("Phân tích lãng phí", lambda: self._run_action("Phân tích lãng phí", analyze_waste)),
            ("Phân tích tiêu thụ cao", lambda: self._run_action("Phân tích tiêu thụ", analyze_high_consumption)),
            ("Chạy tất cả phân tích", lambda: self._run_action("Phân tích tổng", run_all_analyses)),
        ]

        for idx, (text, cmd) in enumerate(buttons):
            btn = ttk.Button(actions_frame, text=text, style="Accent.TButton", command=cmd)
            btn.grid(row=idx // 2, column=idx % 2, padx=4, pady=4, sticky="ew")
            self.action_buttons.append(btn)

        ttk.Button(actions_frame, text="Làm mới thống kê", command=self.refresh_metrics).grid(
            row=2, column=0, columnspan=2, padx=4, pady=(8, 4), sticky="ew"
        )
        ttk.Button(actions_frame, text="Reset database", command=self._confirm_reset_db).grid(
            row=3, column=0, columnspan=2, padx=4, pady=(4, 0), sticky="ew"
        )

        # Recommendations table (tăng chiều cao)
        rec_frame = ttk.LabelFrame(self, text="Khuyến nghị / Cảnh báo", padding=12)
        rec_frame.grid(row=3, column=0, padx=20, pady=(0, 10), sticky="nsew")
        rec_frame.columnconfigure(0, weight=1)
        rec_frame.rowconfigure(0, weight=1)

        columns = ("time", "action", "message")
        self.rec_tree = ttk.Treeview(rec_frame, columns=columns, show="headings", height=14)
        self.rec_tree.heading("time", text="Thời gian")
        self.rec_tree.heading("action", text="Nguồn")
        self.rec_tree.heading("message", text="Nội dung")
        self.rec_tree.column("time", width=150, anchor="center")
        self.rec_tree.column("action", width=160, anchor="w")
        self.rec_tree.column("message", anchor="w")
        self.rec_tree.grid(row=0, column=0, sticky="nsew")

        rec_scroll = ttk.Scrollbar(rec_frame, orient="vertical", command=self.rec_tree.yview)
        rec_scroll.grid(row=0, column=1, sticky="ns")
        self.rec_tree.configure(yscrollcommand=rec_scroll.set)

        # Status bar
        status_bar = ttk.Frame(self, padding=(20, 5))
        status_bar.grid(row=4, column=0, sticky="ew")
        ttk.Label(status_bar, textvariable=self.status_var, foreground="#475569").pack(anchor="w")

        self.rowconfigure(2, weight=1)
        self.rowconfigure(3, weight=2)

    # ------------------------------------------------------------------
    # ACTION HANDLERS
    # ------------------------------------------------------------------
    def _run_action(self, label, func):
        if self._current_worker and self._current_worker.is_alive():
            messagebox.showinfo("Đang bận", "Một tác vụ khác đang chạy. Vui lòng đợi hoàn tất.")
            return

        self._set_actions_state("disabled")
        self.status_var.set(f"Đang chạy: {label} ...")

        def worker():
            buffer = io.StringIO()
            result = None
            error = None

            try:
                with redirect_stdout(buffer):
                    result = func()
            except Exception as exc:
                error = exc

            log_output = buffer.getvalue().strip()
            self.after(0, lambda: self._finalize_action(label, log_output, result, error))

        self._current_worker = threading.Thread(target=worker, daemon=True)
        self._current_worker.start()

    def _finalize_action(self, label, log_output, result, error):
        self._set_actions_state("normal")
        self.status_var.set("Hoàn tất." if not error else f"Lỗi khi chạy: {label}")

        if log_output:
            self._append_log(log_output, "info")
        else:
            self._append_log(f"[{label}] Hoàn tất nhưng không có log.", "warning")

        if error:
            messagebox.showerror("Lỗi", f"{label} thất bại:\n{error}")
            self._append_log(str(error), "error")
        else:
            self._append_log(f"{label} hoàn tất.", "success")

        if isinstance(result, list) and result:
            self._update_recommendations(label, result)

        self.refresh_metrics()

    def _set_actions_state(self, state):
        for btn in self.action_buttons:
            btn.config(state=state)

    def _confirm_reset_db(self):
        should_reset = messagebox.askyesno(
            "Xác nhận reset",
            "Thao tác này sẽ xóa toàn bộ dữ liệu hiện có trong database.\nBạn chắc chắn muốn tiếp tục?",
        )
        if not should_reset:
            return

        try:
            if os.path.exists(config.DB_FILE):
                os.remove(config.DB_FILE)
            create_database_schema()
            self._append_log("Đã reset và tạo mới database.", "warning")
            self.refresh_metrics()
        except Exception as exc:
            messagebox.showerror("Reset thất bại", str(exc))
            self._append_log(str(exc), "error")

    # ------------------------------------------------------------------
    # DATA & UI HELPERS
    # ------------------------------------------------------------------
    def refresh_metrics(self):
        stats = self._fetch_metrics()
        for key, value in stats.items():
            if key in self.metric_vars:
                self.metric_vars[key].set(value)

        self.status_var.set("Đã cập nhật thống kê cơ sở dữ liệu.")

    def _fetch_metrics(self):
        data = {
            "total_records": "0",
            "today_records": "0",
            "last_record": "Chưa có dữ liệu",
            "last_power": "--",
            "last_energy": "--",
        }

        if not os.path.exists(config.DB_FILE):
            return data

        try:
            conn = sqlite3.connect(config.DB_FILE)
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*), MAX(created_at) FROM fact_measurement")
            total, last_ts = cur.fetchone()
            data["total_records"] = f"{total:,}"
            if last_ts:
                data["last_record"] = last_ts

            cur.execute(
                """
                SELECT created_at, power_w, energy_wh
                FROM fact_measurement
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            last_row = cur.fetchone()
            if last_row:
                _, power, energy = last_row
                data["last_power"] = f"{power:.2f}" if power is not None else "--"
                data["last_energy"] = f"{energy:.2f}" if energy is not None else "--"

            cur.execute(
                """
                SELECT COUNT(*)
                FROM fact_measurement
                WHERE date(created_at, 'localtime') = date('now', 'localtime')
                """
            )
            today_total = cur.fetchone()[0]
            data["today_records"] = f"{today_total:,}"

            conn.close()
        except sqlite3.Error as exc:
            self._append_log(f"Lỗi SQLite khi lấy thống kê: {exc}", "error")

        return data

    def _append_log(self, text, tag="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{timestamp}] {text}\n", tag)
        self.log_text.configure(state="disabled")
        self.log_text.see("end")

    def _update_recommendations(self, label, recs):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for rec in recs:
            self.recommendations.insert(0, (timestamp, label, rec.strip()))

        self.recommendations = self.recommendations[:40]

        for item in self.rec_tree.get_children():
            self.rec_tree.delete(item)

        for rec in self.recommendations:
            self.rec_tree.insert("", "end", values=rec)


def launch_app():
    """Hàm tiện ích để chạy GUI trực tiếp."""
    create_database_schema()
    app = SmartHomeDashboard()
    app.mainloop()


if __name__ == "__main__":
    launch_app()
