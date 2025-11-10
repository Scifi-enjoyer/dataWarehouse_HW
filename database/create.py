import sqlite3
import os
import sys

# Đảm bảo sys.path được thêm vào TRƯỚC khi import config
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import config # Import config để lấy DB_FILE

def create_database_schema():
    """
    Hàm chính để tạo cấu trúc kho dữ liệu (đã đơn giản hóa).
    """
    db_path = config.DB_FILE
    
    # Tạo database SQLite
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS fact_measurement;")
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_measurement (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        entry_id INT,
        power_w REAL,     -- Tên cũ: Power(W)
        energy_wh REAL,   -- Tên cũ: Energy(Wh) (Lưu ý: Wh không phải kWh)
        presence INT,     -- Tên cũ: Presence (0/1)
        state INT,        -- Tên cũ: State (0/1) (thay cho onoff)
        time_s REAL       -- Tên cũ: Time_s (s)
    )
    """)

    # ========================
    # 3. Lưu & đóng
    # ========================
    conn.commit()
    conn.close()

    print(f"✅ Mini Data Warehouse created at: {db_path}")

if __name__ == "__main__":
    print("Đang chạy create.py độc lập để tạo schema...")
    create_database_schema()