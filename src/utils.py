import requests
import pandas as pd
import os
import sys
import re
import sqlite3
from datetime import datetime

# SỬA LỖI IMPORT: Đưa khối sys.path lên TRƯỚC import config
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import config

# ===================================================================
# HÀM LẤY TIMESTAMP CUỐI CÙNG (Đã đơn giản hóa)
# ===================================================================
def get_last_timestamp():
    """
    Truy vấn DB để tìm timestamp (created_at) mới nhất.
    (Không cần channel_id nữa)
    """
    conn = sqlite3.connect(config.DB_FILE)
    cur = conn.cursor()
    try:
        # Lấy MAX(created_at) từ toàn bộ bảng
        query = "SELECT MAX(created_at) FROM fact_measurement"
        cur.execute(query)
        result = cur.fetchone()
        
        if result and result[0]:
            return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
        else:
            return None # DB trống
            
    except Exception as e:
        print(f"   [DB] ❌ Lỗi khi lấy last_timestamp: {e}")
        return None
    finally:
        conn.close()

# ===================================================================
# HÀM TRÍCH XUẤT JSON (Không đổi nhiều)
# ===================================================================

def fetch_json(channel_id, api_key="", start_time=None):
    """
    Lấy JSON từ ThingSpeak.
    Nếu có start_time, chỉ lấy dữ liệu mới hơn thời điểm đó.
    """
    url = f"https://api.thingspeak.com/channels/{channel_id}/feeds.json"
    
    params = {}
    if api_key:
        params["api_key"] = api_key
        
    if start_time:
        start_time_str = (start_time + pd.Timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
        params["start"] = start_time_str
        print(f"   [API] Lấy dữ liệu từ sau: {start_time_str}")
    else:
        print(f"   [API] DB trống, lấy {config.RESULTS} bản ghi đầu tiên...")
        params["results"] = config.RESULTS
    
    print(f"   [API] Đang gọi Channel {channel_id}...")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def json_to_df(js, fields=None):
    feeds = js.get("feeds", [])
    if not feeds:
        return pd.DataFrame()

    channel_info = js.get("channel", {})

    if not fields:
        sample = feeds[0]
        fields = [k for k in sample.keys() if k.startswith("field")]

    rename_map = {}
    for f in fields:
        if f in channel_info and channel_info[f]:
            rename_map[f] = channel_info[f].strip() # Xóa khoảng trắng thừa
        else:
            rename_map[f] = f

    rows = []
    for f in feeds:
        # Giữ lại entry_id
        row = {"created_at": f.get("created_at"), "entry_id": f.get("entry_id")}
        for fld in fields:
            row[fld] = f.get(fld)
        rows.append(row)
    df = pd.DataFrame(rows)

    df["created_at"] = pd.to_datetime(df["created_at"])
    for fld in fields:
        df[fld] = pd.to_numeric(df[fld], errors="coerce")
    
    df = df.rename(columns=rename_map)
    
    # Lấy tất cả các cột đã được đổi tên
    all_renamed_cols = list(rename_map.values())
    df = df[["created_at", "entry_id"] + all_renamed_cols] 
    return df

# ===================================================================
# HÀM NẠP VÀO DWH (Đã sửa để khớp DB mới)
# ===================================================================

def to_int_or_none(v):
    try:
        return int(v) if pd.notna(v) else None
    except (ValueError, TypeError):
        return None

def to_float_or_none(v):
    try:
        return float(v) if pd.notna(v) else None
    except (ValueError, TypeError):
        return None

CSV_TO_DB_MAP = {
    "created_at": "created_at",
    "entry_id": "entry_id",
    "Power (W)": "power_w",
    "Energy(Wh)": "energy_wh",
    "Presence (0/1)": "presence",
    "State (0/1)": "state",
    "Time_s (s)": "time_s"
}

def load_dataframe_to_dwh(df):
    """
    Nạp một DataFrame (chỉ chứa dữ liệu MỚI) vào DWH (bảng 7 cột).
    """
    print(f"--- [TL] Bắt đầu Transform & Load ---")

    conn = sqlite3.connect(config.DB_FILE)
    cur = conn.cursor()

    try:
        # Bước 1: Đổi tên cột DF để khớp với DB
        # Chỉ giữ lại các cột có trong bản đồ map
        df_renamed = df.rename(columns=CSV_TO_DB_MAP)
        
        # Lấy danh sách các cột DB mà chúng ta thực sự có
        db_cols_to_insert = [col for col in df_renamed.columns if col in CSV_TO_DB_MAP.values()]
        
        if len(db_cols_to_insert) < 2: # Ít nhất phải có created_at
             print(f"   [TL] ⚠️ Không tìm thấy cột nào khớp với bản đồ . Bỏ qua.")
             print(f"      Các cột tìm thấy: {list(df.columns)}")
             return False

        df_final = df_renamed[db_cols_to_insert]

        facts_to_insert = []
        print(f"   [TL] Chuẩn bị {len(df_final)} bản ghi MỚI...")
        
        for _, row in df_final.iterrows():
            # Tạo tuple theo đúng thứ tự 7 cột trong DB
            facts_to_insert.append((
                row.get("created_at").strftime("%Y-%m-%d %H:%M:%S") if pd.notna(row.get("created_at")) else None,
                to_int_or_none(row.get("entry_id")),
                to_float_or_none(row.get("power_w")),
                to_float_or_none(row.get("energy_wh")),
                to_int_or_none(row.get("presence")),
                to_int_or_none(row.get("state")),
                to_float_or_none(row.get("time_s"))
            ))

        if not facts_to_insert:
            print("   [DB] Không có bản ghi mới nào để nạp.")
            return True

        print(f"   [DB] Đang nạp {len(facts_to_insert)} bản ghi MỚI...")
        
        # Câu lệnh INSERT khớp với 7 cột
        cur.executemany("""
            INSERT INTO fact_measurement (
                created_at, entry_id, power_w, energy_wh,
                presence, state, time_s
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, facts_to_insert)
        
        conn.commit()
        print(f"   [DB] ✅ Đã nạp thành công {len(facts_to_insert)} bản ghi.")
        return True

    except sqlite3.Error as e:
        print(f"   [DB] ❌ Lỗi SQLite khi nạp dữ liệu: {e}")
        conn.rollback()
        return False
    except Exception as e:
         print(f"   [TL] ❌ Lỗi không xác định trong quá trình Transform/Load: {e}")
         print(f"      Kiểm tra lại tên cột trong CSV_TO_DB_MAP?")
         return False
    finally:
        if conn:
            conn.close()