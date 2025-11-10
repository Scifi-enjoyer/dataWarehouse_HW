import pandas as pd
import sqlite3
import os
import sys
from datetime import datetime, timedelta

# Thêm thư mục gốc vào sys.path để import config
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import config # Import config để lấy DB_FILE

# --- Các Ngưỡng Phân Tích (Đã sửa theo yêu cầu) ---
STREAK_TARGET = 120       
LONG_DURATION_SECONDS = 14400 
HIGH_ENERGY_THRESHOLD_WH = 100 # Ngưỡng Wh

def _get_db_connection():
    """Hàm helper để kết nối DB"""
    return sqlite3.connect(config.DB_FILE)

# ===================================================================
# HÀM LUẬT 1: PHÂN TÍCH LÃNG PHÍ (Logic quét 30-streak)
# ===================================================================
def analyze_waste():
    """
    Luật 1 (Logic mới): Quét các bản ghi TRONG NGÀY HÔM NAY.
    Nếu tìm thấy 30 bản ghi LIÊN TIẾP (state=1, presence=0) thì cảnh báo.
    """
    print(f"\n--- 1. Phân tích Lãng phí---")
    recommendations = []
    
    try:
        conn = _get_db_connection()
        
        # Lấy TẤT CẢ bản ghi của ngày hôm nay, sắp xếp từ cũ đến mới
        query = """
        SELECT created_at, state, presence
        FROM fact_measurement
        WHERE date(created_at, 'localtime') = date('now', 'localtime')
        ORDER BY created_at ASC;
        """
        
        # Dùng 'itertuples' để duyệt hiệu quả, không cần nhiều RAM
        df = pd.read_sql_query(query, conn, parse_dates=['created_at'])
        conn.close()

        if df.empty:
            print("   (Không có dữ liệu hôm nay để phân tích)")
            return recommendations

        bad_streak_counter = 0
        streak_start_time = None

        print(f"   🔎 Đang quét {len(df)} bản ghi của hôm nay...")
        # Duyệt qua dữ liệu TỪ CŨ ĐẾN MỚI
        for row in df.itertuples():
            # (row[0] là Index, row[1] là created_at, row[2] là state, row[3] là presence)
            is_bad_state = (row.state == 1 and row.presence == 0)

            if is_bad_state:
                # Nếu là trạng thái xấu, bắt đầu đếm
                if bad_streak_counter == 0:
                    streak_start_time = row.created_at # Ghi lại thời điểm bắt đầu chuỗi
                bad_streak_counter += 1
            
            else:
                # Nếu gặp trạng thái reset, đặt lại bộ đếm
                bad_streak_counter = 0
                streak_start_time = None
            
            # Kiểm tra xem đã đạt mục tiêu 30 chưa
            if bad_streak_counter == STREAK_TARGET:
                streak_end_time = row.created_at
                rec = (
                    f"   ❗️ CẢNH BÁO: Đèn bật không người ! "
                    f"Từ {streak_start_time} đến {streak_end_time}."
                )
                print(rec)
                recommendations.append(rec)
                
                # Reset bộ đếm để tìm chuỗi tiếp theo
                bad_streak_counter = 0
                streak_start_time = None
        
        if not recommendations:
             print("   (Không phát hiện lãng phí nào trong ngày hôm nay.)")

    except sqlite3.Error as e:
         print(f"   ❌ Lỗi SQLite khi phân tích lãng phí: {e}")
    except Exception as e:
         print(f"   ❌ Lỗi Pandas/Python: {e}")
            
    return recommendations
# # ===================================================================
# # HÀM LUẬT 2: PHÂN TÍCH BẬT QUÁ LÂU (Đã sửa query)
# # ===================================================================
# def analyze_long_duration():
#     """
#     Luật 2 (Sửa đổi): Kiểm tra bản ghi MỚI NHẤT.
#     Nếu state=1 VÀ time_s > 4 giờ (14400 giây).
#     """
#     print(f"\n--- 2. Phân tích Bật quá lâu (> {LONG_DURATION_SECONDS / 3600:.0f} giờ) ---")
#     recommendations = []
    
#     try:
#         conn = _get_db_connection()
#         # ✅ SỬA QUERY: Không JOIN nữa, chỉ lấy 1 bản ghi mới nhất
#         query = """
#         SELECT created_at, state, time_s
#         FROM fact_measurement
#         ORDER BY created_at DESC
#         LIMIT 1;
#         """
#         df = pd.read_sql_query(query, conn)
#         conn.close()

#         if df.empty:
#             print("   (Không có dữ liệu để phân tích)")
#             return recommendations

#         row = df.iloc[0]
#         is_on = (row['state'] == 1)
#         time_s_duration = row['time_s'] if pd.notna(row['time_s']) else 0
        
#         print(f"   🔎 Trạng thái mới nhất: state={int(row['state'])}, "
#               f"time_s={time_s_duration:.0f}s")

#         if is_on and (time_s_duration > LONG_DURATION_SECONDS):
#             hours_on = round(time_s_duration / 3600, 1)
#             rec = f"   ⚠️ CẢNH BÁO: Đèn đã bật liên tục {hours_on} giờ. Bạn có quên tắt không?"
#             print(rec)
#             recommendations.append(rec)
#         else:
#             print("      (Trạng thái OK)")
                
#     except sqlite3.Error as e:
#         print(f"   ❌ Lỗi SQLite khi phân tích bật lâu: {e}")
#     except Exception as e:
#          print(f"   ❌ Lỗi Pandas/Python: {e}")
        
#     return recommendations

# ===================================================================
# HÀM LUẬT 3: PHÂN TÍCH TIÊU THỤ CAO (✅ SỬA LOGIC THEO YÊU CẦU)
# ===================================================================
def analyze_high_consumption():
    """
    Luật 3 (Sửa đổi): Quét dữ liệu hôm nay, tìm các chuỗi state=1.
    Trong mỗi chuỗi, tìm MAX(energy_wh) và so sánh với ngưỡng.
    """
    print(f"\n--- 3. Phân tích Tiêu thụ cao (Tìm MAX(energy_wh) trong chuỗi 'state=1') ---")
    recommendations = []
    
    try:
        conn = _get_db_connection()
        
        # Lấy TẤT CẢ bản ghi của ngày hôm nay, sắp xếp từ cũ đến mới
        query = f"""
        SELECT created_at, state, energy_wh
        FROM fact_measurement
        WHERE date(created_at, 'localtime') = date('now', 'localtime')
        ORDER BY created_at ASC;
        """
        df = pd.read_sql_query(query, conn, parse_dates=['created_at'])
        conn.close()

        if df.empty or df['energy_wh'].isnull().all():
            print(f"   (Chưa có dữ liệu năng lượng cho ngày hôm nay)")
            return recommendations

        list_of_max_energies = []
        streak_start_time = None
        current_streak_max_energy = 0.0
        current_streak_end_time = None

        print(f"   🔎 Đang quét {len(df)} bản ghi MỚI của hôm nay...")
        
        # Duyệt qua dữ liệu TỪ CŨ ĐẾN MỚI
        for row in df.itertuples():
            # (row[0] = Index, row[1] = created_at, row[2] = state, row[3] = energy_wh)
            
            if row.state == 1:
                # Đèn đang BẬT (đang trong chuỗi)
                if streak_start_time is None:
                    # Đây là bản ghi BẮT ĐẦU chuỗi
                    streak_start_time = row.created_at
                    # Gán giá trị năng lượng đầu tiên (xử lý NaN)
                    current_streak_max_energy = row.energy_wh if pd.notna(row.energy_wh) else 0.0
                
                # Cập nhật max energy và thời gian kết thúc
                if pd.notna(row.energy_wh):
                    current_streak_max_energy = max(current_streak_max_energy, row.energy_wh)
                
                current_streak_end_time = row.created_at # Luôn cập nhật thời điểm cuối

            else:
                # Đèn TẮT (state == 0). Đây là lúc kết thúc chuỗi.
                if streak_start_time is not None:
                    list_of_max_energies.append(current_streak_max_energy)
                    if current_streak_max_energy > HIGH_ENERGY_THRESHOLD_WH:
                        rec = (
                            f"   ⚡️ CẢNH BÁO: Tiêu thụ vượt ngưỡng! {current_streak_max_energy:.0f} Wh / {HIGH_ENERGY_THRESHOLD_WH}. "
                            f"từ {streak_start_time} đến {current_streak_end_time}."
                        )
                        print(rec)
                        recommendations.append(rec)

                # Reset
                streak_start_time = None
                current_streak_max_energy = 0.0
                current_streak_end_time = None
        
        # Xử lý Edge Case: Nếu file kết thúc mà đèn VẪN BẬT
        if streak_start_time is not None:
            list_of_max_energies.append(current_streak_max_energy)
            if current_streak_max_energy > HIGH_ENERGY_THRESHOLD_WH:
                rec = (
                    f"   ⚡️ CẢNH BÁO: Tiêu thụ vượt ngưỡng! {current_streak_max_energy:.0f} Wh / {HIGH_ENERGY_THRESHOLD_WH}. "
                    f"Đèn đã bật từ {streak_start_time} "
                )
                print(rec)
                recommendations.append(rec)
            else:
                 print(f"      Đèn vẫn đang bật trong ngưỡng cho phép.{current_streak_max_energy:.0f} W.h ")

            # --- ✅ LOGIC MỚI: Tính tổng tiêu thụ trong ngày (Dùng df_total) ---
        if list_of_max_energies:
            total_wh_from_streaks = sum(list_of_max_energies)
        print(f"   🔎 TỔNG TIÊU THỤ ĐIỆN TRONG NGÀY: {total_wh_from_streaks:.0f} Wh")


    except sqlite3.Error as e:
        print(f"   ❌ Lỗi SQLite khi phân tích tiêu thụ: {e}")
    except Exception as e:
         print(f"   ❌ Lỗi Pandas/Python: {e}")
        
    return recommendations

# ===================================================================
# HÀM CHẠY TẤT CẢ (Không đổi)
# ===================================================================
def run_all_analyses():
    """
    Chạy tất cả 3 hàm phân tích và gộp kết quả.
    """
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🧐 Bắt đầu phân tích toàn bộ...")
    all_recs = []
    
    recs1 = analyze_waste()
    all_recs.extend(recs1)
    
    # recs2 = analyze_long_duration()
    # all_recs.extend(recs2)
    
    recs3 = analyze_high_consumption()
    all_recs.extend(recs3)
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Phân tích toàn bộ hoàn tất.")
    if not all_recs:
        print("   👍 Tổng kết: Không có cảnh báo hoặc đề xuất nào.")
    return all_recs

if __name__ == '__main__':
    # Cho phép chạy file này độc lập để test
    run_all_analyses()