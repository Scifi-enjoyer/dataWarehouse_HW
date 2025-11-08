import os
import time
import schedule
import threading
import sys
from datetime import datetime

# Import các hàm từ các file theo cấu trúc mới
import config
# ✅ SỬA IMPORT: Lấy các hàm đã cập nhật
from src.utils import fetch_json, json_to_df, load_dataframe_to_dwh, get_last_timestamp
# ✅ SỬA IMPORT: Lấy các hàm phân tích mới
from src.analyzer import analyze_waste, analyze_high_consumption, run_all_analyses
from database.create import create_database_schema

# Biến toàn cục để điều khiển luồng (thread)
stop_event = threading.Event()
job_thread = None

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

# ===================================================================
# HÀM JOB CHÍNH (Đã sửa logic ETL)
# ===================================================================
def run_full_etl_and_analysis_job():
    """
    Hàm công việc (job) hoàn chỉnh: ETL (tăng dần) cho 1 CHANNEL rồi Phân tích.
    """
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Bắt đầu chu trình ETL")

    etl_success = False 
    print(f"--- Bắt đầu ETL---")
    
    if not config.CHANNEL_IDS or len(config.CHANNEL_IDS) == 0:
        print("   [!] ❌ LỖI: Không có CHANNEL_IDS nào được định nghĩa trong config.py")
    else:
        cid = config.CHANNEL_IDS[0]
        key = config.READ_API_KEYS[0] if config.READ_API_KEYS else ""
        
        print(f"\n--- Đang xử lý Channel {cid} ---")
        try:
            # 1. Lấy timestamp cuối cùng
            last_ts = get_last_timestamp()
            
            # 2. Fetch dữ liệu MỚI HƠN
            js = fetch_json(cid, key, start_time=last_ts)
            df = json_to_df(js, config.FIELDS)

            if df.empty:
                print(f"   [E] ⚠️ Channel {cid} không có dữ liệu mới.")
                etl_success = True 
            else:
                # 3. Nạp dữ liệu mới
                success_load = load_dataframe_to_dwh(df)
                
                if success_load:
                     etl_success = True
                else:
                     etl_success = False

        except Exception as e:
            print(f"   [!] ❌ LỖI NGHIÊM TRỌNG khi xử lý Channel {cid}: {e}")
            etl_success = False

    if etl_success:
         print(f"\n--- ✅ ETL hoàn tất ---")
    else:
         print(f"\n--- ❌ ETL thất bại ---")

    # # --- Phần Phân Tích (Chạy TẤT CẢ tự động) ---
    # if etl_success:
    #     run_all_analyses() # Gọi hàm chạy tất cả phân tích
    # else:
    #     print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Bỏ qua phân tích do ETL gặp lỗi.")

    print(f"\n-----------------------------------------------")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Chu trình ETL HOÀN TẤT.")


# ===================================================================
# MENU MỚI: Menu con cho Phân tích
# ===================================================================
def analysis_submenu():
    """
    Hiển thị menu con cho việc chọn lựa phân tích.
    """
    while True:
        clear_screen()
        print("====================================")
        print("       CHỌN PHƯƠNG THỨC PHÂN TÍCH      ")
        print("====================================")
        print("  1. Phân tích Lãng phí (Đèn bật, không người)")
        # print("  2. Phân tích Bật quá lâu")
        print("  2. Phân tích Tiêu thụ trong ngày")
        print("  3. Chạy tất cả phân tích")
        print("  4. Quay lại Menu chính")
        print("-----------------------------------")
        choice = input("Nhập lựa chọn của bạn: ")

        if choice == '1':
            clear_screen()
            print("[App] 🧐 Đang chạy Phân tích Lãng phí...")
            analyze_waste()
            input("\nHoàn tất! Bấm Enter để quay lại...")
        # elif choice == '2':
        #     clear_screen()
        #     print("[App] 🧐 Đang chạy Phân tích Bật quá lâu...")
        #     analyze_long_duration()
        #     input("\nHoàn tất! Bấm Enter để quay lại...")
        elif choice == '2':
            clear_screen()
            print("[App] 🧐 Đang chạy Phân tích Tiêu thụ ...")
            analyze_high_consumption()
            input("\nHoàn tất! Bấm Enter để quay lại...")
        elif choice == '3':
            clear_screen()
            print("[App] 🧐 Đang chạy TẤT CẢ phân tích...")
            run_all_analyses()
            input("\nHoàn tất! Bấm Enter để quay lại...")
        elif choice == '4':
            break # Thoát vòng lặp, quay lại main_menu
        else:
            input("[App] ❌ Lựa chọn không hợp lệ. (Bấm Enter để thử lại)")

# ===================================================================
# PHẦN GIAO DIỆN MENU (Đã sửa)
# ===================================================================
# def start_scheduler_thread():
#     """
#     Hàm chạy vòng lặp schedule trong một luồng (thread) riêng.
#     """
#     print("\n[Scheduler] ⚙️ Luồng lập lịch đã khởi động...")
    
#     schedule.every(15).seconds.do(run_full_etl_and_analysis_job)

#     print("[Scheduler] ⏳ Đang chạy lần đầu tiên ngay bây giờ...")
#     schedule.run_all()

#     while not stop_event.is_set():
#         schedule.run_pending()
#         time.sleep(1)

#     print("[Scheduler] 🛑 Luồng lập lịch đã dừng.")

def main_menu():
    global job_thread
    is_running = False

    while True:
        clear_screen()
        print("====================================")
        print("  QUẢN LÝ ETL & PHÂN TÍCH DỮ LIỆU   ")
        print("====================================")

        # if is_running:
        #     print("  Trạng thái: 🟢 ĐANG CHẠY ")
        # else:
        #     print("  Trạng thái: 🔴 ĐÃ DỪNG")

        print("\n--- Lựa chọn ---")
        # print("  1. Bắt đầu chạy tự động (ETL + Phân tích)")
        # print("  2. Dừng chạy tự động")
        print("  1. ETL Và nạp dữ liệu vào DWH")
        print("  2. Phân tích & Đưa lời khuyên (Mở Menu con)") # ✅ Sửa mô tả
        print("  3. Xóa và Tạo lại Database (Hard Reset)")
        print("  4. Thoát")
        print("-----------------------------------")

        choice = input("Nhập lựa chọn của bạn: ")

        # if choice == '1': # Bắt đầu tự động
        #     if not is_running:
        #         print("\n[App] ⏳ Đang khởi động...")
        #         stop_event.clear()
        #         job_thread = threading.Thread(target=start_scheduler_thread, daemon=True)
        #         job_thread.start()
        #         is_running = True
        #         print("[App] ✅ Đã BẮT ĐẦU.")
        #         time.sleep(2)
        #     else:
        #         input("[App] ⚠️ Vẫn đang chạy! (Bấm Enter để tiếp tục)")

        # elif choice == '2': # Dừng tự động
        #     if is_running:
        #         print("\n[App] ⏳ Đang dừng...")
        #         stop_event.set()
        #         job_thread.join()
        #         schedule.clear()
        #         is_running = False
        #         job_thread = None
        #         print("[App] ✅ Đã DỪNG.")
        #         time.sleep(2)
        #     else:
        #         input("[App] ⚠️ Vốn dĩ đã dừng! (Bấm Enter để tiếp tục)")

        if choice == '1': # Chạy ETL 1 lần
             clear_screen()
             print("[App] ⚡ Đang chạy ETL & Nạp dữ liệu vào DWH...")
             run_full_etl_and_analysis_job()
             input("\nHoàn tất! Bấm Enter để quay lại menu...")

        elif choice == '2': # ✅ GỌI MENU CON
            analysis_submenu()

        elif choice == '3': # Reset DB
            clear_screen()
            print("[App] 🛑 CẢNH BÁO 🛑")
            print("Thao tác này sẽ XÓA TẤT CẢ dữ liệu trong database")
            confirm = input("Bạn có CHẮC CHẮN muốn tiếp tục? (nhập 'yes' để xác nhận): ")
            if confirm.lower() == 'yes':
                 print("\n[DB] ⏳ Đang dừng dịch vụ (nếu có)...")
                 was_running = False
                 if is_running:
                      was_running = True
                      stop_event.set()
                      job_thread.join()
                      schedule.clear()
                      is_running = False
                      job_thread = None

                 print(f"[DB] ⏳ Đang xóa file DB cũ: {config.DB_FILE}")
                 try:
                      if os.path.exists(config.DB_FILE):
                           os.remove(config.DB_FILE)
                           print("[DB] ✅ Đã xóa DB cũ.")
                      else:
                           print("[DB] ℹ️ Không tìm thấy file DB cũ, bỏ qua bước xóa.")

                      print("[DB] ⏳ Đang tạo lại schema database...")
                      create_database_schema()
                      print("[DB] ✅ Tạo lại schema thành công.")

                      if was_running:
                           print("[App] ⏳ Khởi động lại dịch vụ tự động...")
                           stop_event.clear()
                           job_thread = threading.Thread(target=start_scheduler_thread, daemon=True)
                           job_thread.start()
                           is_running = True

                 except Exception as e:
                      print(f"[DB] ❌ Lỗi khi dọn dẹp database: {e}")

                 input("\nHoàn tất! Bấm Enter để quay lại menu...")
            else:
                 input("\nĐã hủy. Bấm Enter để quay lại menu...")

        elif choice == '4': # Thoát
            if is_running:
                print("\n[App] ⏳ Đang dừng các luồng trước khi thoát...")
                stop_event.set()
                job_thread.join()
            print("\n[App] 👋 Tạm biệt!")
            sys.exit()

        else:
            input("[App] ❌ Lựa chọn không hợp lệ. (Bấm Enter để thử lại)")

if __name__ == "__main__":
    try:
        clear_screen()
        print("[App] 🏃 Đang kiểm tra và khởi tạo database (nếu cần)...")
        create_database_schema()
        print("[App] ✅ Database sẵn sàng.")
        time.sleep(1.5)

        main_menu()
    except KeyboardInterrupt:
        print("\n[App] 🛑Đang thoát...")
        if job_thread:
            stop_event.set()
            job_thread.join()
        sys.exit()