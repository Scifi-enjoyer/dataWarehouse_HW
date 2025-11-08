import os

# Lấy đường dẫn gốc của dự án (folder kho_du_lieu)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Đường dẫn tới thư mục data và database
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_DIR = os.path.join(BASE_DIR, "database")

# Đảm bảo các folder tồn tại
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

# File CSV tổng hợp từ ThingSpeak
CSV_FILE = os.path.join(DATA_DIR, "")
ARFF_FILE = os.path.join(DATA_DIR, "thingspeak_data.arff")

# File SQLite database
DB_FILE = os.path.join(DB_DIR, "smarthome_dw.db")

# Các cấu hình khác
THRESHOLD_HIGH = 30   # Ngưỡng xác định "high" consumption

# Cấu hình cho ThingSpeak
CHANNEL_IDS = ["3152988"]
READ_API_KEYS = ["W0CSOTQCFZYNN83D"]

FIELDS = []         # nếu để [] thì sẽ tự detect
RESULTS = 8000      # số bản ghi tối đa lấy về
CLASS_FIELD = None  # e.g. "field3" nếu muốn làm class label

if __name__ == "__main__":
    # In ra để kiểm tra
    print("📂 BASE_DIR =", BASE_DIR)
    print("📂 DATA_DIR =", DATA_DIR)
    print("📂 DB_DIR   =", DB_DIR)
    print("📄 CSV_FILE =", CSV_FILE)
    print("💾 DB_FILE  =", DB_FILE)
    print("⚡ THRESHOLD_HIGH =", THRESHOLD_HIGH)
    print("📡 CHANNEL_IDS =", CHANNEL_IDS)
    print("🔑 READ_API_KEYS =", READ_API_KEYS)