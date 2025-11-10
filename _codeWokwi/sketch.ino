#include <WiFi.h>
#include <ThingSpeak.h>

// ===== WiFi & ThingSpeak =====
WiFiClient client;
const unsigned long CHANNEL_NUMBER = 3152988;      // Channel phòng khách
const char*        WRITE_API_KEY   = "W0CSOTQCFZYNN83D";
const char*        SSID            = "Wokwi-GUEST";
const char*        PASSWORD        = "";

// ===== Pin mapping =====
#define PIR_PIN     4     // Presence chỉ để phân tích (không điều khiển đèn)
#define SWITCH_PIN  5     // Công tắc kéo GND -> nhấn/gạt = LOW
#define LED_PIN     2     // LED hiển thị trạng thái đèn

// ===== Tham số mô phỏng =====
float power_on_w  = 100.0f;  // công suất khi bật
float power_off_w = 0.0f;    // công suất khi tắt (0 tuyệt đối)
float power_mean  = 1.0f;    // nhiễu (chỉ áp dụng khi bật)
float power_std   = 3.0f;    // nhiễu (chỉ áp dụng khi bật)

// ===== Cảnh báo =====
const float    POWER_LOW_W     = 5.0f;      // A2: state=1 nhưng power < 5W
const uint32_t CORE_THRESH_S   = 1800;      // A1: bật > 30 phút khi không có người
const uint32_t DAY_MS          = 24UL*60UL*60UL*1000UL;
const uint16_t REPEAT_THRESHOLD= 3;         // A3: số lần/ngày

// ===== Tiện ích =====
inline float clampf(float v, float lo, float hi){ return v < lo ? lo : (v > hi ? hi : v); }
float addNoise(float value, float mean, float stddev){
  float u = (float)random(-1000,1000) / 1000.0f;  // ~U(-1,1)
  return value + (u * stddev) + mean;
}

// ===== Trạng thái tích lũy =====
unsigned long lastMs = 0;
float energy_since_on_Wh = 0.0f;   // F2
float on_time_s          = 0.0f;   // F5
bool  prev_light_on      = false;

// ===== Thói quen lãng phí =====
bool  prev_waste_cond    = false;
uint16_t waste_count_day = 0;
unsigned long day_anchor_ms = 0;

void connectWiFi(){
  WiFi.mode(WIFI_STA);
  WiFi.begin(SSID, PASSWORD);
  Serial.print("Connecting to WiFi");
  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && (millis() - t0) < 15000){
    delay(500);
    Serial.print(".");
  }
  Serial.println(WiFi.status()==WL_CONNECTED ? "\nWiFi connected" : "\nWiFi connect timeout");
}

void setup(){
  Serial.begin(115200);
  pinMode(PIR_PIN, INPUT);
  pinMode(SWITCH_PIN, INPUT_PULLUP);   // quan trọng: PULLUP -> LOW khi đóng về GND
  pinMode(LED_PIN, OUTPUT);

  connectWiFi();
  ThingSpeak.begin(client);

  randomSeed(esp_random());
  lastMs = millis();
  day_anchor_ms = lastMs;

  Serial.println("Ready. GAT switch (Slide switch) D5 <-> GND de BAT/TAT den.");
}

void loop(){
  if (WiFi.status() != WL_CONNECTED) connectWiFi();

  // ---- Presence chỉ để phân tích ----
  int presence = (digitalRead(PIR_PIN) == HIGH) ? 1 : 0;   // F3

  // ---- STATE chỉ do công tắc ----
  int d5_raw     = digitalRead(SWITCH_PIN);
  bool switch_on = (d5_raw == LOW);        // gạt về phía GND => LOW => bật
  bool light_on  = switch_on;              // F4
  digitalWrite(LED_PIN, light_on ? HIGH : LOW);

  // Debug tức thời để kiểm tra wiring
  Serial.printf("D5=%d  switch_on=%d  light_on=%d  presence=%d\n",
                d5_raw, (int)switch_on, (int)light_on, presence);

  // ---- Power: đèn tắt = 0 tuyệt đối ----
  float base_power  = light_on ? power_on_w : 0.0f;
  float noisy_power = light_on
                      ? clampf(addNoise(base_power, power_mean, power_std), 0.0f, 1500.0f)
                      : 0.0f;  // tắt = 0W tuyệt đối (F1 sẽ = 0)

  // ---- Tính dt & tích lũy “phiên bật” ----
  unsigned long nowMs = millis();
  unsigned long dtMs  = nowMs - lastMs;
  lastMs = nowMs;

  // Reset khi 0 -> 1 (bắt đầu phiên)
  if (!prev_light_on && light_on){
    energy_since_on_Wh = 0.0f;
    on_time_s          = 0.0f;
  }

  // Cộng dồn khi đang bật (F2 sạch nhiễu)
  if (light_on){
    energy_since_on_Wh += (base_power * (dtMs / 1000.0f)) / 3600.0f; // Wh = W*s/3600
    if (energy_since_on_Wh < 0) energy_since_on_Wh = 0.0f;
    on_time_s += dtMs / 1000.0f; // giây
  }
  // Khi tắt: giữ nguyên F2 & F5 (đỉnh phiên)

  // ---- Đếm “thói quen lãng phí” (presence=0 & state=1) ----
  bool waste_cond = (presence == 0) && light_on;
  if (!prev_waste_cond && waste_cond) waste_count_day++;
  prev_waste_cond = waste_cond;

  if ((nowMs - day_anchor_ms) >= DAY_MS){
    day_anchor_ms = nowMs;
    waste_count_day = 0;
  }

  // ---- Cờ cảnh báo ----
  bool A1_core = (presence == 0) && light_on && (on_time_s >= CORE_THRESH_S);
  bool A2_lowP = light_on && (noisy_power < POWER_LOW_W);
  bool A3_habit= (waste_count_day >= REPEAT_THRESHOLD);

  // ---- Gửi ThingSpeak (F1..F5) ----
  ThingSpeak.setField(1, noisy_power);           // F1: Power (W)
  ThingSpeak.setField(2, energy_since_on_Wh);    // F2: Energy_since_on (Wh)
  ThingSpeak.setField(3, presence);              // F3: Presence (0/1)
  ThingSpeak.setField(4, light_on ? 1 : 0);      // F4: State (0/1)
  ThingSpeak.setField(5, on_time_s);             // F5: On_time_s (s)

  // Status: tổng hợp cờ
  char statusBuf[96];
  snprintf(statusBuf, sizeof(statusBuf), "A1=%d;A2=%d;A3=%d;count=%u",
           A1_core?1:0, A2_lowP?1:0, A3_habit?1:0, waste_count_day);
  ThingSpeak.setStatus(statusBuf);

  int response = ThingSpeak.writeFields(CHANNEL_NUMBER, WRITE_API_KEY);
  if (response == 200){
    Serial.printf("OK | state=%d pres=%d | P=%.1fW E_since_on=%.4fWh on_time=%.1fs | %s\n",
                  light_on, presence, noisy_power, energy_since_on_Wh, on_time_s, statusBuf);
  } else {
    Serial.printf("FAIL (%d)\n", response);
  }

  prev_light_on = light_on;
  delay(15000); // ≥15s cho free tier
}