#include <WiFi.h>
#include <WiFiUdp.h>
#include <Adafruit_MPU6050.h>
#include <Adafruit_Sensor.h>
#include <Wire.h>

// ── CONFIG ─────────────────────────────────────────
const char* WIFI_SSID = "Utalca-visitas";
const char* WIFI_PASS = "";
const char* SERVER_IP = "38.242.251.218";
const uint16_t SERVER_PORT = 5005;

Adafruit_MPU6050 mpu;
WiFiUDP udp;

// Offsets calibrados (SOLO GIROSCOPIO)
float gyroOffsetX = 0, gyroOffsetY = 0, gyroOffsetZ = 0;

// ── FUNCIONES ─────────────────────────────────────
void calibrateGyroOnly() {
    Serial.println("[CAL] Calibrando Giroscopio (Mantén el clinostato QUIETO)...");
    float gx = 0, gy = 0, gz = 0;
    int samples = 500;
    
    for (int i = 0; i < samples; i++) {
        sensors_event_t a, g, temp;
        mpu.getEvent(&a, &g, &temp);
        
        // Sumamos las lecturas convirtiendo rad/s a grados/s
        gx += g.gyro.x * 57.2958f;
        gy += g.gyro.y * 57.2958f;
        gz += g.gyro.z * 57.2958f;
        
        delay(5);
    }
    
    gyroOffsetX = gx / samples;
    gyroOffsetY = gy / samples;
    gyroOffsetZ = gz / samples;
    
    Serial.printf("[CAL] Gyro Offsets → X: %.2f Y: %.2f Z: %.2f\n", 
                  gyroOffsetX, gyroOffsetY, gyroOffsetZ);
}

void connectWiFi() {
    if (WiFi.status() == WL_CONNECTED) return;
    WiFi.begin(WIFI_SSID, WIFI_PASS);
    unsigned long t = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - t < 8000) {
        delay(200);
        Serial.print(".");
    }
    if (WiFi.status() == WL_CONNECTED) {
        Serial.println("\n[WiFi OK]");
        udp.begin(0);
    } else {
        Serial.println("\n[WiFi FAIL]");
    }
}

// ── SETUP ─────────────────────────────────────────
void setup() {
    Serial.begin(115200);
    Wire.begin(21, 22); // Revisa si estos son los pines I2C de tu placa
    
    if (!mpu.begin()) {
        Serial.println("MPU6050 no detectado");
        while (1);
    }
    
    mpu.setAccelerometerRange(MPU6050_RANGE_2_G);
    mpu.setFilterBandwidth(MPU6050_BAND_21_HZ);
    
    // Solo calibramos el giroscopio al inicio
    calibrateGyroOnly();
    connectWiFi();
}

// ── LOOP ──────────────────────────────────────────
void loop() {
    sensors_event_t a, g, temp;
    mpu.getEvent(&a, &g, &temp);

    // 1. Aceleración CRUDA (sin offsets de software, usamos la calibración de fábrica)
    float ax = a.acceleration.x / 9.81f;
    float ay = a.acceleration.y / 9.81f;
    float az = a.acceleration.z / 9.81f;

    // 2. Giroscopio CON offset (restamos el error estático calculado al inicio)
    float gx = (g.gyro.x * 57.2958f) - gyroOffsetX;
    float gy = (g.gyro.y * 57.2958f) - gyroOffsetY;
    float gz = (g.gyro.z * 57.2958f) - gyroOffsetZ;

    // 3. Envío UDP de los 6 valores exactos que espera el Server.py
    if (WiFi.status() == WL_CONNECTED) {
        char packet[100];
        // Formato estricto: ax,ay,az,gx,gy,gz
        snprintf(packet, sizeof(packet), "%.4f,%.4f,%.4f,%.4f,%.4f,%.4f\n", ax, ay, az, gx, gy, gz);
        
        udp.beginPacket(SERVER_IP, SERVER_PORT);
        udp.write((uint8_t*)packet, strlen(packet));
        udp.endPacket();
    }

    delay(20); // 50 Hz es una tasa excelente para este análisis
}