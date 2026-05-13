/*
  Clinostato MQTT - Nodo Arduino UNO R4 WiFi

  Cada nodo:
  - Publica telemetria ambiental en:
      inclinometro/<ID_PLACA>/sensores
  - Publica estado del nodo en:
      inclinometro/<ID_PLACA>/estado
  - Recibe comandos de RPM en:
      inclinometro/<ID_PLACA>/motores/config

  Cambia solo `ID_PLACA` para distinguir los tres nodos:
  - Nodo_1
  - Nodo_2
  - Nodo_3
*/

#include <WiFiS3.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <BH1750.h>
#include <Adafruit_BME280.h>
#include <Adafruit_CCS811.h>

// ============================================================
// CONFIGURACION DEL NODO
// ============================================================
const char* ID_PLACA = "Nodo_1";

// ============================================================
// RED / MQTT
// ============================================================
const char* ssid = "Utalca-visitas";
const char* wifi_pass = "";

const char* mqtt_server = "38.242.251.218";
const int mqtt_port = 1883;
const char* mqtt_topic_root = "inclinometro";

WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);

char topicSensores[96];
char topicEstado[96];
char topicConfigMotores[96];

// ============================================================
// SENSORES
// ============================================================
BH1750 lightMeter;
Adafruit_BME280 bme;
Adafruit_CCS811 ccs;

bool bh1750Ok = false;
bool bme280Ok = false;
bool ccs811Ok = false;

float lux = -999;
float temperatura = -999;
float humedad = -999;
float presion = -999;
int co2 = -999;
int tvoc = -999;

// ============================================================
// MOTORES
// ============================================================
#define STEP1 3
#define DIR1  4
#define EN1   5

#define STEP2 6
#define DIR2  7
#define EN2   8

const int MICROSTEPPING = 16;
const int PASOS_VUELTA = 200;

float rpmMotor1 = 12.0;
float rpmMotor2 = 11.0;

unsigned long intervaloToggleM1 = 0;
unsigned long intervaloToggleM2 = 0;
unsigned long ultimoToggleM1 = 0;
unsigned long ultimoToggleM2 = 0;
bool estadoStep1 = LOW;
bool estadoStep2 = LOW;

// ============================================================
// TEMPORIZADORES
// ============================================================
unsigned long lastSensorRead = 0;
unsigned long lastTelemetry = 0;
unsigned long lastStatusHeartbeat = 0;
unsigned long lastWiFiTry = 0;
unsigned long lastMQTTTry = 0;

const unsigned long SENSOR_INTERVAL_MS = 5000;
const unsigned long TELEMETRY_INTERVAL_MS = 5000;
const unsigned long STATUS_INTERVAL_MS = 15000;
const unsigned long WIFI_RETRY_MS = 10000;
const unsigned long MQTT_RETRY_MS = 5000;

// ============================================================
// TOPICOS
// ============================================================
void configurarTopicos() {
  snprintf(topicSensores, sizeof(topicSensores), "%s/%s/sensores", mqtt_topic_root, ID_PLACA);
  snprintf(topicEstado, sizeof(topicEstado), "%s/%s/estado", mqtt_topic_root, ID_PLACA);
  snprintf(topicConfigMotores, sizeof(topicConfigMotores), "%s/%s/motores/config", mqtt_topic_root, ID_PLACA);
}

// ============================================================
// MOTORES
// ============================================================
unsigned long calcularIntervaloToggle(float rpm) {
  if (rpm <= 0) {
    return 0;
  }

  float microstepsPorSegundo = (rpm / 60.0) * PASOS_VUELTA * MICROSTEPPING;
  float togglesPorSegundo = microstepsPorSegundo * 2.0;

  if (togglesPorSegundo <= 0) {
    return 0;
  }

  return (unsigned long)(1000000.0 / togglesPorSegundo);
}

void recalcularMotores() {
  intervaloToggleM1 = calcularIntervaloToggle(rpmMotor1);
  intervaloToggleM2 = calcularIntervaloToggle(rpmMotor2);

  Serial.println("RPM aplicadas:");
  Serial.print("  Motor 1: ");
  Serial.print(rpmMotor1);
  Serial.print(" RPM | toggle us: ");
  Serial.println(intervaloToggleM1);

  Serial.print("  Motor 2: ");
  Serial.print(rpmMotor2);
  Serial.print(" RPM | toggle us: ");
  Serial.println(intervaloToggleM2);
}

void configurarMotores() {
  pinMode(STEP1, OUTPUT);
  pinMode(DIR1, OUTPUT);
  pinMode(EN1, OUTPUT);
  pinMode(STEP2, OUTPUT);
  pinMode(DIR2, OUTPUT);
  pinMode(EN2, OUTPUT);

  digitalWrite(STEP1, LOW);
  digitalWrite(STEP2, LOW);
  digitalWrite(DIR1, HIGH);
  digitalWrite(DIR2, HIGH);

  // En TB6600 suele habilitarse con LOW.
  digitalWrite(EN1, LOW);
  digitalWrite(EN2, LOW);

  recalcularMotores();
}

void actualizarMotores() {
  unsigned long ahora = micros();

  if (intervaloToggleM1 > 0 && ahora - ultimoToggleM1 >= intervaloToggleM1) {
    ultimoToggleM1 = ahora;
    estadoStep1 = !estadoStep1;
    digitalWrite(STEP1, estadoStep1);
  }

  if (intervaloToggleM2 > 0 && ahora - ultimoToggleM2 >= intervaloToggleM2) {
    ultimoToggleM2 = ahora;
    estadoStep2 = !estadoStep2;
    digitalWrite(STEP2, estadoStep2);
  }
}

// ============================================================
// SENSORES
// ============================================================
void configurarSensores() {
  Wire.begin();

  bh1750Ok = lightMeter.begin();
  Serial.println(bh1750Ok ? "BH1750 OK" : "BH1750 no encontrado");

  bme280Ok = bme.begin(0x76);
  Serial.println(bme280Ok ? "BME280 OK" : "BME280 no encontrado");

  ccs811Ok = ccs.begin();
  Serial.println(ccs811Ok ? "CCS811 OK" : "CCS811 no encontrado");
}

void leerSensores() {
  if (bh1750Ok) {
    lux = lightMeter.readLightLevel();
  } else {
    lux = -999;
  }

  if (bme280Ok) {
    temperatura = bme.readTemperature();
    humedad = bme.readHumidity();
    presion = bme.readPressure() / 100.0F;
  } else {
    temperatura = -999;
    humedad = -999;
    presion = -999;
  }

  if (ccs811Ok && ccs.available()) {
    if (!ccs.readData()) {
      co2 = ccs.geteCO2();
      tvoc = ccs.getTVOC();
    }
  } else if (!ccs811Ok) {
    co2 = -999;
    tvoc = -999;
  }
}

// ============================================================
// PUBLICACION MQTT
// ============================================================
void publicarEstado(const char* estado) {
  if (!mqtt.connected()) {
    return;
  }

  StaticJsonDocument<256> doc;
  doc["placa_id"] = ID_PLACA;
  doc["estado"] = estado;
  doc["rpm_m1"] = rpmMotor1;
  doc["rpm_m2"] = rpmMotor2;
  doc["ts_ms"] = millis();

  char buffer[256];
  serializeJson(doc, buffer);
  mqtt.publish(topicEstado, buffer);
}

void publicarSensores() {
  if (!mqtt.connected()) {
    Serial.println("No se publica telemetria: MQTT desconectado");
    return;
  }

  StaticJsonDocument<512> doc;
  doc["placa_id"] = ID_PLACA;
  doc["lux"] = lux;
  doc["temperatura"] = temperatura;
  doc["humedad"] = humedad;
  doc["presion"] = presion;
  doc["co2"] = co2;
  doc["tvoc"] = tvoc;
  doc["rpm_m1"] = rpmMotor1;
  doc["rpm_m2"] = rpmMotor2;
  doc["microstepping"] = MICROSTEPPING;
  doc["pasos_vuelta"] = PASOS_VUELTA;
  doc["bh1750_ok"] = bh1750Ok;
  doc["bme280_ok"] = bme280Ok;
  doc["ccs811_ok"] = ccs811Ok;
  doc["ts_ms"] = millis();

  char buffer[512];
  serializeJson(doc, buffer);

  if (mqtt.publish(topicSensores, buffer)) {
    Serial.print("MQTT TX [");
    Serial.print(topicSensores);
    Serial.print("]: ");
    Serial.println(buffer);
  } else {
    Serial.println("Error publicando telemetria");
  }
}

// ============================================================
// COMANDOS MQTT
// ============================================================
void procesarComandoMotores(JsonDocument& doc) {
  float nuevaRPM1 = doc["rpm_m1"] | rpmMotor1;
  float nuevaRPM2 = doc["rpm_m2"] | rpmMotor2;

  if (nuevaRPM1 < 0 || nuevaRPM2 < 0) {
    Serial.println("Comando ignorado: RPM negativas");
    return;
  }

  rpmMotor1 = nuevaRPM1;
  rpmMotor2 = nuevaRPM2;
  recalcularMotores();
  publicarEstado("rpm_updated");
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  Serial.print("MQTT RX [");
  Serial.print(topic);
  Serial.println("]");

  StaticJsonDocument<256> doc;
  DeserializationError error = deserializeJson(doc, payload, length);
  if (error) {
    Serial.println("JSON invalido recibido");
    return;
  }

  String topicStr = String(topic);
  if (topicStr == topicConfigMotores) {
    procesarComandoMotores(doc);
  }
}

// ============================================================
// CONECTIVIDAD
// ============================================================
void conectarWiFi() {
  Serial.print("Conectando WiFi a ");
  Serial.println(ssid);

  if (strlen(wifi_pass) == 0) {
    WiFi.begin(ssid);
  } else {
    WiFi.begin(ssid, wifi_pass);
  }
}

void gestionarWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    return;
  }

  unsigned long ahora = millis();
  if (ahora - lastWiFiTry >= WIFI_RETRY_MS) {
    lastWiFiTry = ahora;
    conectarWiFi();
  }
}

void gestionarMQTT() {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }

  if (mqtt.connected()) {
    mqtt.loop();
    return;
  }

  unsigned long ahora = millis();
  if (ahora - lastMQTTTry < MQTT_RETRY_MS) {
    return;
  }

  lastMQTTTry = ahora;

  Serial.println("Intentando conectar MQTT...");
  if (mqtt.connect(ID_PLACA)) {
    Serial.println("MQTT conectado");
    mqtt.subscribe(topicConfigMotores);
    Serial.print("Suscrito a: ");
    Serial.println(topicConfigMotores);
    publicarEstado("online");
  } else {
    Serial.print("Fallo MQTT, rc=");
    Serial.println(mqtt.state());
  }
}

// ============================================================
// SETUP / LOOP
// ============================================================
void setup() {
  Serial.begin(9600);
  delay(400);

  configurarTopicos();
  configurarMotores();
  configurarSensores();

  mqtt.setServer(mqtt_server, mqtt_port);
  mqtt.setCallback(mqttCallback);
  mqtt.setKeepAlive(15);
  mqtt.setSocketTimeout(3);
  mqtt.setBufferSize(512);

  conectarWiFi();

  lastSensorRead = millis() - SENSOR_INTERVAL_MS;
  lastTelemetry = millis() - TELEMETRY_INTERVAL_MS;
  lastStatusHeartbeat = millis() - STATUS_INTERVAL_MS;
}

void loop() {
  actualizarMotores();
  gestionarWiFi();
  gestionarMQTT();

  unsigned long ahora = millis();

  if (ahora - lastSensorRead >= SENSOR_INTERVAL_MS) {
    leerSensores();
    lastSensorRead = ahora;
  }

  if (ahora - lastTelemetry >= TELEMETRY_INTERVAL_MS) {
    publicarSensores();
    lastTelemetry = ahora;
  }

  if (ahora - lastStatusHeartbeat >= STATUS_INTERVAL_MS) {
    publicarEstado("online");
    lastStatusHeartbeat = ahora;
  }
}
