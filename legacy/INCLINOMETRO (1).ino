// 2x NEMA 17 + 2x TB6600 + Arduino UNO R4
// Velocidad fija, ambos motores simultáneos

// ── Motor 1 ──
#define STEP1  3
#define DIR1   4
#define EN1    5

// ── Motor 2 ──
#define STEP2  6
#define DIR2   7
#define EN2    8

// ── Parámetros ────────────────────────────
const int   MICROSTEPPING = 16;
const int   PASOS_VUELTA  = 200;
const float RPM_M1        = 60.0;   // velocidad motor 1
const float RPM_M2        = 60.0;   // velocidad motor 2
// ─────────────────────────────────────────

long delayM1, delayM2;
long ultimoPasoM1 = 0, ultimoPasoM2 = 0;
bool estadoM1 = false, estadoM2 = false;

long calcularDelay(float rpm) {
  float pasosSegundo = (rpm / 60.0) * PASOS_VUELTA * MICROSTEPPING;
  return (long)(1000000.0 / pasosSegundo / 2);
}

void setup() {
  pinMode(STEP1, OUTPUT); pinMode(DIR1, OUTPUT); pinMode(EN1, OUTPUT);
  pinMode(STEP2, OUTPUT); pinMode(DIR2, OUTPUT); pinMode(EN2, OUTPUT);

  // Habilitar ambos drivers
  digitalWrite(EN1, LOW);
  digitalWrite(EN2, LOW);

  // Dirección inicial (HIGH=horario, LOW=antihorario)
  digitalWrite(DIR1, HIGH);
  digitalWrite(DIR2, HIGH);  // cambiar a LOW si gira al revés

  delayM1 = calcularDelay(RPM_M1);
  delayM2 = calcularDelay(RPM_M2);

  Serial.begin(9600);
  Serial.println("Motores iniciados");
  Serial.print("Motor 1 delay: "); Serial.print(delayM1); Serial.println(" µs");
  Serial.print("Motor 2 delay: "); Serial.print(delayM2); Serial.println(" µs");
}

// Control sin delay() — ambos motores corren en paralelo
void loop() {
  long ahora = micros();

  // ── Motor 1 ──
  if (ahora - ultimoPasoM1 >= delayM1) {
    estadoM1 = !estadoM1;
    digitalWrite(STEP1, estadoM1);
    ultimoPasoM1 = ahora;
  }

  // ── Motor 2 ──
  if (ahora - ultimoPasoM2 >= delayM2) {
    estadoM2 = !estadoM2;
    digitalWrite(STEP2, estadoM2);
    ultimoPasoM2 = ahora;
  }
}