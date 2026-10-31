
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>

#define LED_PIN 26
#define HUMEDAD_PIN 34
#define LUZ_PIN 35
#define TEMPERATURA_PIN 32

const char* WIFI_SSID = "MikeRevolucion";
const char* WIFI_PASSWORD = "123456789+";

const char* MQTT_SERVER = "192.168.137.162";
const int MQTT_PORT = 1883;

const char* TOPIC_TELEMETRIA = "sensores/esp32/telemetria";
const char* TOPIC_CONTROL = "dispositivos/esp32/control";

const unsigned long INTERVALO_ENVIO = 1000;

const float AJUSTE_TEMPERATURA = -0.86f;
const int NUM_MUESTRAS = 20;

WiFiClient espClient;
PubSubClient mqtt(espClient);

unsigned long sequence_id = 0;
unsigned long ultimoEnvio = 0;


// --------------------------------------------------
// CONEXION WIFI
// --------------------------------------------------

void conectarWiFi() {

    Serial.print("Conectando a Wi-Fi");

    WiFi.mode(WIFI_STA);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }

    Serial.println();
    Serial.println("Wi-Fi conectado");

    Serial.print("IP del ESP32: ");
    Serial.println(WiFi.localIP());

    Serial.print("IP del broker MQTT: ");
    Serial.println(MQTT_SERVER);

    Serial.print("Gateway del ESP32: ");
    Serial.println(WiFi.gatewayIP());

    Serial.print("RSSI WiFi: ");
    Serial.println(WiFi.RSSI());
}


// --------------------------------------------------
// OBTENER TIMESTAMP UNIX EN MILISEGUNDOS
// --------------------------------------------------

bool obtenerTimestampUnixMs(uint64_t &timestamp) {

    struct timeval tiempoActual;

    gettimeofday(&tiempoActual, nullptr);

    if (tiempoActual.tv_sec < 1700000000) {
        timestamp = 0;
        return false;
    }

    timestamp =
        ((uint64_t)tiempoActual.tv_sec * 1000ULL) +
        (tiempoActual.tv_usec / 1000ULL);

    return true;
}


// --------------------------------------------------
// SINCRONIZACION DEL RELOJ NTP
// --------------------------------------------------

bool sincronizarReloj() {

    configTime(
        0,
        0,
        "pool.ntp.org",
        "time.nist.gov",
        "time.google.com"
    );

    Serial.println("Sincronizando reloj NTP...");

    struct tm tiempo;

    for (int intentos = 0; intentos < 30; intentos++) {

        if (getLocalTime(&tiempo, 1000)) {

            uint64_t timestamp = 0;

            if (obtenerTimestampUnixMs(timestamp)) {

                Serial.println("Reloj NTP sincronizado");

                Serial.print("Fecha y hora ESP32: ");
                Serial.println(
                    &tiempo,
                    "%d/%m/%Y %H:%M:%S"
                );

                Serial.print("Timestamp Unix: ");
                Serial.println(
                    (unsigned long long)timestamp
                );

                return true;
            }
        }

        Serial.println(
            "Esperando respuesta del servidor NTP..."
        );

        delay(500);
    }

    Serial.println(
        "No se pudo sincronizar el reloj NTP"
    );

    return false;
}


// --------------------------------------------------
// LECTURA DEL SENSOR LM35
// --------------------------------------------------

float leerTemperatura() {

    uint32_t sumaVoltaje = 0;

    for (int i = 0; i < NUM_MUESTRAS; i++) {

        sumaVoltaje += analogReadMilliVolts(
            TEMPERATURA_PIN
        );

        delay(2);
    }

    float voltajePromedioMV =
        (float)sumaVoltaje / NUM_MUESTRAS;

    float temperaturaOriginal =
        voltajePromedioMV / 10.0f;

    float temperaturaCalibrada =
        temperaturaOriginal + AJUSTE_TEMPERATURA;

    Serial.println("----- LECTURA LM35 -----");

    Serial.print("Voltaje promedio: ");
    Serial.print(voltajePromedioMV, 2);
    Serial.println(" mV");

    Serial.print("Temperatura original: ");
    Serial.print(temperaturaOriginal, 2);
    Serial.println(" C");

    Serial.print("Temperatura calibrada: ");
    Serial.print(temperaturaCalibrada, 2);
    Serial.println(" C");

    Serial.println("------------------------");

    return temperaturaCalibrada;
}


// --------------------------------------------------
// RECEPCION DE COMANDOS MQTT
// --------------------------------------------------

void callback(
    char* topic,
    byte* payload,
    unsigned int length
) {

    if (strcmp(topic, TOPIC_CONTROL) != 0) {
        return;
    }

    StaticJsonDocument<128> doc;

    DeserializationError error =
        deserializeJson(doc, payload, length);

    if (error) {

        Serial.println("Comando MQTT no valido");
        return;
    }

    const char* action = doc["action"];

    if (action == nullptr) {

        Serial.println("No se recibio ninguna accion");
        return;
    }

    if (strcmp(action, "ON") == 0) {

        digitalWrite(LED_PIN, HIGH);
        Serial.println("LED ENCENDIDO");

    } else if (strcmp(action, "OFF") == 0) {

        digitalWrite(LED_PIN, LOW);
        Serial.println("LED APAGADO");

    } else {

        Serial.println("Accion desconocida");
    }
}


// --------------------------------------------------
// CONEXION MQTT
// --------------------------------------------------

void conectarMQTT() {

    while (!mqtt.connected()) {

        Serial.print("Conectando a MQTT...");

        String clientId = "ESP32_INVERNADERO_01";

        clientId += "_";

        clientId += String(
            (uint32_t)ESP.getEfuseMac(),
            HEX
        );

        if (mqtt.connect(clientId.c_str())) {

            Serial.println(" conectado");

            mqtt.subscribe(TOPIC_CONTROL);

            Serial.println(
                "Suscrito al topico de control"
            );

        } else {

            Serial.print(" error: ");
            Serial.println(mqtt.state());

            Serial.println(
                "Nuevo intento en 3 segundos"
            );

            delay(3000);
        }
    }
}


// --------------------------------------------------
// CONFIGURACION INICIAL
// --------------------------------------------------

void setup() {

    Serial.begin(115200);

    analogReadResolution(12);

    analogSetPinAttenuation(
        HUMEDAD_PIN,
        ADC_11db
    );

    analogSetPinAttenuation(
        LUZ_PIN,
        ADC_11db
    );

    analogSetPinAttenuation(
        TEMPERATURA_PIN,
        ADC_11db
    );

    pinMode(HUMEDAD_PIN, INPUT);
    pinMode(LUZ_PIN, INPUT);
    pinMode(TEMPERATURA_PIN, INPUT);

    pinMode(LED_PIN, OUTPUT);
    digitalWrite(LED_PIN, LOW);

    conectarWiFi();

    sincronizarReloj();

    mqtt.setServer(
        MQTT_SERVER,
        MQTT_PORT
    );

    mqtt.setCallback(callback);

    mqtt.setBufferSize(512);
}


// --------------------------------------------------
// BUCLE PRINCIPAL
// --------------------------------------------------

void loop() {

    if (WiFi.status() != WL_CONNECTED) {
        conectarWiFi();
        sincronizarReloj();
    }

    if (!mqtt.connected()) {
        conectarMQTT();
    }

    mqtt.loop();

    unsigned long ahora = millis();

    if (
        ahora - ultimoEnvio >= INTERVALO_ENVIO
    ) {

        ultimoEnvio = ahora;

        // Leer sensores

        int humedad = analogRead(HUMEDAD_PIN);

        int luz = analogRead(LUZ_PIN);

        float temperatura = leerTemperatura();


        // Obtener hora Unix sincronizada

        uint64_t timestampUnixMs = 0;

        bool relojValido =
            obtenerTimestampUnixMs(timestampUnixMs);

        if (relojValido) {

            Serial.println("Hora NTP disponible");

            Serial.print("Timestamp Unix ESP32: ");

            Serial.println(
                (unsigned long long)timestampUnixMs
            );

        } else {

            Serial.println(
                "Hora NTP no disponible. "
                "No se enviara esta lectura."
            );

            return;
        }


        // Crear JSON

        StaticJsonDocument<384> doc;

        doc["node_id"] = "ESP32_INVERNADERO_01";

        doc["sequence_id"] = ++sequence_id;

        doc["timestamp_ms"] = millis();

        doc["timestamp_unix_ms"] = timestampUnixMs;

        JsonObject data = doc.createNestedObject("data");

        data["humedad"] = humedad;

        data["temperatura"] = temperatura;

        data["luz"] = luz;


        // Serializar JSON

        char mensaje[384];

        size_t longitud = serializeJson(
            doc,
            mensaje,
            sizeof(mensaje)
        );


        // Publicar por MQTT

        if (
            longitud > 0 &&
            mqtt.publish(
                TOPIC_TELEMETRIA,
                mensaje
            )
        ) {

            Serial.println("Mensaje MQTT publicado:");

            Serial.println(mensaje);

        } else {

            Serial.println(
                "Error al publicar MQTT"
            );
        }
    }
}
