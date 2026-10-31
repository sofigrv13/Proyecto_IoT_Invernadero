
import asyncio
import json
import time
import threading
from pathlib import Path
from datetime import datetime, timezone

import joblib
import pandas as pd
import paho.mqtt.client as mqtt

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    String,
    DateTime,
    inspect,
    text
)
from sqlalchemy.orm import declarative_base, sessionmaker


# =====================================================
# CONFIGURACION GENERAL
# =====================================================

app = FastAPI(title="Sistema IoT Invernadero")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

main_loop = None

clientes_ws = []
lock_clientes = threading.Lock()


# =====================================================
# MODELO RANDOM FOREST
# =====================================================

RUTA_MODELO = (
    Path(__file__).resolve().parent.parent
    / "modelo_random_forest.pkl"
)

modelo_rf = joblib.load(RUTA_MODELO)

print("Modelo Random Forest cargado correctamente")


def diagnosticar_cultivo(humedad, temperatura, luz):

    if temperatura >= 30:
        return "Golpe de Calor"

    datos_sensor = pd.DataFrame([
        {
            "humedad_raw": humedad,
            "temperatura_c": temperatura,
            "luz_raw": luz
        }
    ])

    prediccion = modelo_rf.predict(datos_sensor)

    return str(prediccion[0])


# =====================================================
# BASE DE DATOS SQLITE
# =====================================================

BASE_DIR = Path(__file__).resolve().parent

DATABASE_URL = f"sqlite:///{BASE_DIR / 'iot.db'}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


class LecturaIoT(Base):

    __tablename__ = "lecturas"

    id = Column(Integer, primary_key=True, index=True)

    valor = Column(Float, nullable=True)

    humedad = Column(Float, nullable=True)
    temperatura = Column(Float, nullable=True)
    luz = Column(Float, nullable=True)

    sequence_id = Column(Integer, nullable=True)
    node_id = Column(String, nullable=True)

    timestamp_ms = Column(Float, nullable=True)
    timestamp_unix_ms = Column(Float, nullable=True)

    latencia = Column(Float, nullable=True)

    prediccion = Column(String, nullable=True)

    fecha_registro = Column(
        DateTime,
        default=datetime.utcnow
    )


Base.metadata.create_all(bind=engine)


# =====================================================
# MIGRACION DE COLUMNAS
# =====================================================

def verificar_columnas():

    inspector = inspect(engine)

    columnas_existentes = [
        columna["name"]
        for columna in inspector.get_columns("lecturas")
    ]

    columnas_necesarias = {
        "valor": "FLOAT",
        "humedad": "FLOAT",
        "temperatura": "FLOAT",
        "luz": "FLOAT",
        "sequence_id": "INTEGER",
        "node_id": "VARCHAR",
        "timestamp_ms": "FLOAT",
        "timestamp_unix_ms": "FLOAT",
        "latencia": "FLOAT",
        "prediccion": "VARCHAR",
        "fecha_registro": "DATETIME"
    }

    with engine.begin() as conexion:

        for nombre, tipo in columnas_necesarias.items():

            if nombre not in columnas_existentes:

                conexion.execute(
                    text(
                        f"ALTER TABLE lecturas "
                        f"ADD COLUMN {nombre} {tipo}"
                    )
                )

                print(f"Columna agregada: {nombre}")


verificar_columnas()


# =====================================================
# CONFIGURACION MQTT
# =====================================================

MQTT_SERVER = "localhost"
MQTT_PORT = 1883

TOPIC_TELEMETRIA = "sensores/esp32/telemetria"
TOPIC_CONTROL = "dispositivos/esp32/control"


# =====================================================
# VARIABLES DE TELEMETRIA
# =====================================================

secuencias_recibidas = set()

ultimo_sequence_id = None

lock_secuencias = threading.Lock()


def calcular_perdida_paquetes():

    with lock_secuencias:

        if len(secuencias_recibidas) < 2:
            return 0.0

        minimo = min(secuencias_recibidas)
        maximo = max(secuencias_recibidas)

        total_esperado = maximo - minimo + 1
        total_recibido = len(secuencias_recibidas)

        if total_esperado <= 0:
            return 0.0

        perdida = (
            1 - total_recibido / total_esperado
        ) * 100

        return round(perdida, 2)


# =====================================================
# WEBSOCKET: ENVIO A CLIENTES
# =====================================================

async def enviar_a_clientes(mensaje):

    with lock_clientes:
        clientes = list(clientes_ws)

    desconectados = []

    for cliente in clientes:

        try:
            await cliente.send_json(mensaje)

        except Exception:
            desconectados.append(cliente)

    if desconectados:

        with lock_clientes:

            for cliente in desconectados:

                if cliente in clientes_ws:
                    clientes_ws.remove(cliente)


# =====================================================
# MQTT: RECEPCION DE TELEMETRIA
# =====================================================

def on_message(client, userdata, msg):

    global ultimo_sequence_id

    db = SessionLocal()

    try:

        payload = json.loads(
            msg.payload.decode("utf-8")
        )

        # ---------------------------------------------
        # DATOS GENERALES
        # ---------------------------------------------

        node_id = payload.get(
            "node_id",
            "ESP32_INVERNADERO_01"
        )

        sequence_id = payload.get("sequence_id")

        timestamp_ms = payload.get("timestamp_ms")

        timestamp_unix_ms = payload.get(
            "timestamp_unix_ms"
        )

        # ---------------------------------------------
        # VARIABLES DE LOS SENSORES
        # ---------------------------------------------

        datos = payload.get("data", {})

        humedad = float(
            datos.get("humedad", 0)
        )

        temperatura = float(
            datos.get("temperatura", 0)
        )

        luz = float(
            datos.get("luz", 0)
        )

        # ---------------------------------------------
        # CALCULO DE LATENCIA
        # ---------------------------------------------

        recepcion_unix_ms = int(
            time.time() * 1000
        )

        latencia = None

        if timestamp_unix_ms is not None:

            timestamp_unix_ms = float(
                timestamp_unix_ms
            )

            if timestamp_unix_ms > 0:

                latencia = (
                    recepcion_unix_ms
                    - timestamp_unix_ms
                )

                if latencia < 0:

                    print(
                        "Advertencia: latencia negativa. "
                        "Verificar sincronizacion NTP."
                    )

                    latencia = None

        print("----- PRUEBA DE LATENCIA -----")
        print("Timestamp ESP32:", timestamp_unix_ms)
        print("Recepcion PC:", recepcion_unix_ms)
        print("Latencia calculada:", latencia)
        print("------------------------------")
        # ---------------------------------------------
        # DIAGNOSTICO DEL CULTIVO
        # ---------------------------------------------

        prediccion = diagnosticar_cultivo(
            humedad,
            temperatura,
            luz
        )

        print(
            "Diagnostico del cultivo:",
            prediccion
        )

        # ---------------------------------------------
        # REGISTRO DE SECUENCIAS
        # ---------------------------------------------

        if sequence_id is not None:

            sequence_id = int(sequence_id)

            with lock_secuencias:

                secuencias_recibidas.add(sequence_id)

                ultimo_sequence_id = sequence_id

        perdida = calcular_perdida_paquetes()

        # ---------------------------------------------
        # GUARDAR EN SQLITE
        # ---------------------------------------------

        nueva_lectura = LecturaIoT(

            valor=humedad,

            humedad=humedad,
            temperatura=temperatura,
            luz=luz,

            sequence_id=sequence_id,
            node_id=node_id,

            timestamp_ms=timestamp_ms,
            timestamp_unix_ms=timestamp_unix_ms,

            latencia=latencia,

            prediccion=prediccion,

            fecha_registro=datetime.now(
                timezone.utc
            ).replace(tzinfo=None)

        )

        db.add(nueva_lectura)

        db.commit()

        db.refresh(nueva_lectura)

        # ---------------------------------------------
        # MENSAJE PARA EL DASHBOARD
        # ---------------------------------------------

        mensaje = {

            "id": nueva_lectura.id,

            "node_id": node_id,

            "sequence_id": sequence_id,

            "humedad": humedad,

            "temperatura": temperatura,

            "luz": luz,

            "latencia": latencia,

            "perdida_paquetes_pct": perdida,

            "prediccion": prediccion,

            "timestamp": datetime.now(
                timezone.utc
            ).isoformat()

        }

        # ---------------------------------------------
        # ENVIAR POR WEBSOCKET
        # ---------------------------------------------

        if main_loop is not None and main_loop.is_running():

            asyncio.run_coroutine_threadsafe(
                enviar_a_clientes(mensaje),
                main_loop
            )

        else:

            print(
                "El event loop principal no esta disponible"
            )

        # ---------------------------------------------
        # MOSTRAR RESULTADOS
        # ---------------------------------------------

        print("----------------------------------")

        print("Telemetria recibida")
        print("Nodo:", node_id)
        print("Secuencia:", sequence_id)
        print("Humedad:", humedad)
        print("Temperatura:", temperatura)
        print("Luz:", luz)
        print("Latencia:", latencia)
        print("Perdida:", perdida, "%")
        print("Diagnostico:", prediccion)

        print("----------------------------------")

    except Exception as error:

        db.rollback()

        print(
            "Error procesando telemetria:",
            error
        )

    finally:

        db.close()


# =====================================================
# MQTT: CONEXION
# =====================================================

def on_connect(client, userdata, flags, rc):

    if rc == 0:

        print("Conexion MQTT exitosa")

        client.subscribe(
            TOPIC_TELEMETRIA
        )

        print(
            "Suscrito al topic:",
            TOPIC_TELEMETRIA
        )

    else:

        print(
            "Error de conexion MQTT:",
            rc
        )


mqtt_client = mqtt.Client()

mqtt_client.on_connect = on_connect

mqtt_client.on_message = on_message


# =====================================================
# INICIAR MQTT
# =====================================================

def iniciar_mqtt():

    try:

        mqtt_client.connect(
            MQTT_SERVER,
            MQTT_PORT,
            60
        )

        mqtt_client.loop_start()

        print("Cliente MQTT iniciado")

    except Exception as error:

        print(
            "No se pudo conectar con MQTT:",
            error
        )


# =====================================================
# EVENT LOOP PRINCIPAL
# =====================================================

@app.on_event("startup")
async def startup_event():

    global main_loop

    main_loop = asyncio.get_running_loop()

    print(
        "Event loop principal guardado correctamente"
    )

    iniciar_mqtt()


@app.on_event("shutdown")
async def shutdown_event():

    mqtt_client.loop_stop()

    mqtt_client.disconnect()


# =====================================================
# ENDPOINT PRINCIPAL
# =====================================================

@app.get("/")
def inicio():

    return {
        "mensaje": "Sistema IoT de monitoreo de invernadero funcionando",
        "estado": "activo",
        "modelo": "Random Forest + reglas de temperatura"
    }


# =====================================================
# CONSULTAR LECTURAS
# =====================================================

@app.get("/lecturas")
def obtener_lecturas():

    db = SessionLocal()

    try:

        lecturas = (
            db.query(LecturaIoT)
            .order_by(LecturaIoT.id.desc())
            .limit(100)
            .all()
        )

        resultado = []

        for lectura in lecturas:

            resultado.append({

                "id": lectura.id,

                "valor": lectura.valor,

                "humedad": lectura.humedad,

                "temperatura": lectura.temperatura,

                "luz": lectura.luz,

                "sequence_id": lectura.sequence_id,

                "node_id": lectura.node_id,

                "timestamp_ms": lectura.timestamp_ms,

                "timestamp_unix_ms": lectura.timestamp_unix_ms,

                "latencia": lectura.latencia,

                "prediccion": lectura.prediccion,

                "fecha_registro": (
                    lectura.fecha_registro.isoformat()
                    if lectura.fecha_registro
                    else None
                )

            })

        return resultado

    finally:

        db.close()


# =====================================================
# WEBSOCKET: TELEMETRIA
# =====================================================

@app.websocket("/ws/telemetria")
async def websocket_telemetria(websocket: WebSocket):

    await websocket.accept()

    with lock_clientes:
        clientes_ws.append(websocket)

    print("Dashboard conectado")

    try:

        while True:

            await websocket.receive_text()

    except WebSocketDisconnect:

        with lock_clientes:

            if websocket in clientes_ws:
                clientes_ws.remove(websocket)

        print("Dashboard desconectado")

    except Exception as error:

        with lock_clientes:

            if websocket in clientes_ws:
                clientes_ws.remove(websocket)

        print(
            "Error en WebSocket:",
            error
        )


# =====================================================
# WEBSOCKET: CONTROL DEL ESP32
# =====================================================

@app.websocket("/ws/control")
async def websocket_control(websocket: WebSocket):

    await websocket.accept()

    print("Conexion de control establecida")

    try:

        while True:

            mensaje = await websocket.receive_text()

            datos = json.loads(mensaje)

            accion = datos.get("action")

            if accion in ["ON", "OFF"]:

                payload = json.dumps({
                    "action": accion
                })

                resultado = mqtt_client.publish(
                    TOPIC_CONTROL,
                    payload
                )

                print(
                    "Comando enviado al ESP32:",
                    accion
                )

                await websocket.send_json({

                    "status": "enviado",

                    "action": accion,

                    "mqtt_result": resultado.rc

                })

            else:

                await websocket.send_json({

                    "status": "error",

                    "mensaje": "Accion no valida"

                })

    except WebSocketDisconnect:

        print("Conexion de control cerrada")

    except Exception as error:

        print(
            "Error en control:",
            error
        )


# =====================================================
# FIN DEL BACKEND
# =====================================================
