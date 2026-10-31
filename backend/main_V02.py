
import asyncio
import json
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    Float,
    DateTime,
    String,
    create_engine,
    inspect,
    text
)
from sqlalchemy.orm import declarative_base, sessionmaker


app = FastAPI()

DATABASE_URL = "sqlite:///./iot.db"

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


# --------------------------------------------------
# TABLA DE SQLITE
# --------------------------------------------------

class LecturaIoT(Base):
    __tablename__ = "lecturas"

    id = Column(Integer, primary_key=True, index=True)
    valor = Column(Float)
    humedad = Column(Float)
    temperatura = Column(Float)
    luz = Column(Float)
    sequence_id = Column(Integer)
    node_id = Column(String)
    timestamp_ms = Column(Integer)
    timestamp_unix_ms = Column(BigInteger)
    latencia = Column(Float)
    prediccion = Column(String)
    fecha_registro = Column(DateTime)


Base.metadata.create_all(bind=engine)


# --------------------------------------------------
# ACTUALIZAR ESTRUCTURA DE LA BASE DE DATOS
# --------------------------------------------------

def actualizar_esquema():

    columnas_nuevas = {
        "humedad": "FLOAT",
        "temperatura": "FLOAT",
        "luz": "FLOAT",
        "node_id": "VARCHAR",
        "timestamp_ms": "INTEGER",
        "timestamp_unix_ms": "BIGINT",
        "prediccion": "VARCHAR"
    }

    columnas_actuales = {
        columna["name"]
        for columna in inspect(engine).get_columns("lecturas")
    }

    with engine.begin() as conexion:

        for nombre, tipo in columnas_nuevas.items():

            if nombre not in columnas_actuales:

                conexion.execute(
                    text(
                        f"ALTER TABLE lecturas "
                        f"ADD COLUMN {nombre} {tipo}"
                    )
                )


actualizar_esquema()


# --------------------------------------------------
# CONFIGURACION MQTT
# --------------------------------------------------

MQTT_SERVER = "localhost"
MQTT_PORT = 1883

MQTT_TOPIC = "sensores/esp32/telemetria"
CONTROL_TOPIC = "dispositivos/esp32/control"

mqtt_cliente = None

clientes_websocket = set()

cola_telemetria = None
bucle_principal = None

estadisticas_por_nodo = {}


# --------------------------------------------------
# CALCULAR PERDIDA DE PAQUETES
# --------------------------------------------------

def calcular_perdida(node_id, sequence_id):

    estado = estadisticas_por_nodo.get(node_id)

    # Detectar el primer mensaje o un reinicio del ESP32.
    if estado is None or (
        sequence_id == 1 and estado["max"] > 1
    ):

        estado = {
            "min": sequence_id,
            "max": sequence_id,
            "vistas": set()
        }

        estadisticas_por_nodo[node_id] = estado

    # Detectar mensajes duplicados.
    if sequence_id in estado["vistas"]:

        esperados = estado["max"] - estado["min"] + 1
        recibidos = len(estado["vistas"])

        perdida = (
            (esperados - recibidos) / esperados * 100
            if esperados > 0 else 0.0
        )

        return round(perdida, 2), True

    # Registrar una secuencia nueva.
    estado["vistas"].add(sequence_id)

    estado["min"] = min(
        estado["min"],
        sequence_id
    )

    estado["max"] = max(
        estado["max"],
        sequence_id
    )

    total_esperado = (
        estado["max"] - estado["min"] + 1
    )

    total_recibido = len(estado["vistas"])

    if total_esperado <= 0:
        return 0.0, False

    perdida = (
        (total_esperado - total_recibido)
        / total_esperado
    ) * 100

    return round(perdida, 2), False


# --------------------------------------------------
# CONEXION MQTT
# --------------------------------------------------

def on_connect(client, userdata, flags, rc):

    if rc == 0:

        print("Conectado a MQTT")

        client.subscribe(MQTT_TOPIC)

        print("Suscrito a:", MQTT_TOPIC)

    else:

        print("Error de conexion MQTT:", rc)


# --------------------------------------------------
# RECEPCION Y PROCESAMIENTO DE TELEMETRIA
# --------------------------------------------------

def on_message(client, userdata, msg):

    try:

        mensaje = msg.payload.decode("utf-8")

        datos = json.loads(mensaje)

        print("Mensaje MQTT recibido:")
        print(mensaje)

        # Identificacion del dispositivo.
        node_id = datos["node_id"]

        sequence_id = int(datos["sequence_id"])

        timestamp_ms = int(datos["timestamp_ms"])

        # Nueva marca de tiempo sincronizada por NTP.
        timestamp_unix_ms = datos.get(
            "timestamp_unix_ms"
        )

        if timestamp_unix_ms is not None:
            timestamp_unix_ms = int(timestamp_unix_ms)

        # Obtener las variables de los sensores.
        sensores = datos["data"]

        humedad = float(sensores["humedad"])

        temperatura = float(sensores["temperatura"])

        luz = float(sensores["luz"])

        # Hora de recepcion en la computadora.
        fecha_actual = datetime.now(
            timezone.utc
        ).replace(tzinfo=None)

        recepcion_unix_ms = int(
            time.time() * 1000
        )

        # ------------------------------------------
        # CALCULAR LATENCIA
        # ------------------------------------------

        latencia = None

        if (
            timestamp_unix_ms is not None
            and timestamp_unix_ms > 0
        ):

            diferencia = (
                recepcion_unix_ms - timestamp_unix_ms
            )

            if diferencia >= 0:

                latencia = round(
                    float(diferencia),
                    2
                )

            else:

                print(
                    "Aviso: el reloj del ESP32 esta "
                    "adelantado respecto a la computadora."
                )

        # ------------------------------------------
        # CALCULAR PERDIDA DE PAQUETES
        # ------------------------------------------

        perdida, duplicado = calcular_perdida(
            node_id,
            sequence_id
        )

        if duplicado:

            print(
                "Mensaje duplicado ignorado:",
                node_id,
                "secuencia",
                sequence_id
            )

            return

        # ------------------------------------------
        # GUARDAR EN SQLITE
        # ------------------------------------------

        db = SessionLocal()

        try:

            lectura = LecturaIoT(

                valor=humedad,

                humedad=humedad,

                temperatura=temperatura,

                luz=luz,

                sequence_id=sequence_id,

                node_id=node_id,

                timestamp_ms=timestamp_ms,

                timestamp_unix_ms=timestamp_unix_ms,

                latencia=latencia,

                prediccion=(
                    "Pendiente de modelo Random Forest"
                ),

                fecha_registro=fecha_actual
            )

            db.add(lectura)

            db.commit()

            db.refresh(lectura)

        except Exception:

            db.rollback()

            raise

        finally:

            db.close()

        # ------------------------------------------
        # ENVIAR DATOS AL DASHBOARD
        # ------------------------------------------

        mensaje_websocket = {

            "id": lectura.id,

            "node_id": node_id,

            "sequence_id": sequence_id,

            "timestamp_ms": timestamp_ms,

            "timestamp_unix_ms": timestamp_unix_ms,

            "humedad": humedad,

            "temperatura": temperatura,

            "luz": luz,

            "latencia": latencia,

            "perdida_paquetes_pct": perdida,

            "prediccion": (
                "Pendiente de modelo Random Forest"
            ),

            "fecha_registro": fecha_actual.isoformat()
        }

        if (
            bucle_principal is not None
            and cola_telemetria is not None
        ):

            bucle_principal.call_soon_threadsafe(

                cola_telemetria.put_nowait,

                json.dumps(mensaje_websocket)

            )

        # ------------------------------------------
        # MOSTRAR INFORMACION EN TERMINAL
        # ------------------------------------------

        print("Lectura guardada en SQLite")

        print("Humedad:", humedad)

        print("Temperatura:", temperatura)

        print("Luz:", luz)

        print("Latencia:", latencia, "ms")

        print(
            "Perdida estimada de paquetes:",
            perdida,
            "%"
        )

    except Exception as error:

        print(
            "Error procesando mensaje MQTT:",
            error
        )


# --------------------------------------------------
# INICIAR CLIENTE MQTT
# --------------------------------------------------

def iniciar_mqtt():

    global mqtt_cliente

    try:

        mqtt_cliente = mqtt.Client()

        mqtt_cliente.on_connect = on_connect

        mqtt_cliente.on_message = on_message

        mqtt_cliente.connect(
            MQTT_SERVER,
            MQTT_PORT,
            60
        )

        mqtt_cliente.loop_forever()

    except Exception as error:

        print(
            "Error en el cliente MQTT:",
            error
        )


# --------------------------------------------------
# DIFUNDIR TELEMETRIA POR WEBSOCKET
# --------------------------------------------------

async def difundir_telemetria():

    while True:

        mensaje = await cola_telemetria.get()

        desconectados = []

        for websocket in list(clientes_websocket):

            try:

                await websocket.send_text(mensaje)

            except Exception:

                desconectados.append(websocket)

        for websocket in desconectados:

            clientes_websocket.discard(websocket)


# --------------------------------------------------
# INICIAR FASTAPI
# --------------------------------------------------

@app.on_event("startup")
async def iniciar_servidor():

    global cola_telemetria

    global bucle_principal

    cola_telemetria = asyncio.Queue()

    bucle_principal = asyncio.get_running_loop()

    threading.Thread(
        target=iniciar_mqtt,
        daemon=True
    ).start()

    asyncio.create_task(
        difundir_telemetria()
    )


# --------------------------------------------------
# RUTA PRINCIPAL
# --------------------------------------------------

@app.get("/")
def inicio():

    return {
        "mensaje": "Backend IoT funcionando"
    }


# --------------------------------------------------
# CONSULTAR REGISTROS
# --------------------------------------------------

@app.get("/lecturas")
def obtener_lecturas():

    db = SessionLocal()

    try:

        lecturas = (
            db.query(LecturaIoT)
            .order_by(LecturaIoT.id.desc())
            .limit(200)
            .all()
        )

        return [

            {

                "id": lectura.id,

                "node_id": lectura.node_id,

                "valor": lectura.valor,

                "humedad": lectura.humedad,

                "temperatura": lectura.temperatura,

                "luz": lectura.luz,

                "sequence_id": lectura.sequence_id,

                "timestamp_ms": lectura.timestamp_ms,

                "timestamp_unix_ms": (
                    lectura.timestamp_unix_ms
                ),

                "latencia": lectura.latencia,

                "prediccion": lectura.prediccion,

                "fecha_registro": (

                    lectura.fecha_registro.isoformat()

                    if lectura.fecha_registro

                    else None
                )
            }

            for lectura in lecturas

        ]

    finally:

        db.close()


# --------------------------------------------------
# WEBSOCKET Y CONTROL DEL ESP32
# --------------------------------------------------

@app.websocket("/ws/telemetria")
async def websocket_telemetria(websocket: WebSocket):

    await websocket.accept()

    clientes_websocket.add(websocket)

    print("Cliente WebSocket conectado")

    try:

        while True:

            mensaje = await websocket.receive_text()

            print(
                "Mensaje recibido desde WebSocket:"
            )

            print(mensaje)

            try:

                datos = json.loads(mensaje)

                action = datos.get("action")

                if action in ["ON", "OFF"]:

                    if (
                        mqtt_cliente is not None
                        and mqtt_cliente.is_connected()
                    ):

                        comando = json.dumps({
                            "action": action
                        })

                        resultado = mqtt_cliente.publish(

                            CONTROL_TOPIC,

                            comando

                        )

                        if (
                            resultado.rc
                            == mqtt.MQTT_ERR_SUCCESS
                        ):

                            print(
                                "Comando enviado por MQTT:",
                                comando
                            )

                        else:

                            print(
                                "No se pudo publicar "
                                "el comando MQTT"
                            )

                    else:

                        print(
                            "MQTT no esta conectado"
                        )

            except (ValueError, TypeError) as error:

                print(
                    "Comando no valido:",
                    error
                )

    except WebSocketDisconnect:

        pass

    except Exception as error:

        print(
            "Error en WebSocket:",
            error
        )

    finally:

        clientes_websocket.discard(websocket)

        print("Cliente WebSocket desconectado")
