
import asyncio
import json
import threading
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import (
    Column,
    Integer,
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
    latencia = Column(Float)
    prediccion = Column(String)
    fecha_registro = Column(DateTime)


Base.metadata.create_all(bind=engine)


def actualizar_esquema():
    columnas_nuevas = {
        "humedad": "FLOAT",
        "temperatura": "FLOAT",
        "luz": "FLOAT",
        "node_id": "VARCHAR",
        "timestamp_ms": "INTEGER",
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


MQTT_SERVER = "localhost"
MQTT_PORT = 1883

MQTT_TOPIC = "sensores/esp32/telemetria"
CONTROL_TOPIC = "dispositivos/esp32/control"

mqtt_cliente = None
clientes_websocket = set()

cola_telemetria = None
bucle_principal = None

ultima_secuencia = None
paquetes_recibidos = 0
paquetes_perdidos = 0


def calcular_perdida(sequence_id):
    global ultima_secuencia
    global paquetes_recibidos
    global paquetes_perdidos

    if ultima_secuencia is not None:
        if sequence_id <= ultima_secuencia:
            # El contador del ESP32 pudo reiniciarse.
            paquetes_recibidos = 0
            paquetes_perdidos = 0
            ultima_secuencia = None

    if ultima_secuencia is not None:
        if sequence_id > ultima_secuencia + 1:
            paquetes_perdidos += sequence_id - ultima_secuencia - 1

    paquetes_recibidos += 1
    ultima_secuencia = sequence_id

    total_esperado = paquetes_recibidos + paquetes_perdidos

    if total_esperado == 0:
        return 0.0

    return paquetes_perdidos / total_esperado * 100


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado a MQTT")
        client.subscribe(MQTT_TOPIC)
        print("Suscrito a:")
        print(MQTT_TOPIC)
    else:
        print("Error de conexion MQTT:", rc)


def on_message(client, userdata, msg):
    try:
        mensaje = msg.payload.decode("utf-8")
        datos = json.loads(mensaje)

        print("Mensaje MQTT recibido:")
        print(mensaje)

        node_id = datos["node_id"]
        sequence_id = int(datos["sequence_id"])
        timestamp_ms = int(datos["timestamp_ms"])

        sensores = datos["data"]

        humedad = float(sensores["humedad"])
        temperatura = float(sensores["temperatura"])
        luz = float(sensores["luz"])

        fecha_actual = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        # timestamp_ms viene de millis() del ESP32.
        # No equivale a la hora actual de la computadora.
        # Por eso no calculamos una latencia falsa.
        latencia = None

        perdida = calcular_perdida(sequence_id)

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
                latencia=latencia,
                prediccion="Pendiente de modelo Random Forest",
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

        mensaje_websocket = {
            "id": lectura.id,
            "node_id": node_id,
            "sequence_id": sequence_id,
            "timestamp_ms": timestamp_ms,
            "humedad": humedad,
            "temperatura": temperatura,
            "luz": luz,
            "latencia": latencia,
            "perdida_paquetes_pct": round(perdida, 2),
            "prediccion": "Pendiente de modelo Random Forest",
            "fecha_registro": fecha_actual.isoformat()
        }

        if bucle_principal is not None and cola_telemetria is not None:
            bucle_principal.call_soon_threadsafe(
                cola_telemetria.put_nowait,
                json.dumps(mensaje_websocket)
            )

        print("Lectura guardada en SQLite")
        print("Humedad:", humedad)
        print("Temperatura:", temperatura)
        print("Luz:", luz)
        print("Perdida estimada de paquetes:", round(perdida, 2), "%")

    except Exception as error:
        print("Error procesando mensaje MQTT:", error)


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
        print("Error en el cliente MQTT:", error)


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

    asyncio.create_task(difundir_telemetria())


@app.get("/")
def inicio():
    return {
        "mensaje": "Backend IoT funcionando"
    }


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
                "latencia": lectura.latencia,
                "prediccion": lectura.prediccion,
                "fecha_registro": (
                    lectura.fecha_registro.isoformat()
                    if lectura.fecha_registro else None
                )
            }
            for lectura in lecturas
        ]

    finally:
        db.close()


@app.websocket("/ws/telemetria")
async def websocket_telemetria(websocket: WebSocket):
    await websocket.accept()
    clientes_websocket.add(websocket)

    print("Cliente WebSocket conectado")

    try:
        while True:
            mensaje = await websocket.receive_text()

            print("Mensaje recibido desde WebSocket:")
            print(mensaje)

            try:
                datos = json.loads(mensaje)
                action = datos.get("action")

                if action in ["ON", "OFF"]:
                    if mqtt_cliente is not None and mqtt_cliente.is_connected():
                        comando = json.dumps({
                            "action": action
                        })

                        resultado = mqtt_cliente.publish(
                            CONTROL_TOPIC,
                            comando
                        )

                        if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
                            print("Comando enviado por MQTT:", comando)
                        else:
                            print("No se pudo publicar el comando MQTT")

                    else:
                        print("MQTT no esta conectado")

            except (ValueError, TypeError) as error:
                print("Comando no valido:", error)

    except WebSocketDisconnect:
        pass

    except Exception as error:
        print("Error en WebSocket:", error)

    finally:
        clientes_websocket.discard(websocket)
        print("Cliente WebSocket desconectado")
