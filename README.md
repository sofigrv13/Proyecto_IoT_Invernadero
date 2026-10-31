\# Monitoreo IoT de Cultivos en Invernaderos Inteligentes



\## Descripción del proyecto



Sistema de monitoreo y diagnóstico automatizado de cultivos mediante tecnologías de Internet de las Cosas (IoT), desarrollado para su aplicación en invernaderos inteligentes.



El proyecto utiliza sensores conectados a un microcontrolador ESP32 para obtener información sobre las condiciones del cultivo. Los datos se transmiten mediante el protocolo MQTT hacia un servidor que procesa la información, realiza diagnósticos mediante un modelo de Machine Learning y permite visualizar las lecturas desde una interfaz web.



\## Objetivo



Desarrollar un sistema IoT capaz de monitorear variables ambientales y del suelo para identificar condiciones relacionadas con el estado de los cultivos, facilitando su supervisión y diagnóstico.



\## Arquitectura del sistema



El sistema está integrado por los siguientes componentes:



1\. \*\*Adquisición de datos:\*\* ESP32 conectado a sensores de humedad del suelo, temperatura y luminosidad.

2\. \*\*Comunicación:\*\* protocolo MQTT para transmitir la telemetría y recibir instrucciones.

3\. \*\*Backend:\*\* servidor desarrollado con Python y FastAPI para procesar la información.

4\. \*\*Machine Learning:\*\* modelo Random Forest para realizar la clasificación de las condiciones del cultivo.

5\. \*\*Base de datos:\*\* SQLite para almacenar información de las lecturas.

6\. \*\*Frontend:\*\* interfaz web desarrollada con HTML y JavaScript para visualizar información y gestionar el sistema.



\### Flujo de comunicación



\*\*Telemetría:\*\*



ESP32 → MQTT → Backend → Random Forest → SQLite → Dashboard.



\*\*Control:\*\*



Dashboard → Backend → MQTT → ESP32.



\## Tecnologías utilizadas



\- ESP32

\- Arduino IDE

\- Python

\- FastAPI

\- MQTT / Mosquitto

\- Random Forest

\- Scikit-learn

\- SQLite

\- SQLAlchemy

\- HTML

\- JavaScript

\- Chart.js

\- WebSockets



\## Sensores utilizados



\- HW-390: sensor de humedad del suelo.

\- LM35: sensor de temperatura.

\- OKY3101: sensor de luminosidad.



\## Estructura del repositorio



```text

Proyecto\_IoT\_Invernadero/

│

├── backend/

│   ├── main.py

│   ├── entrenar\_modelo.py

│   ├── main\_anterior.py

│   ├── main\_V02.py

│   └── iot.db

│

├── esp32/

│   └── Invernadero\_IOT.ino

│

├── frontend/

│   └── index.html

│

├── docs/

│

├── dataset\_invernadero.csv

├── modelo\_random\_forest.pkl

├── README.md

└── .gitignore

```



\## Ejecución del proyecto



Para ejecutar el sistema se requiere contar con:



\- Python y las dependencias utilizadas por el backend.

\- Broker MQTT Mosquitto.

\- Arduino IDE para trabajar con el ESP32.

\- Conexión de los sensores y el microcontrolador.

\- Navegador web para acceder al dashboard.



La configuración de los componentes y servicios debe realizarse de acuerdo con los parámetros definidos en los archivos del proyecto.



\## Propósito académico



Este repositorio reúne el desarrollo y los recursos correspondientes al proyecto integrador de IoT, incluyendo el firmware del ESP32, el backend, la interfaz web y los archivos relacionados con el modelo de clasificación.



\---



\*\*Proyecto:\*\* Monitoreo Multisensor y Diagnóstico Automatizado de Cultivos mediante IoT en Invernaderos Inteligentes.



\*\*Repositorio académico

