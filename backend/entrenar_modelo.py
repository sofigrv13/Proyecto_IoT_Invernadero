
import pandas as pd
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report


datos = {
    "humedad_raw": [
        3119, 1657, 2976, 3011,
        3104, 1703, 2965, 3002,
        2837, 2997, 3088, 3110,
        1648, 2945, 3039, 3120
    ],

    "temperatura_c": [
        23.35, 24.42, 25.36, 26.03,
        26.75, 26.66, 26.63, 26.59,
        26.01, 25.63, 24.50, 24.00,
        26.55, 26.55, 26.66, 26.61
    ],

    "luz_raw": [
        1413, 1891, 2007, 1380,
        972, 683, 845, 557,
        654, 357, 126, 126,
        1478, 1379, 1265, 987
    ],

    "diagnostico": [
        "Estres Hidrico",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Estres Hidrico",
        "Hidratacion Optima",
        "Exceso de Agua",
        "Hidratacion Optima",
        "Hidratacion Optima",
        "Hidratacion Optima"
    ]
}


df = pd.DataFrame(datos)

print("===== DATASET =====")
print(df)

print("\n===== CANTIDAD POR CLASE =====")
print(df["diagnostico"].value_counts())


X = df[["humedad_raw", "temperatura_c", "luz_raw"]]
y = df["diagnostico"]


modelo = RandomForestClassifier(
    n_estimators=100,
    random_state=42,
    class_weight="balanced"
)


modelo.fit(X, y)


predicciones = modelo.predict(X)

precision = accuracy_score(y, predicciones)


print("\n===== RESULTADOS =====")
print("Precision sobre datos de entrenamiento:", precision)

print("\n===== REPORTE DE CLASIFICACION =====")

print(
    classification_report(
        y,
        predicciones,
        zero_division=0
    )
)


print("\n===== PREDICCIONES =====")

for i in range(len(df)):
    print(
        "Real:", y.iloc[i],
        "| Prediccion:", predicciones[i]
    )


joblib.dump(modelo, "modelo_random_forest.pkl")

print("\nModelo guardado correctamente:")
print("modelo_random_forest.pkl")
