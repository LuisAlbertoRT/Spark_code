import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score

# =========================================================================
# 1. CARGA Y CONSOLIDACIÓN DE DATOS (MÉTRICAS TRANSACCIONALES + GRAFO)
# =========================================================================
ruta_tx = "./proyecto_aml/datos_estructurados_locales.csv"
ruta_grafo = "./proyecto_aml/metricas_topologicas_grafo.csv"

if not os.path.exists(ruta_tx) or not os.path.exists(ruta_grafo):
    raise FileNotFoundError("Faltan archivos base. Asegúrate de correr primero el pipeline de Spark y el módulo de grafos.")

print("--> [INFO] Cargando datasets para consolidación de features...")
df_tx = pd.read_csv(ruta_tx)
df_grafo = pd.read_csv(ruta_grafo)

print("--> [INFO] Fusionando variables transaccionales con métricas topológicas...")
df_master = pd.merge(
    df_tx, 
    df_grafo, 
    left_on="id_origen", 
    right_on="id_cliente", 
    how="left"
).fillna(0)

# =========================================================================
# 2. PREPARACIÓN DE MATRICES DE ENTRENAMIENTO (X, y)
# =========================================================================
features = [
    'monto', 
    'monto_acumulado_24h', 
    'conteo_transacciones_24h', 
    'monto_promedio_24h',
    'centralidad_entrada', 
    'centralidad_salida', 
    'score_pagerank'
]

X = df_master[features]
y = df_master['sospecha_estructuracion']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"--> [OK] Matrices listas. Entrenamiento: {X_train.shape[0]} muestras | Prueba: {X_test.shape[0]} muestras.")

# =========================================================================
# 3. ENTRENAMIENTO DEL MODELO DE MACHINE LEARNING (RANDOM FOREST)
# =========================================================================
print("--> [INFO] Entrenando Clasificador Random Forest para Cumplimiento AML...")

modelo_pld = RandomForestClassifier(
    n_estimators=100,
    max_depth=8,
    class_weight="balanced", 
    random_state=42
)

modelo_pld.fit(X_train, y_train)
print("--> [OK] ¡Modelo entrenado con éxito!")

# =========================================================================
# 4. EVALUACIÓN DE CAPACIDAD PREDICTIVA Y MÉTRICAS DE CONTROL
# =========================================================================
print("\n==================================================================")
print("--> EVALUACIÓN DEL MODELO PREDICTIVO AML")
print("==================================================================")

y_pred = modelo_pld.predict(X_test)
y_prob = modelo_pld.predict_proba(X_test)[:, 1]

print("\nReporte de Clasificación:")
print(classification_report(y_test, y_pred))

auc_score = roc_auc_score(y_test, y_prob)
print(f"Métrica ROC-AUC global: {auc_score:.4f}")

print("\nImportancia de las variables en la detección de riesgo:")
importancias = modelo_pld.feature_importances_
for feat, imp in sorted(zip(features, importancias), key=lambda x: x[1], reverse=True):
    print(f" * {feat:<25}: {imp*100:.2f}%")

print(f"\n==================================================================")
print(f"--> [OK] ¡SISTEMA END-TO-END COMPLETADO EXITOSAMENTE!")
print(f"==================================================================")