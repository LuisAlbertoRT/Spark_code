# Sistema Inteligente de Detección de Lavado de Dinero (AML) 🚀

Este proyecto implementa un pipeline integral (*End-to-End*) para la prevención y detección de lavado de dinero (AML/PLD), especializado en la identificación de **alertas por estructuración (pitufeo)** en operaciones financieras transaccionales. 

El ecosistema combina el procesamiento distribuido en memoria de **PySpark**, el análisis topológico de redes con **NetworkX** y la clasificación predictiva mediante **Machine Learning (Random Forest)**.

---

## ⚙️ Arquitectura del Sistema

El flujo de información se divide en tres módulos secuenciales desacoplados para garantizar la escalabilidad y facilitar el despliegue en entornos locales:


1. **Ingeniería de Características en Tiempo Real (PySpark):** Simula y procesa flujos masivos de transferencias (`SPEI`, `Efectivo`, `Cajero`). Calcula perfiles agregados mediante ventanas analíticas móviles de 24 horas (`monto_acumulado_24h`, `conteo_transacciones_24h`) para encender las primeras alertas regulatorias.
2. **Enriquecimiento Topológico (NetworkX):** Construye un grafo dirigido del flujo del dinero. Transforma las cuentas en nodos y las operaciones en aristas, extrayendo métricas de red avanzadas como la centralidad de entrada/salida y el algoritmo *PageRank* ponderado por monto para identificar nodos concentradores o dispersores de fondos.
3. **Clasificador Predictivo (Scikit-Learn):** Fusiona las variables transaccionales con los indicadores del grafo. Entrena un modelo *Random Forest* con pesos de clase balanceados para resolver el desbalanceo natural del riesgo financiero, evaluando la probabilidad final de lavado de dinero.

---

## 📁 Estructura del Repositorio

```text
Spark_code/
│
├── .venv/─────────────── # Entorno virtual de Python (Excluido en .gitignore)
├── proyecto_aml/──────── # Carpeta local de artefactos generados (Excluida en .gitignore)
│   ├── datos_estructurados_locales.csv ── # Salida del pipeline de Spark
│   ├── metricas_topologicas_grafo.csv ─── # Métricas extraídas del Grafo
│   └── subgrafo_alertas_aml.png ───────── # Visualización del mapa de riesgo
│
├── aml_pyspark_pipeline.py ── # Módulo 1: Procesamiento masivo y ventanas Spark
├── aml_graph_module.py ────── # Módulo 2: Modelado de red y centralidad de conexiones
├── aml_predictive_model.py ── # Módulo 3: Entrenamiento y evaluación de Machine Learning
├── .gitignore ─────────────── # Reglas de exclusión de archivos pesados/entorno
└── README.md ──────────────── # Documentación del proyecto
