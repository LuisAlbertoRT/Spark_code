import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Configuración de Entorno Consolidada para tu PC
ruta_python = r"C:\Users\Luis Alberto\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["JAVA_HOME"] = r"C:\java\jdk-17.0.19+10"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = ruta_python
os.environ["PYSPARK_DRIVER_PYTHON"] = ruta_python
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Añadir site-packages de tu Python 3.11
ruta_librerias = r"C:\Users\Luis Alberto\AppData\Local\Programs\Python\Python311\Lib\site-packages"
if ruta_librerias not in sys.path:
    sys.path.insert(0, ruta_librerias)

sys.path.append(os.path.join(os.environ["HADOOP_HOME"], "bin"))
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

# 2. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("BBVA_AML_Compliance_Pipeline") \
    .master("local[2]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.enabled", "false") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .getOrCreate()

print("--> [INFO] Generando transacciones simuladas de alta densidad...")

# 3. Simular Dataset Transaccional Financiero
# Generamos 500,000 transacciones para simular comportamiento
df_base = spark.range(0, 500000)
df_transacciones = df_base.withColumn(
    "id_transaccion", F.concat(F.lit("TX_"), F.col("id"))
).withColumn(
    "id_origen", F.concat(F.lit("CLI_"), F.floor(F.rand() * 5000)) # 5,000 clientes
).withColumn(
    "id_destino", F.concat(F.lit("CLI_"), F.floor(F.rand() * 5000))
).withColumn(
    "monto", F.round((F.rand() * 200000) + 5, 2)
).withColumn(
    "tipo_canal", 
    F.when(F.rand() < 0.3, "SPEI")
     .when(F.rand() < 0.65, "EFECTIVO_VENTANILLA")
     .otherwise("CAJERO_AUTOMATICO")
).withColumn(
    "timestamp", 
    F.current_timestamp() - F.expr("CAST(rand() * 100000 AS INT) * INTERVAL 1 SECOND")
).drop("id")

# 4. Ingeniería de Variables con Enfoque Antilavado (Ventanas Lógicas)
print("--> [INFO] Calculando perfiles de riesgo y variables de estructuración...")

# Ventana para analizar las transacciones de las últimas 24 horas por cliente de origen
# 24 horas = 86,400 segundos
ventana_24h = Window.partitionBy("id_origen") \
                    .orderBy(F.col("timestamp").cast("long")) \
                    .rangeBetween(-86400, 0)

df_features = df_transacciones.withColumn(
    "monto_acumulado_24h", F.round(F.sum("monto").over(ventana_24h), 2)
).withColumn(
    "conteo_transacciones_24h", F.count("id_transaccion").over(ventana_24h)
).withColumn(
    "monto_promedio_24h", F.round(F.avg("monto").over(ventana_24h), 2)
)

# 5. Regla de Cumplimiento (Compliance Rule)
# Marcamos transacciones sospechosas de pitufeo: más de 5 operaciones en 24 horas 
# donde los montos individuales suelen mantenerse por debajo de un umbral regulatorio ficticio (e.g., 50,000)
df_analisis = df_features.withColumn(
    "sospecha_estructuracion",
    F.when((F.col("conteo_transacciones_24h") >= 5) & (F.col("monto_promedio_24h") < 50000), 1).otherwise(0)
)

# 6. Mostrar métricas ejecutadas
print("\n=== MONITOREO DE OPERACIONES CON SOSPECHA DE ESTRUCTURACIÓN ===")
df_analisis.filter(F.col("sospecha_estructuracion") == 1) \
           .select("id_origen", "timestamp", "monto", "conteo_transacciones_24h", "monto_acumulado_24h") \
           .orderBy(F.desc("conteo_transacciones_24h")) \
           .show(10, truncate=False)

# 7. Guardar en formato optimizado de datos
ruta_parquet = "./proyecto_aml/datos_estructurados.parquet"
df_analisis.write.mode("overwrite").parquet(ruta_parquet)
print(f"--> [INFO] Datos del pipeline de Big Data guardados en: {ruta_parquet}")

spark.stop()