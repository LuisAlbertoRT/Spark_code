import os
import sys
from pathlib import Path
import random
from datetime import datetime, timedelta

# =========================================================================
# 1. BYPASS DE ENTORNO EN WINDOWS MEDIANTE RUTA RELATIVA SEGURA
# =========================================================================
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Guardar el directorio de trabajo actual (donde está tu proyecto Spark_code)
directorio_proyecto = os.getcwd()

# Obtener la carpeta exacta donde reside el ejecutable de Python de tu .venv
carpeta_binaria_venv = Path(sys.executable).parent

# Mover temporalmente el proceso de Python a la carpeta del .venv
os.chdir(carpeta_binaria_venv)

# Inyectar el ejecutable de forma relativa y limpia para burlar el espacio de "Luis Alberto"
os.environ["PYSPARK_PYTHON"] = "python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python.exe"

print(f"--> [INFO] Saltando espacios de Windows mediante Working Directory temporal.")
print(f"--> [INFO] PYSPARK_PYTHON seteado limpiamente como: {os.environ['PYSPARK_PYTHON']}")

# Regresar de inmediato al directorio original del proyecto para mantener las rutas de guardado
os.chdir(directorio_proyecto)

# =========================================================================
# 2. INYECCIÓN DE PARCHES JVM PARA RESTRICCIONES DE ARROW / JAVA 17
# =========================================================================
opciones_jvm = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.security=ALL-UNNAMED "
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Inicializar Spark Session bloqueando Arrow para prevenir errores crónicos de Netty en Windows
spark = SparkSession.builder \
    .appName("BBVA_AML_Compliance_Pipeline") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.enabled", "false") \
    .config("spark.driver.extraJavaOptions", opciones_jvm) \
    .config("spark.executor.extraJavaOptions", opciones_jvm) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

# =========================================================================
# 3. GENERACIÓN DE MATRIZ DE DATOS EN MEMORIA (PROCESAMIENTO SPARK)
# =========================================================================
print("--> [INFO] Generando matriz transaccional estática en memoria...")

canales = ["SPEI", "EFECTIVO_VENTANILLA", "CAJERO_AUTOMATICO"]
fecha_base = datetime(2026, 6, 25, 10, 0, 0)

data_sintetica = []
for i in range(15000):
    id_tx = f"TX_{i}"
    id_org = f"CLI_{random.randint(1, 400)}"
    id_dest = f"CLI_{random.randint(1, 400)}"
    monto = round(random.uniform(500, 180000), 2)
    canal = random.choice(canales)
    delta_tiempo = fecha_base - timedelta(seconds=random.randint(0, 86400))
    
    data_sintetica.append((id_tx, id_org, id_dest, monto, canal, delta_tiempo.strftime("%Y-%m-%d %H:%M:%S")))

columnas = ["id_transaccion", "id_origen", "id_destino", "monto", "tipo_canal", "timestamp_str"]
df_base = spark.createDataFrame(data_sintetica, schema=columnas)
df_transacciones = df_base.withColumn("timestamp", F.to_timestamp("timestamp_str"))

# =========================================================================
# 4. INGENIERÍA DE CARACTERÍSTICAS Y REGLAS DE NEGOCIO AML
# =========================================================================
print("--> [INFO] Calculando perfiles de riesgo mediante ventanas distribuidas...")

# Ventana temporal móvil de 24 horas agrupada por cliente emisor
ventana_24h = Window.partitionBy("id_origen").orderBy(F.col("timestamp").cast("long")).rangeBetween(-86400, 0)

df_features = df_transacciones.withColumn(
    "monto_acumulado_24h", F.round(F.sum("monto").over(ventana_24h), 2)
).withColumn(
    "conteo_transacciones_24h", F.count("id_transaccion").over(ventana_24h)
).withColumn(
    "monto_promedio_24h", F.round(F.avg("monto").over(ventana_24h), 2)
)

# Aplicación de Regla de Prevención de Lavado de Dinero (Estructuración / Pitufeo)
df_analisis = df_features.withColumn(
    "sospecha_estructuracion",
    F.when((F.col("conteo_transacciones_24h") >= 3) & (F.col("monto_promedio_24h") < 90000), 1).otherwise(0)
)

# =========================================================================
# 5. BYPASS SEGURO DE PERSISTENCIA LOCAL (STANDARD SERIALIZER)
# =========================================================================
print("--> [INFO] Ejecutando bypass de persistencia seguro via Pandas...")

# Recolección limpia y segura desde la JVM hacia la memoria nativa de Python
df_pandas = df_analisis.toPandas()

# Garantizar la existencia de la carpeta destino en Windows
os.makedirs("./proyecto_aml", exist_ok=True)
ruta_destino = "./proyecto_aml/datos_estructurados_locales.csv"

# Escritura directa controlada por Python, evitando llamadas nativas a Hadoop
df_pandas.to_csv(ruta_destino, index=False)

print(f"\n==================================================================")
print(f"--> [OK] ¡PIPELINE DE SPARK COMPLETADO EXITOSAMENTE!")
print(f"--> Dataset verificado y persistido en: {ruta_destino}")
print(f"==================================================================")

spark.stop()