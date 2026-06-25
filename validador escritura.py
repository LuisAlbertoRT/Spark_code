import os
import sys
import shutil

print("================================================================")
print("     INICIANDO DIAGNÓSTICO DE ESCRITURA Y ENTORNO SPARK         ")
print("================================================================")

# 1. Verificar Variables de Entorno Clave
variables = ["JAVA_HOME", "HADOOP_HOME", "SPARK_LOCAL_IP", "PYSPARK_PYTHON"]
print("\n[1] Revisando Variables de Entorno:")
for var in variables:
    valor = os.environ.get(var, "❌ NO DEFINIDA")
    print(f"   -> {var}: {valor}")

# 2. Forzar Configuración del Entorno para la prueba
os.environ["JAVA_HOME"] = r"C:\java\jdk-17.0.19+10" 
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

sys.path.append(os.path.join(os.environ["HADOOP_HOME"], "bin"))
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

# 3. Probar si Python puro puede escribir en la ruta destino
print("\n[2] Probando permisos de escritura de Windows (Sin Spark)...")
test_dir = "./proyecto_aml/prueba_permisos"
try:
    os.makedirs(test_dir, exist_ok=True)
    with open(os.path.join(test_dir, "test.txt"), "w") as f:
        f.write("Python puede escribir aquí.")
    print("   -> [OK] Windows permite crear y escribir carpetas localmente.")
    shutil.rmtree(test_dir)
except Exception as e:
    print(f"   -> [❌ ERROR DE PERMISOS EN WINDOWS]: {e}")
    sys.exit()

# 4. Levantar SparkSession con Parches Inyectados
print("\n[3] Levantando SparkSession de Diagnóstico...")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("AML_Diagnostic_Tool") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
    .getOrCreate()

# 5. Crear un DataFrame mínimo de prueba
data = [("CLI_1", "CLI_2", 5000.0), ("CLI_3", "CLI_4", 12500.5)]
columnas = ["id_origen", "id_destino", "monto"]
df = spark.createDataFrame(data, schema=columnas).coalesce(1)

print(f"   -> [OK] DataFrame de prueba creado en memoria. Filas: {df.count()}")

# 6. BATALLA FINAL: Probar los 3 métodos de persistencia para ver cuál sobrevive
print("\n[4] Ejecutando pruebas de persistencia distribuidas...")

# --- PRUEBA A: PARQUET ---
ruta_a = "./proyecto_aml/test_parquet.parquet"
print("\n   Ejecutando Prueba A (Formato Parquet)...")
try:
    df.write.mode("overwrite").parquet(ruta_a)
    archivos = os.listdir(ruta_a) if os.path.exists(ruta_a) else []
    print(f"   -> Resultado de archivos en carpeta Parquet: {archivos}")
    if any("part-" in f for f in archivos):
        print("   -> [ÉXITO PARQUET] Archivos escritos correctamente.")
    else:
        print("   -> [ALERTA] La carpeta se creó pero quedó VACÍA (Hadoop Committer Error).")
except Exception as e:
    print(f"   -> [FALLO CRÍTICO EN PARQUET]: {e}")

# --- PRUEBA B: CSV VIA SPARK ---
ruta_b = "./proyecto_aml/test_csv"
print("\n   Ejecutando Prueba B (Formato CSV vía Spark)...")
try:
    df.write.mode("overwrite").option("header", "true").csv(ruta_b)
    archivos = os.listdir(ruta_b) if os.path.exists(ruta_b) else []
    print(f"   -> Resultado de archivos en carpeta CSV: {archivos}")
    if any("part-" in f for f in archivos):
        print("   -> [ÉXITO CSV SPARK] Archivos escritos correctamente.")
    else:
        print("   -> [ALERTA] Carpeta CSV vacía.")
except Exception as e:
    print(f"   -> [FALLO CRÍTICO EN CSV SPARK]: {e}")

# --- PRUEBA C: BYPASS COMPLETO A PANDAS LOCAL ---
print("\n   Ejecutando Prueba C (Conversión y persistencia nativa con Pandas)...")
try:
    # Si Spark no puede escribir por culpa de Hadoop, recolectamos a Pandas 
    # y dejamos que el motor nativo de Python maneje el disco de Windows
    df_pandas = df.toPandas()
    ruta_c = "./proyecto_aml/test_pandas.csv"
    df_pandas.to_csv(ruta_c, index=False)
    if os.path.exists(ruta_c) and os.path.getsize(ruta_c) > 0:
        print(f"   -> [ÉXITO PANDAS BYPASS] Archivo guardado y verificado en: {ruta_c}")
    else:
        print("   -> [ALERTA] Archivo de Pandas vacío o no creado.")
except Exception as e:
    print(f"   -> [FALLO CRÍTICO EN BYPASS PANDAS]: {e}")

print("\n================================================================")
print("               DIAGNÓSTICO FINALIZADO                           ")
print("================================================================")
spark.stop()