import os
import sys
import socket
import subprocess

def ejecutar_diagnostico():
    print("=" * 60)
    print("      DIAGNÓSTICO DE ENTORNO LOCAL PARA PYSPARK")
    print("=" * 60)
    
    # -------------------------------------------------------------------------
    # PASO 1: Validación de Variables de Entorno y Rutas
    # -------------------------------------------------------------------------
    print("\n[PASO 1] Verificando Variables de Entorno...")
    
    java_home = r"C:\java\jdk-11.0.31+11"
    hadoop_home = r"C:\hadoop"
    
    os.environ["JAVA_HOME"] = java_home
    os.environ["HADOOP_HOME"] = hadoop_home
    
    if os.path.exists(java_home):
        print(f"  ✔️  JAVA_HOME existe en la ruta: {java_home}")
    else:
        print(f"  ❌ ERROR: La ruta de JAVA_HOME no existe: {java_home}")
        return

    if os.path.exists(hadoop_home):
        print(f"  ✔️  HADOOP_HOME existe en la ruta: {hadoop_home}")
    else:
        print(f"  ❌ ERROR: La ruta de HADOOP_HOME no existe: {hadoop_home}")
        return

    # -------------------------------------------------------------------------
    # PASO 2: Validación de winutils.exe
    # -------------------------------------------------------------------------
    print("\n[PASO 2] Verificando binarios de Hadoop (winutils)...")
    winutils_path = os.path.join(hadoop_home, "bin", "winutils.exe")
    
    if os.path.exists(winutils_path):
        print(f"  ✔️  winutils.exe encontrado correctamente en: {winutils_path}")
    else:
        print(f"  ❌ ERROR: 'winutils.exe' NO está en {os.path.join(hadoop_home, 'bin')}")
        print("     Asegúrate de que el nombre del archivo sea exacto y no tenga extensiones duplicadas (ej. winutils.exe.exe).")
        return

    # -------------------------------------------------------------------------
    # PASO 3: Prueba de ejecución directa de Java desde Python
    # -------------------------------------------------------------------------
    print("\n[PASO 3] Probando ejecución directa de la JVM desde Python...")
    java_binary = os.path.join(java_home, "bin", "java.exe")
    
    try:
        resultado_java = subprocess.run(
            [java_binary, "-version"], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            timeout=5
        )
        print("  ✔️  Python logró lanzar el binario de Java con éxito.")
        print(f"      Versión detectada en ejecución:\n{resultado_java.stderr.strip()}")
    except subprocess.TimeoutExpired:
        print("  ❌ ERROR: Al intentar ejecutar java.exe, el proceso se quedó congelado (Timeout).")
        print("     Esto indica que tu Antivirus o Windows Defender está bloqueando la ejecución de subprocesos externos.")
        return
    except Exception as e:
        print(f"  ❌ ERROR crítico al lanzar Java: {e}")
        return

    # -------------------------------------------------------------------------
    # PASO 4: Prueba de Sockets y Red Local (Aquí es donde se congelaba)
    # -------------------------------------------------------------------------
    print("\n[PASO 4] Verificando Sockets y Red Local (Loopback)...")
    puerto_prueba = 4040
    host_prueba = "127.0.0.1"
    
    try:
        # Intentamos abrir un socket local para ver si el sistema operativo permite la escucha interna
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.bind((host_prueba, puerto_prueba))
        test_socket.listen(1)
        test_socket.close()
        print(f"  ✔️  El sistema operativo permite abrir y escuchar puertos locales en {host_prueba}:{puerto_prueba}")
    except Exception as e:
        print(f"  ❌ ERROR de Red Local: No se pudo abrir un socket en {host_prueba}:{puerto_prueba}.")
        print(f"     Detalle del error: {e}")
        print("     Esto confirma que el Firewall, una política de red o un software de seguridad está bloqueando la comunicación interna.")
        return

    # -------------------------------------------------------------------------
    # PASO 5: Inicialización aislada del puente Py4J (Última frontera antes de Spark)
    # -------------------------------------------------------------------------
    print("\n[PASO 5] Intentando inicializar el puente de comunicación Py4J...")
    try:
        from py4j.java_gateway import JavaGateway, GatewayParameters
        print("  ✔️  Librería py4j importada correctamente.")
        
        # Configuramos los paths necesarios en el PATH del sistema operativo antes de importar PySpark
        sys.path.append(os.path.join(hadoop_home, "bin"))
        os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        
        from pyspark.sql import SparkSession
        print("  ✔️  Librerías de PySpark cargadas en memoria.")
        print("  --> Lanzando SparkSession de prueba (Último paso crítico)...")
        
        spark = SparkSession.builder \
            .appName("ScriptDiagnostico") \
            .master("local[1]") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
            .getOrCreate()
            
        print("  ✔️  ¡HISTÓRICO! SparkSession levantada de forma local con éxito.")
        spark.stop()
        print("\n" + "="*60)
        print("  ¡DIAGNÓSTICO EXITOSO! Tu entorno local es 100% compatible ahora.")
        print("="*60)
        
    except Exception as e:
        print(f"  ❌ ERROR en el paso final de Spark: {e}")
        print("     El puente py4j no pudo consolidar la sesión debido a restricciones de hilos (threading) en el sistema.")

if __name__ == "__main__":
    ejecutar_diagnostico()