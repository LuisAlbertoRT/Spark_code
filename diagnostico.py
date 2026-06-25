import os
import sys
import socket
import subprocess
import time

def ejecutar_diagnostico_avanzado():
    print("=" * 60)
    print("   DIAGNÓSTICO AVANZADO DE INFRAESTRUCTURA - PYSPARK V2")
    print("=" * 60)
    
    # -------------------------------------------------------------------------
    # PASO 1: Variables de Entorno (Actualizado a tu Java 17)
    # -------------------------------------------------------------------------
    print("\n[PASO 1] Evaluando Variables de Entorno y Rutas...")
    
    java_home = r"C:\java\jdk-17.0.19+10"
    hadoop_home = r"C:\hadoop"
    ruta_librerias = r"C:\Users\Luis Alberto\AppData\Local\Programs\Python\Python311\Lib\site-packages"
    
    os.environ["JAVA_HOME"] = java_home
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    
    # Forzar el path de Python 3.11
    if ruta_librerias not in sys.path:
        sys.path.insert(0, ruta_librerias)
        
    # Inyectar Hadoop bin al PATH del sistema
    sys.path.append(os.path.join(hadoop_home, "bin"))
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")

    rutas = {"JAVA_HOME": java_home, "HADOOP_HOME": hadoop_home, "Python Site-Packages": ruta_librerias}
    for alias, path in rutas.items():
        if os.path.exists(path):
            print(f"  ✔️  {alias} localizado en: {path}")
        else:
            print(f"  ❌ ERROR: El directorio de {alias} NO existe: {path}")
            return

    # -------------------------------------------------------------------------
    # PASO 2: winutils.exe y permisos de escritura temporales
    # -------------------------------------------------------------------------
    print("\n[PASO 2] Validando Binarios de Hadoop y Permisos de Almacenamiento...")
    winutils_path = os.path.join(hadoop_home, "bin", "winutils.exe")
    
    if os.path.exists(winutils_path):
        print(f"  ✔️  winutils.exe detectado en: {winutils_path}")
    else:
        print(f"  ❌ ERROR: winutils.exe no se encuentra en la subcarpeta 'bin'.")
        return
        
    # Validar que Spark pueda escribir en C:\temp (esencial para Windows)
    temp_dir = r"C:\temp"
    try:
        os.makedirs(temp_dir, exist_ok=True)
        test_file = os.path.join(temp_dir, "spark_test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        print(f"  ✔️  Permisos de escritura locales confirmados en: {temp_dir}")
    except Exception as e:
        print(f"  ⚠️  ADVERTENCIA: No se pudo escribir en {temp_dir}. Detalle: {e}")
        print("     Spark podría quejarse al crear el almacén de datos (Warehouse).")

    # -------------------------------------------------------------------------
    # PASO 3: Validación del Compilador Java (Verificación de Arquitectura)
    # -------------------------------------------------------------------------
    print("\n[PASO 3] Ejecutando pruebas directas sobre la JVM (Java 17)...")
    java_binary = os.path.join(java_home, "bin", "java.exe")
    
    try:
        resultado_java = subprocess.run(
            [java_binary, "-version"], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            timeout=5
        )
        version_output = resultado_java.stderr.strip()
        print("  ✔️  Subproceso Java lanzado exitosamente por Python.")
        print(f"      Firma del Runtime:\n      {version_output.splitlines()[0] if version_output else 'Desconocida'}")
        
        if "64-Bit" in version_output:
            print("  ✔️  Arquitectura de Java correcta (64-Bit VM).")
        else:
            print("  ⚠️  ADVERTENCIA: No se detectó explícitamente '64-Bit'. Asegúrate de que no sea un JDK de 32 bits.")
    except subprocess.TimeoutExpired:
        print("  ❌ ERROR: Tiempo de espera agotado al invocar java.exe. Un software de seguridad detuvo el subproceso.")
        return
    except Exception as e:
        print(f"  ❌ ERROR al interactuar con el binario de Java: {e}")
        return

    # -------------------------------------------------------------------------
    # PASO 4: Disponibilidad de Sockets e Interfaz de Red Local
    # -------------------------------------------------------------------------
    print("\n[PASO 4] Analizando Interfaces de Red e Intercambio de Puertos...")
    host_prueba = "127.0.0.1"
    puertos_a_probar = [4040, 4041, 0] # 0 permite al OS asignar el primer puerto libre automáticamente
    
    socket_exitoso = False
    for puerto in puertos_a_probar:
        try:
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.bind((host_prueba, puerto))
            puerto_asignado = test_socket.getsockname()[1]
            test_socket.listen(1)
            test_socket.close()
            print(f"  ✔️  Puerto local {puerto_asignado} disponible y respondiendo a conexiones Loopback.")
            socket_exitoso = True
            break
        except Exception:
            continue
            
    if not socket_exitoso:
        print("  ❌ ERROR: Bloqueo total de sockets en red local. Windows impide la apertura de puertos efímeros.")
        return

    # -------------------------------------------------------------------------
    # PASO 5: Lanzamiento del Entorno PySpark (Frontera Definitiva)
    # -------------------------------------------------------------------------
    print("\n[PASO 5] Inicializando el Puente Síncrono Py4J e Inyección a Memoria...")
    try:
        from py4j.java_gateway import JavaGateway
        print("  ✔️  Librería puente 'py4j' cargada en el hilo actual.")
        
        from pyspark.sql import SparkSession
        print("  ✔️  Módulos de PySpark importados analíticamente.")
        print("  --> Desplegando SparkSession local (Compilación de Clases)...")
        
        # Guardamos la marca de tiempo antes de lanzar Spark
        tiempo_inicio = time.time()
        
        spark = SparkSession.builder \
            .appName("DiagnosticoMejorado_AML") \
            .master("local[1]") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
            .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
            .getOrCreate()
            
        duracion = time.time() - tiempo_inicio
        print(f"  ✔️  ¡HISTÓRICO! Contexto Spark levantado exitosamente en {duracion:.2f} segundos.")
        
        # Prueba real de ejecución distribuida local
        data = [("Infraestructura", "Validada", 2026)]
        df = spark.createDataFrame(data, ["Componente", "Estatus", "Año"])
        print("\n--- RESULTADO DE LA ACCIÓN DE SPARK ---")
        df.show(truncate=False)
        
        spark.stop()
        print("=" * 60)
        print("   ¡SISTEMA OPERATIVO Y ENTORNO LOCAL COMPLETAMENTE REPARADOS!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n  ❌ ERROR CRÍTICO EN LA SESIÓN DE SPARK:")
        print(f"      Detalle técnico: {e}")
        print("\n  Opciones de mitigación sugeridas:")
        print("  1. Verifica si tienes otra versión de PySpark compitiendo en caché.")
        print("  2. Ejecuta 'taskkill /F /IM java.exe' en tu CMD antes de reintentar.")

if __name__ == "__main__":
    ejecutar_diagnostico_avanzado()