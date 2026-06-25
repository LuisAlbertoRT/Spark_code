import os
import sys
import pandas as pd
import networkx as nx

print("--> [INFO] Iniciando Módulo de Análisis de Grafos...")

# 1. Rutas locales de tus datos procesados por Spark
ruta_parquet = "./proyecto_aml/datos_estructurados.parquet"

if not os.path.exists(ruta_parquet):
    print(f"❌ ERROR: No se encontraron los datos de Spark en {ruta_parquet}.")
    print("Asegúrate de haber ejecutado el script de PySpark primero.")
    sys.exit()

# 2. Cargar los datos a Pandas para el análisis de red local
# Spark guardó un archivo optimizado; lo leemos directamente
df_transacciones = pd.read_parquet(ruta_parquet)

print(f"--> [INFO] Registros cargados para el grafo: {len(df_transacciones)}")

# 3. Construir el Grafo Dirigido (Origen -> Destino)
# En prevención de lavado de dinero, la dirección del dinero lo es todo
G = nx.from_pandas_edgelist(
    df_transacciones.head(50000), # Procesamos un subconjunto denso para optimizar memoria local
    source='id_origen',
    target='id_destino',
    edge_attr=['monto', 'tipo_canal'],
    create_using=nx.DiGraph()
)

print(f"--> [INFO] Grafo construido con {G.number_of_nodes()} nodos (clientes) y {G.number_of_edges()} bordes (transferencias).")

# 4. Calcular Métricas de Centralidad Topológica
print("--> [INFO] Calculando métricas de conectividad estructural (Degree Centrality)...")

# Grado de salida (Out-Degree): Cuántas transferencias envía un cliente. 
# Un valor inusualmente alto indica un posible dispersor de fondos.
out_degree_dict = nx.out_degree_centrality(G)

# Grado de entrada (In-Degree): Cuántas transferencias recibe un cliente.
# Un valor alto indica un concentrador de fondos.
in_degree_dict = nx.in_degree_centrality(G)

# 5. Mapear las métricas de grafos de regreso al DataFrame original
df_transacciones['grafo_out_centrality'] = df_transacciones['id_origen'].map(out_degree_dict).fillna(0)
df_transacciones['grafo_in_centrality'] = df_transacciones['id_destino'].map(in_degree_dict).fillna(0)

# 6. Identificar anomalías por estructura de red
print("\n=== DETECCIÓN DE CLIENTES CON COMPORTAMIENTO ANÓMALO EN RED ===")
df_analisis_red = df_transacciones.sort_values(by='grafo_out_centrality', ascending=False)
print(df_analisis_red[['id_origen', 'id_destino', 'monto', 'grafo_out_centrality', 'sospecha_estructuracion']].head(10))

# 7. Guardar el Dataset Enriquecido con variables de Spark + Grafos
ruta_salida_enriquecida = "./proyecto_aml/datos_enriquecidos_modelo.csv"
df_transacciones.to_csv(ruta_salida_enriquecida, index=False)
print(f"\n--> [INFO] Master Dataset listo para Modelado Predictivo guardado en: {ruta_salida_enriquecida}")