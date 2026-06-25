import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# =========================================================================
# 1. CARGA DE DATOS DESDE EL BYPASS DE PYSPARK
# =========================================================================
ruta_csv = "./proyecto_aml/datos_estructurados_locales.csv"

if not os.path.exists(ruta_csv):
    raise FileNotFoundError(f"No se encontró el archivo {ruta_csv}. Ejecuta primero aml_pyspark_pipeline.py")

print(f"--> [INFO] Cargando datos estructurados desde: {ruta_csv}")
df = pd.read_csv(ruta_csv)

# =========================================================================
# 2. CONSTRUCCIÓN DEL GRAFO DIRIGIDO CON NETWORKX
# =========================================================================
print("--> [INFO] Inicializando grafo dirigido de transacciones...")
# Usamos DiGraph porque el flujo de dinero tiene un origen y un destino claro
G = nx.DiGraph()

# Añadir aristas con atributos embebidos desde el DataFrame
for _, fila in df.iterrows():
    G.add_edge(
        fila['id_origen'], 
        fila['id_destino'], 
        id_transaccion=fila['id_transaccion'],
        monto=float(fila['monto']),
        canal=fila['tipo_canal'],
        timestamp=fila['timestamp_str'],
        sospecha_estructuracion=int(fila['sospecha_estructuracion'])
    )

print(f"--> [OK] Grafo construido: {G.number_of_nodes()} Clientes (Nodos) y {G.number_of_edges()} Transacciones (Aristas).")

# =========================================================================
# 3. ANÁLISIS DE TOPOLOGÍA Y MÉTRICAS DE RED (PLD / AML)
# =========================================================================
print("--> [INFO] Calculando métricas de centralidad para detección de anomalías...")

# In-Degree: Cuentas que reciben transferencias masivas (posibles concentradoras)
in_degree = nx.in_degree_centrality(G)
# Out-Degree: Cuentas que dispersan fondos rápidamente (posibles pitufos/estructuradores)
out_degree = nx.out_degree_centrality(G)
# PageRank: Identifica la relevancia/riesgo de una cuenta basado en sus conexiones estructurales
pagerank = nx.pagerank(G, weight='monto')

# Mapear métricas al DataFrame de nodos para análisis posterior
df_nodos = pd.DataFrame({
    'id_cliente': list(G.nodes()),
    'centralidad_entrada': [in_degree[nodo] for nodo in G.nodes()],
    'centralidad_salida': [out_degree[nodo] for nodo in G.nodes()],
    'score_pagerank': [pagerank[nodo] for nodo in G.nodes()]
})

# Guardar métricas topológicas para el modelo predictivo
ruta_metricas = "./proyecto_aml/metricas_topologicas_grafo.csv"
df_nodos.to_csv(ruta_metricas, index=False)
print(f"--> [OK] Métricas de red exportadas a: {ruta_metricas}")

# =========================================================================
# 4. VISUALIZACIÓN COMPACTA DEL SUBGRAFO DE ALTO RIESGO
# =========================================================================
print("--> [INFO] Generando mapa visual del subgrafo de transacciones sospechosas...")

# Filtrar transacciones marcadas con sospecha de estructuración por el pipeline de Spark
aristas_sospechosas = [
    (u, v) for u, v, attrs in G.edges(data=True) if attrs['sospecha_estructuracion'] == 1
]

if aristas_sospechosas:
    # Crear un subgrafo exclusivamente con las alertas de lavado de dinero
    H = G.edge_subgraph(aristas_sospechosas).copy()
    
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(H, k=0.3, seed=42)
    
    # Dibujar nodos del subgrafo afectado
    nx.draw_networkx_nodes(H, pos, node_size=300, node_color='red', alpha=0.8)
    # Dibujar flechas de flujo de fondos sospechosos
    nx.draw_networkx_edges(H, pos, edgelist=aristas_sospechosas, width=1.5, edge_color='darkred', arrowstyle='->', arrowsize=15)
    # Etiquetas de las cuentas
    nx.draw_networkx_labels(H, pos, font_size=8, font_color='black')
    
    plt.title("Subgrafo de Alertas por Estructuración (Bypass Pipeline)", fontsize=14, fontweight='bold')
    plt.axis('off')
    
    # Guardar visualización en disco
    ruta_grafica = "./proyecto_aml/subgrafo_alertas_aml.png"
    plt.savefig(ruta_grafica, bbox_inches='tight', dpi=150)
    plt.close()
    print(f"--> [OK] Gráfica de red guardada en: {ruta_grafica}")
else:
    print("--> [INFO] No se encontraron transacciones sospechosas con los umbrales actuales para graficar.")

print(f"\n==================================================================")
print(f"--> [OK] ¡MÓDULO DE GRAFOS COMPLETADO EXITOSAMENTE!")
print(f"==================================================================")