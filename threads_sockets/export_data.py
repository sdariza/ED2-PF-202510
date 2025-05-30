import os
import pandas as pd
from sql_connection import get_data, get_connection
from export_utils import export_csv, export_json, export_parquet, export_feather

# Definimos un único directorio de salida dentro de threads_sockets/exports
BASE_DIR = os.path.dirname(__file__)               # carpeta threads_sockets
EXPORT_DIR = os.path.join(BASE_DIR, "exports")     # threads_sockets/exports

# Consulta a la base de datos
cnx = get_connection()

print("Connection established")

data = get_data(cnx, "SELECT * FROM UN.VENTAS")
df = pd.DataFrame(
    data,
    columns=[
        'ID_VENTA', 'FECHA_VENTA', 'ID_CLIENTE', 'ID_EMPLEADO',
        'ID_PRODUCTO', 'CANTIDAD', 'PRECIO_UNITARIO', 'DESCUENTO', 'FORMA_PAGO'
    ]
)

# Crear carpeta 'threads_sockets/exports' si no existe
os.makedirs(EXPORT_DIR, exist_ok=True)

# Rutas de destino
csv_path     = os.path.join(EXPORT_DIR, "ventas.csv")
json_path    = os.path.join(EXPORT_DIR, "ventas.json")
parquet_path = os.path.join(EXPORT_DIR, "ventas.parquet")
feather_path = os.path.join(EXPORT_DIR, "ventas.feather")

# Exportar
export_csv(df,csv_path)
export_json(df,json_path)
export_parquet(df,parquet_path)
export_feather(df,feather_path)

print(f"Exportación completada correctamente ✅\nArchivos en: {EXPORT_DIR}")

