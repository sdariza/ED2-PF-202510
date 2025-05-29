import os, pandas as pd
from sql_connection import get_data, get_connection
from export_utils import (
    export_csv, export_json, export_parquet, export_feather
)
#Esto se podría hacer con hilos?

# Consulta a la base de datos
cnx = get_connection()

print("Connection established")

data = get_data(cnx, "SELECT * FROM UN.VENTAS LIMIT 10")


df = pd.DataFrame(data, columns=['ID_VENTA', 'FECHA_VENTA', 'ID_CLIENTE', 'ID_EMPLEADO',
                  'ID_PRODUCTO', 'CANTIDAD', 'PRECIO_UNITARIO', 'DESCUENTO', 'FORMA_PAGO'])

# Crear carpeta 'exports' si no existe
os.makedirs("exports", exist_ok=True)

# Exportar a CSV
export_csv(df, "exports/ventas.csv")

# Exportar a JSON
export_json(df, "exports/ventas.json")

# Exportar a Parquet
export_parquet(df, "exports/ventas.parquet")

# Exportar a Feather
export_feather(df, "exports/ventas.feather")

print("Exportación completada correctamente ✅")
