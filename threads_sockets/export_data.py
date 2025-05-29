import os, pandas as pd
from sql_connection import get_data, get_connection
from export_utils import (
    export_csv, export_json, export_parquet, export_avro
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

#Avro: define el schema manualmente (ajusta según las columnas reales)
avro_schema = {
    "doc": "Ventas exportadas",
    "name": "ventas_record",
    "namespace": "mi.proyecto",
    "type": "record",
    "fields": [
        # Asegúrate de que estos nombres y tipos coincidan con las columnas
        # Falta verificar los tipos de las columnas al realizar las peticiones y los nombres
        {"name": "ID_VENTA", "type": "object"},
        {"name": "FECHA_VENTA", "type": "object"},
        {"name": "ID_CLIENTE", "type": "int64"},
        {"name": "ID_EMPLEADO", "type": "int64"},
        {"name": "ID_PRODUCTO", "type": "int64"},
        {"name": "CANTIDAD", "type": "int64"},
        {"name": "PRECIO_UNITARIO", "type": "object"},
        {"name": "DESCUENTO", "type": "object"},
        {"name": "FORMA_PAGO", "type": "object"}
    ]
}

# Exportar a Avro
export_avro(df, "exports/ventas.avro", avro_schema)

print("Exportación completada correctamente ✅")
