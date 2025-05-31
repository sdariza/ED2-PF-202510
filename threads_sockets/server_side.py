import socket
import threading
import json
import time
import pandas as pd
from sql_connection import get_data, get_connection
from export_utils import export_csv, export_json, export_parquet, export_feather
#from sorting_algorithms import quicksort, mergesort, radixsort, shellsort
import sys
import os
import csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from algorithms.mergesort import mergesort_wrapper
from algorithms.quicksort import quicksort_wrapper
from algorithms.radixsort import radixsort_wrapper
from algorithms.shellsort import shellsort_wrapper

HOST = '127.0.0.1' #máquina local donde está el servidor
PORT = 8080

FORMATS = ['csv', 'json', 'feather', 'parquet']
#Cada algoritmo está asociado a su método
ALGORITHMS = {
    'quicksort': quicksort_wrapper,
    'mergesort': mergesort_wrapper,
    'radixsort': radixsort_wrapper,
    'shellsort': shellsort_wrapper
}

# ==== Función de exportación de datos ====  
def export_data_from_db():
    """
    Consulta la base, genera DataFrame y exporta los 4 formatos en ./exports.
    Además crea un CSV con los tamaños de cada archivo.
    """
    # Directorio base y carpeta de exports
    base = os.path.dirname(__file__)
    export_dir = os.path.join(base, 'exports')
    os.makedirs(export_dir, exist_ok=True)

    # Traer datos
    cnx = get_connection()
    data = get_data(cnx, "SELECT * FROM UN.VENTAS")
    df = pd.DataFrame(data, columns=[
        'ID_VENTA', 'FECHA_VENTA', 'ID_CLIENTE', 'ID_EMPLEADO',
        'ID_PRODUCTO', 'CANTIDAD', 'PRECIO_UNITARIO', 'DESCUENTO', 'FORMA_PAGO'
    ])

    # Rutas de archivos
    paths = {
        'csv':     os.path.join(export_dir, 'ventas.csv'),
        'json':    os.path.join(export_dir, 'ventas.json'),
        'parquet': os.path.join(export_dir, 'ventas.parquet'),
        'feather': os.path.join(export_dir, 'ventas.feather')
    }

    # Exportar a cada formato
    export_csv(df,     paths['csv'])
    export_json(df,    paths['json'])
    export_parquet(df, paths['parquet'])
    export_feather(df, paths['feather'])

    # Después de exportar, medir tamaños de cada archivo
    sizes = []  # lista de tuplas (formato, tamaño en bytes)
    for fmt, path in paths.items():
        try:
            size_bytes = os.path.getsize(path)
            sizes.append((fmt, size_bytes))
        except OSError:
            sizes.append((fmt, None))

    # Crear CSV con tamaños en la carpeta exports
    sizes_csv_path = os.path.join(export_dir, 'filesizes.csv')
    with open(sizes_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Formato', 'Tamaño (bytes)'])
        for fmt, sz in sizes:
            writer.writerow([fmt, sz if sz is not None else 'Error'])

    # Devolver estados de exportación
    return [f"{fmt} OK" for fmt in FORMATS]

# ==== Carga datos para sorting (midiendo tiempo de lectura con time.time) ====  
def load_data_with_timing(fmt):
    path = os.path.join(os.path.dirname(__file__), 'exports', f"ventas.{fmt}")
    start_read = time.time()
    if fmt == 'csv':
        df = pd.read_csv(path)
    elif fmt == 'json':
        # Primero intento JSON Lines, y si falla pruebo JSON estándar
        try:
            df = pd.read_json(path, lines=True)
        except ValueError:
            df = pd.read_json(path)
    elif fmt == 'feather':
        df = pd.read_feather(path)
    elif fmt == 'parquet':
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Formato no soportado: {fmt}")
    end_read = time.time()
    read_time = round(end_read - start_read, 15)
    #Retorna la columna cantidad como lista de valores a ordenar por el algoritmo
    return df['CANTIDAD'].tolist(), read_time

# ==== Sorting y medición (lectura + ordenamiento con time.time) ====  
def sort_and_time(fmt, algorithm_func, result_dict):
    try:
        # Mide tiempo de lectura y obtiene datos
        data, read_time = load_data_with_timing(fmt)
        # Mide tiempo de ordenamiento
        start_sort = time.time()
        algorithm_func(data.copy()) # Copia para no modificar el original
        end_sort = time.time()
        sort_time = round(end_sort - start_sort, 15)
        # Guarda ambos tiempos en el diccionario o un string de error si algo falla
        result_dict[fmt] = {
            'read_time': read_time,
            'sort_time': sort_time
        }
    except Exception as e:
        result_dict[fmt] = {'error': str(e)}

# ==== Manejo de cliente ====  
def handle_client(conn, addr):
    print(f"[+] Conexión desde {addr}")
    try:
        while True:
            #decodifica la información del socket
            cmd = conn.recv(1024).decode().strip()
            if not cmd: #si la cadena es vacía, el cliente cerró la conexión y se sale
                break

            if cmd == 'export':
                # Ejecutar export
                statuses = export_data_from_db()
                resp = json.dumps({ 'export': statuses }) + "\n"
                #envía la respuesta al cliente
                conn.sendall(resp.encode())

            elif cmd == 'all':
                # Ejecutar los 4 algoritmos en paralelo
                global_results = {} #diccionario con los 4 algoritmos que a su vez tienen diccionarios con formatos y tiempos
                threads = [] #lista de hilos de los algoritmos
                #nombre algoritmo, función asociada
                #para cada algoritmo crea un hilo y para cada formato en el que se ejecuta ese algoritmo crea un hilo
                for algo, func in ALGORITHMS.items():
                    #es más cómoda tener esta función aquí porque ya hereda los parámetros necesarios
                    def worker(a=algo, f=func):
                        res = {}
                        fmt_threads = [] #lista de hilos de los formatos
                        for fmt in FORMATS:
                            #                                         fmt = format, f = algoritmo, res = diccionario resultante
                            t = threading.Thread(target=sort_and_time, args=(fmt, f, res))
                            t.start(); fmt_threads.append(t)
                        for t in fmt_threads:
                            t.join()
                        global_results[a] = res


                    t = threading.Thread(target=worker)
                    t.start(); threads.append(t)
                for t in threads:
                    t.join()

                resp = json.dumps(global_results) + "\n"
                #envía el diccionario al cliente
                conn.sendall(resp.encode())
                break
            else:
                conn.sendall(json.dumps({'error': 'Comando inválido'}).encode())
                break
    finally:
        conn.close()

# ==== Main server ====
def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen()
        print(f"[🔌] Servidor escuchando en {HOST}:{PORT}...")

        while True:
            conn, addr = server.accept()
            #hilo del cliente
            client_thread = threading.Thread(target=handle_client, args=(conn, addr))
            client_thread.start()

if __name__ == "__main__":
    main()
