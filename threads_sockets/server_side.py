import socket
import threading
import json
import time
import os
import pandas as pd
from sql_connection import get_data, get_connection
from export_utils import export_csv, export_json, export_parquet, export_feather
from sorting_algorithms import quicksort, mergesort, radixsort, shellsort

HOST = '127.0.0.1' #máquina local donde está el servidor
PORT = 8080

FORMATS = ['csv', 'json', 'feather', 'parquet']
#Cada algoritmo está asociado a su método
ALGORITHMS = {
    'quicksort': quicksort,
    'mergesort': mergesort,
    'radixsort': radixsort,
    'shellsort': shellsort
}

# ==== Función de exportación de datos ====  
def export_data_from_db():
    """
    Consulta la base, genera DataFrame y exporta los 4 formatos en ./exports.
    """
    #los formatos se pondrán en una carpeta llamada exports dentro de threads_sockets
    base = os.path.dirname(__file__)
    export_dir = os.path.join(base, 'exports')
    os.makedirs(export_dir, exist_ok=True)

    #se crea la conección a la base de datos y se obtienen todos los registros de la tabla ventas
    cnx = get_connection()
    data = get_data(cnx, "SELECT * FROM UN.VENTAS")
    #construye un dataframe con las columnas indicadas
    df = pd.DataFrame(data, columns=[
        'ID_VENTA', 'FECHA_VENTA', 'ID_CLIENTE', 'ID_EMPLEADO',
        'ID_PRODUCTO', 'CANTIDAD', 'PRECIO_UNITARIO', 'DESCUENTO', 'FORMA_PAGO'
    ])

    #define las rutas donde estarán los archivos
    paths = {
        'csv':os.path.join(export_dir, 'ventas.csv'),
        'json':os.path.join(export_dir, 'ventas.json'),
        'parquet':os.path.join(export_dir, 'ventas.parquet'),
        'feather':os.path.join(export_dir, 'ventas.feather')
    }

    #se utilizan las funciones que están en export_utils
    export_csv(df,paths['csv'])
    export_json(df,paths['json'])
    export_parquet(df,paths['parquet'])
    export_feather(df,paths['feather'])

    #devuelve un listado de strings para informar al cliente que el proceso fue exitoso
    #"csv OK", "json OK"...
    return [f"{fmt} OK" for fmt in FORMATS]

# ==== Carga datos para sorting ====  
def load_data(fmt):
    #construye la ruta del archivo a leer
    path = os.path.join(os.path.dirname(__file__), 'exports', f"ventas.{fmt}")
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
    #Retorna la columna cantidad como lista de valores a ordenar por el algoritmo
    return df['CANTIDAD'].tolist()

# ==== Ordenar y medir tiempo ====
def sort_and_time(algorithm_func, format_name, result_dict):
    try:
        data = load_data(format_name)
        #comienza a medir el tiempo
        start = time.time()
        #ejecuta el algoritmo dado
        algorithm_func(data.copy())  # Copia para no modificar el original
        #termina de medir el tiempo
        end = time.time()
        #guarda en un diccionario el tiempo obtenido y string de error si algo falla
        result_dict[format_name] = round(end - start, 15)
    except Exception as e:
        result_dict[format_name] = f"Error: {str(e)}"

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
                            #                                         f = algoritmo, fmt = format, res = diccionario resultante
                            t = threading.Thread(target=sort_and_time, args=(f, fmt, res))
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
