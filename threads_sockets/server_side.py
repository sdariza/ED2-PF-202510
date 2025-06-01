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

HOST = '192.168.18.91' #máquina local donde está el servidor
PORT = 8080

FORMATS = ['csv', 'json', 'feather', 'parquet']
#Cada algoritmo está asociado a su método
ALGORITHMS = {
    'quicksort': quicksort_wrapper,
    'mergesort': mergesort_wrapper,
    'radixsort': radixsort_wrapper,
    'shellsort': shellsort_wrapper
}

# Variables globales para control del servidor
active_clients = 0
clients_lock = threading.Lock()
server_should_stop = False
IDLE_TIMEOUT = 30  # Segundos de espera sin clientes antes de cerrar

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
            size_mb = round(size_bytes / (1024 * 1024), 4)  # Convertir a MB
            sizes.append((fmt,size_mb))
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
    global active_clients
    
    # Incrementar contador de clientes activos
    with clients_lock:
        active_clients += 1
    
    print(f"[+] Cliente conectado desde {addr}. Clientes activos: {active_clients}")
    
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
                            #fmt = format, f = algoritmo, res = diccionario resultante
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
        # Decrementar contador de clientes activos
        with clients_lock:
            active_clients -= 1
        print(f"[-] Cliente {addr} desconectado. Clientes activos: {active_clients}")

def monitor_idle_server():
    """
    Hilo que monitorea si el servidor está inactivo y lo cierra automáticamente.
    
    Esta función se ejecuta en un hilo separado y su trabajo es:
    1. Revisar periódicamente si hay clientes conectados
    2. Si no hay clientes, esperar un tiempo antes de cerrar el servidor
    3. Durante la espera, revisar si llegan nuevos clientes
    4. Si no llegan clientes en el tiempo límite, marcar el servidor para cierre
    """
    global server_should_stop  # Variable global que indica si el servidor debe cerrarse
    
    # Bucle que se ejecuta hasta que se marque el cierre
    while not server_should_stop:
        time.sleep(5)  # Revisar cada 5 segundos si hay actividad
        
        # Usar lock para acceso thread-safe al contador de clientes
        # Esto evita race conditions cuando múltiples hilos leen/escriben active_clients
        with clients_lock:
            current_clients = active_clients  # Copia local del contador
        
        # Si no hay clientes conectados, iniciar proceso de cierre
        if current_clients == 0:
            print(f"[⏰] Servidor sin clientes. Esperando {IDLE_TIMEOUT} segundos antes de cerrar...")

            # Bandera para saber si hubo reconexión durante la espera
            client_reconnected = False


            # Bucle de espera: revisar cada segundo durante IDLE_TIMEOUT segundos
            # Esto permite cancelar el cierre si llega un cliente durante la espera
            for i in range(IDLE_TIMEOUT):
                # Si otro hilo ya marcó el servidor para cierre, salir inmediatamente
                if server_should_stop:
                    return
                
                time.sleep(1)  # Esperar 1 segundo antes de la siguiente revisión
                
                # Revisar si llegó algún cliente durante este segundo
                with clients_lock:
                    if active_clients > 0:
                        print("[✓] Cliente reconectado, cancelando cierre automático")
                        client_reconnected = True
                        break  # Salir del bucle for, cancelar el cierre

            # Si NO hubo reconexión, proceder con el cierre
            if not client_reconnected:
                with clients_lock:
                    if active_clients == 0:
                        print("[🔴] Cerrando servidor por inactividad...")
                        server_should_stop = True
                        return


def main():
    """
    Función principal del servidor que maneja las conexiones de clientes.
    
    Configura el socket del servidor, inicia el hilo monitor de inactividad,
    y maneja el bucle principal de aceptación de conexiones.
    """
    
    # ===== CONFIGURACIÓN DEL HILO MONITOR =====
    # Crear y iniciar el hilo que monitoreará la inactividad del servidor
    monitor_thread = threading.Thread(target=monitor_idle_server, daemon=True)
    # daemon=True significa que este hilo se cerrará automáticamente cuando termine main()
    monitor_thread.start()
    
    # ===== CONFIGURACIÓN DEL SOCKET DEL SERVIDOR =====
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        # SO_REUSEADDR permite reusar la dirección inmediatamente después de cerrar
        # Esto evita el error "Address already in use" al reiniciar el servidor
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Timeout de 1 segundo para accept() - esto permite que el bucle principal
        # revise periódicamente si server_should_stop cambió a True
        # Sin timeout, accept() bloquearía indefinidamente esperando conexiones
        server.settimeout(1.0)
        
        # Vincular el socket al HOST y PORT definidos
        server.bind((HOST, PORT))
        # Poner el socket en modo escucha para conexiones entrantes
        server.listen()
        
        print(f"[🔌] Servidor escuchando en {HOST}:{PORT}...")
        print(f"[ℹ️] El servidor se cerrará automáticamente tras {IDLE_TIMEOUT}s sin clientes")

        # ===== BUCLE PRINCIPAL DE ACEPTACIÓN DE CONEXIONES =====
        while not server_should_stop:  # Continuar hasta que se marque el cierre
            try:
                # Intentar aceptar una nueva conexión
                # Si no hay conexiones en 1 segundo, lanza socket.timeout
                conn, addr = server.accept()
                
                # Crear un nuevo hilo para manejar este cliente específico
                # Esto permite manejar múltiples clientes simultáneamente
                client_thread = threading.Thread(target=handle_client, args=(conn, addr))
                client_thread.start()
                
            except socket.timeout:
                # Timeout normal después de 1 segundo sin conexiones
                # No es un error - simplemente continuar el bucle
                # Esto permite revisar si server_should_stop cambió
                continue
                
            except OSError:
                # Error en el socket - probablemente porque se está cerrando
                # o hay algún problema de red. Salir del bucle
                break
    
    # ===== CIERRE LIMPIO =====
    print("[✅] Servidor cerrado correctamente")


# ===== PUNTO DE ENTRADA DEL PROGRAMA =====
if __name__ == "__main__":
    try:
        main()  # Ejecutar la función principal
    except KeyboardInterrupt:
        # Manejar Ctrl+C del usuario
        print("\n[⚠️] Cerrando servidor por interrupción del usuario...")
        server_should_stop = True  # Marcar para cierre inmediato