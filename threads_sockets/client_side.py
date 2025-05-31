import socket
import json
import csv
import os

HOST = '192.168.18.91'
PORT = 8080

# Solicita export de datos al servidor
def request_export():
    #with asegura que al salir el socket se cierre automáticamente
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        #\n sirve de delimitador para que el servidor sepa que ahí acaba el mensaje enviado
        sock.sendall("export\n".encode()) #envía la primera solicitud
        #lee hasta 4096 bytes de respuesta
        data = sock.recv(4096).decode().strip() #recibe la respuesta del servidor
        #interpresta la cadena recibida como json y lo convierte en un diccionario de python
        resp = json.loads(data)
        if 'export' in resp:
            print("[✓] Export formats created:", resp['export'])
        else:
            raise RuntimeError(resp.get('error', 'Unknown error'))

# Solicita tiempos de ordenamiento de todos los algoritmos
def request_all():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        sock.sendall("all\n".encode()) #envía la segunda solicitud
        buffer = ""
        #acumula las respuestas en la cadena buffer hasta que encuentre un \n
        while True:
            data = sock.recv(4096)
            if not data: break
            buffer += data.decode()
            if "\n" in buffer: break
        return json.loads(buffer.strip())

# Guarda cada algoritmo en CSV con sus tiempos por formato
# Crea carpeta 'results' si no existe
def save_csvs(results):
    output_dir = os.path.join(os.path.dirname(__file__), 'results')
    os.makedirs(output_dir, exist_ok=True)
    for algo, times in results.items():
        fname = os.path.join(output_dir, f"{algo}_times.csv")
        with open(fname, 'w', newline='') as f:
            w = csv.writer(f)
            #En el csv se guardan los datos como: formato, tiempo de lectura, tiempo de orden
            w.writerow(["Formato", "Tiempo Lectura (s)", "Tiempo Orden (s)"])
            for fmt, vals in times.items():
                if 'error' in vals:
                    w.writerow([fmt, vals['error'], ""])
                else:
                    read_t = vals['read_time']
                    sort_t = vals['sort_time']
                    w.writerow([fmt, read_t, sort_t])
        print(f"[✓] {fname} guardado")

# Función para ejecuta múltiples pruebas de ordenamiento y lectura, recopilando los tiempos para calcular promedios
def run_multiple_tests(numReps=50):
    # Diccionario para almacenar los tiempos acumulados de todas las pruebas
    combined_results = {}  

    for i in range(numReps):  # Repite las pruebas n veces (por defecto 50)
        # Solicita al servidor los tiempos de lectura y ordenamiento
        run_results = request_all()  

        # Itera por cada algoritmo (BubbleSort, QuickSort, etc.)
        for algo, formatos in run_results.items():  
            if algo not in combined_results:
                # Inicializa el algoritmo si aún no está registrado
                combined_results[algo] = {}  

            # Itera por cada formato (CSV, JSON, etc.)
            for fmt, datos in formatos.items():  
                if fmt not in combined_results[algo]:
                    # Inicializa listas vacías para almacenar tiempos si es la primera vez que se ve ese formato
                    combined_results[algo][fmt] = {
                        'read_times': [],
                        'sort_times': []
                    }

                if 'error' in datos:
                    # Si hubo un error al procesar ese formato, lo omite
                    continue

                # Agrega los tiempos a las listas correspondientes
                combined_results[algo][fmt]['read_times'].append(datos['read_time'])
                combined_results[algo][fmt]['sort_times'].append(datos['sort_time'])

    # Devuelve todos los tiempos recolectados para calcular promedios
    return combined_results  


# Calcula los promedios de tiempo de lectura y ordenamiento para cada algoritmo y formato
def compute_averages(combined_results):
    averaged_results = {}  # Diccionario donde se almacenarán los promedios

    # Itera por cada algoritmo
    for algo, formatos in combined_results.items():  
        averaged_results[algo] = {}

        # Itera por cada formato
        for fmt, tiempos in formatos.items():  
            # Verifica que haya datos válidos para evitar divisiones por cero
            if tiempos['read_times'] and tiempos['sort_times']:
                # Calcula el promedio redondeado a 6 decimales para los tiempos de lectura y ordenamiento
                avg_read = round(sum(tiempos['read_times']) / len(tiempos['read_times']), 6)
                avg_sort = round(sum(tiempos['sort_times']) / len(tiempos['sort_times']), 6)

                # Guarda los promedios en el nuevo diccionario
                averaged_results[algo][fmt] = {
                    'read_time': avg_read,
                    'sort_time': avg_sort
                }

    # Devuelve los resultados promedio para cada algoritmo y formato
    return averaged_results  


# Código principal que se ejecuta al correr el script
if __name__ == '__main__':
    # Paso 1: export
    print("[*] Exportando archivos csv, json, parquet y feather...")
    request_export()

    print("[*] Ejecutando ordenamientos 50 veces para calcular promedios...")
    # Paso 2: ejecuta las pruebas múltiples veces y recolecta los tiempos
    combined = run_multiple_tests(numReps=50)  

    # Paso 3: calcular los promedios de los tiempos recolectados y guardar CSVs
    averaged = compute_averages(combined)  

    save_csvs(averaged)