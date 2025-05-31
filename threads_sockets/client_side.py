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

if __name__ == '__main__':
    # Paso 1: export
    print("[*] Solicitando archivos csv, json, parquet, feather...")
    request_export()
    # Paso 2: ordenar y guardar CSVs
    print("[*] Solicitando tiempos de ordenamiento...")
    all_times = request_all()
    save_csvs(all_times)

