"""import socket
import threading


class ClientThread(threading.Thread):
    def __init__(self, clientAddress, clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        print("New connection added: ", clientAddress)

    def run(self):
        print("Connection from : ", clientAddress)
        msg = ''
        while True:
            data = self.csocket.recv(2048)
            msg = data.decode()
            if msg == 'bye':
                break
            print("from client", msg)
            self.csocket.send(bytes(msg, 'UTF-8'))
        print("Client at ", clientAddress, " disconnected")


LOCALHOST = "192.168.1.4"
PORT = 8080
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((LOCALHOST, PORT))
print("Server started")
print("Waiting for client request..")
while True:
    server.listen(3)
    clientsock, clientAddress = server.accept()
    newthread = ClientThread(clientAddress, clientsock)
    newthread.start()
"""
#INTENTO 1
"""
import socket, threading, json
from orchestrator import sort_for_algorithm  # función que lleva a cabo los hilos y devuelve el dict

HOST, PORT = '127.0.0.1', 8080

def handle_client(conn, addr):
    #lee hasta 2048 bits enviados por el cliente
    #.decode() convierte bytes a string y .strip() quita espacios y saltos de línea
    data = conn.recv(2048).decode().strip()  # e.g. "RUN:quicksort"
    if data.startswith("RUN:"):
        #extrae el nombre del algoritmo
        alg = data.split(":",1)[1]
        # Ejecuta los hilos de ordenamiento y recoge resultados
        result = sort_for_algorithm(alg)  # → {'algorithm': alg, 'times': {...}} devuelve el diccionario con la información
        #devuelve el resultado al cliente y añade un \n como delimitador
        resp = json.dumps(result) + "\n"
        conn.sendall(resp.encode())
    conn.close()

with socket.socket() as s: #crea el socket del servidor
    s.bind((HOST, PORT)) # asocia el socket a un host y a un puerto
    s.listen() #se pone en modo escucha
    print(f"Servidor escuchando en {HOST}:{PORT}")
    while True:
        #conn es el socket del cliente
        conn, addr = s.accept() #espera a que un cliente se conecte
        threading.Thread(target=handle_client, args=(conn,addr)).start() #crea un hilo para ese cliente y lo empieza
        print("HILO CLIENTE CREADO SUCCESSFULLY")"""

#INTENTO 2
# server_side.py
import socket
import threading
import json
import time
import os
import pandas as pd

from sorting_algorithms import quicksort, mergesort, radixsort, shellsort

HOST = '127.0.0.1'   # escúchalo en todas las interfaces
PORT = 8080

FORMATS = ['csv', 'json', 'feather', 'parquet']
ALGORITHMS = {
    'quicksort': quicksort,
    'mergesort': mergesort,
    'radixsort': radixsort,
    'shellsort': shellsort
}

# ==== Cargar datos según formato ====
def load_data(format):
    path = f"exports/ventas.{format}"
    if format == 'csv':
        df = pd.read_csv(path)
    elif format == 'json':
        df = pd.read_json(path)
    elif format == 'feather':
        df = pd.read_feather(path)
    elif format == 'parquet':
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Formato no soportado: {format}")
    return df['CANTIDAD'].tolist()

# ==== Ordenar y medir tiempo ====
def sort_and_time(algorithm_func, format_name, result_dict):
    try:
        data = load_data(format_name)
        start = time.time()
        algorithm_func(data.copy())  # Copia para no modificar el original
        end = time.time()
        result_dict[format_name] = round(end - start, 15)
    except Exception as e:
        result_dict[format_name] = f"Error: {str(e)}"

# ==== Hilo por algoritmo ====
def handle_algorithm(algorithm_name, global_results):
    results = {}
    threads = []

    for fmt in FORMATS:
        t = threading.Thread(target=sort_and_time, args=(ALGORITHMS[algorithm_name], fmt, results))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    global_results[algorithm_name] = results

# ==== Hilo por cliente ====
def handle_client(conn, addr):
    print(f"[+] Conexión establecida desde {addr}")
    try:
        data = conn.recv(1024).decode().strip()
        if data != "all":
            response = json.dumps({"error": "Solicitud inválida. Usa 'all'"}) + "\n"
            conn.sendall(response.encode())
            return

        global_results = {}
        algo_threads = []

        # Ejecutar los 4 algoritmos en paralelo
        for algo in ALGORITHMS.keys():
            t = threading.Thread(target=handle_algorithm, args=(algo, global_results))
            algo_threads.append(t)
            t.start()

        for t in algo_threads:
            t.join()

        # Enviar resultados al cliente
        response = json.dumps(global_results) + "\n"
        conn.sendall(response.encode())
        print(f"[✓] Resultados enviados a {addr}")

    except Exception as e:
        error_response = json.dumps({"error": str(e)}) + "\n"
        conn.sendall(error_response.encode())
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
            client_thread = threading.Thread(target=handle_client, args=(conn, addr))
            client_thread.start()

if __name__ == "__main__":
    main()
