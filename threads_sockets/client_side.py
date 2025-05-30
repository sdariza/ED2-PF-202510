"""import socket
SERVER = "192.168.1.8"
PORT = 8080
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((SERVER, PORT))
client.sendall(bytes("This is from Client", 'UTF-8'))
while True:
    in_data = client.recv(1024)
    print("From Server :", in_data.decode())
    out_data = input()
    client.sendall(bytes(out_data, 'UTF-8'))
    if out_data == 'bye':
        break
client.close()"""

#intento 1

"""import socket, json
import pandas as pd

HOST, PORT = '127.0.0.1', 8080

def request(alg_name):
    msg = f"RUN:{alg_name}\n" #envía nombre del algoritmo a ejecutar
    #with asegura que el socket se cierre automáticamente después de usarlo
    with socket.socket() as s: #crea el socket cliente
        s.connect((HOST, PORT)) #abre una conección con el servidor
        s.sendall(msg.encode()) #convierte el mensaje en bytes y lo envía
        data = s.recv(4096).decode().strip() #espera la respuesta del servidor
    return json.loads(data) #convierte la respuesta json a diccionario

if __name__ == '__main__':
    for alg in ["quicksort","mergesort","radixsort","shellsort"]:
        res = request(alg) #realiza la solicitud al servidor
        print(res)
        #convierte los tiempos en un dataframe de pandas (una tabla con columnas: formato y tiempos)
        df = pd.DataFrame([
            {"formato": fmt, "tiempo_s": t}
            for fmt,t in res["times"].items()
        ])
        df.to_csv(f"client_tiempos_{alg}.csv", index=False) #guarda el resultado en un csv con el nombre del algoritmo
        print(f"Guardado client_tiempos_{alg}.csv")"""

# client_side.py

import socket
import json
import csv

# Dirección del servidor
HOST = '127.0.0.1'  # o la IP del servidor
PORT = 8080

# Solicita al servidor que ejecute todos los algoritmos en paralelo
# y devuelve un diccionario con los resultados
def request_all():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        # Enviamos la palabra clave 'all' para solicitar todos los algoritmos
        sock.sendall("all\n".encode())

        # Recibimos la respuesta (hasta encontrar un '\n')
        buffer = ""
        while True:
            data = sock.recv(4096)
            if not data:
                break
            buffer += data.decode()
            if "\n" in buffer:
                break

        # Procesamos el JSON recibido
        response_line = buffer.strip()
        response = json.loads(response_line)

        # Verificamos si hubo error en el servidor
        if 'error' in response:
            print(f"[!] Error del servidor: {response['error']}")
            return None

        return response

# Guarda los tiempos en archivos CSV separados, uno por algoritmo
def save_csvs(results):
    for algo, times in results.items():
        filename = f"{algo}_times.csv"
        with open(filename, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Formato", "Tiempo (s)"])
            for fmt, t in times.items():
                writer.writerow([fmt, t])
        print(f"[✓] CSV guardado como '{filename}'")

# Función principal
def main():
    print("[*] Solicitando ejecución de todos los algoritmos...")
    results = request_all()
    if results:
        save_csvs(results)

if __name__ == "__main__":
    main()

