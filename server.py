import socket, threading, pickle
import requests, json

HEADER = 4096
SERVER = '127.0.0.1'
PORT = 8081
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = '!DISCONNECT'
QUEUE = 5

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def handle_client(connection, addr):
    print(f'[NEW CONNECTION] {addr} connected.')
    client_connected = True
    
    while client_connected:
        content_message = ''
        get_lenght = True
        byte_progress = []
        track_data = []
        car_data = []
        time_data = []
        track_payload = {}
        car_payload = {}
        time_payload = {}
        bytes_recd = 0

        while True:
            if get_lenght:
                chunk = connection.recv(HEADER)

            if get_lenght and chunk.decode(FORMAT) != '':
                MESSAGE_LENGHT = int(chunk[:HEADER])
                get_lenght = False
            else:
                while bytes_recd < MESSAGE_LENGHT:
                    chunk = connection.recv(min(MESSAGE_LENGHT - bytes_recd, HEADER))

                    if chunk == b'':
                        break

                    byte_progress.append(chunk)
                    bytes_recd = bytes_recd + len(chunk)

                content_message = b''.join(byte_progress)
                content_message = pickle.loads(content_message)

                print(content_message)

                if content_message == DISCONNECT_MESSAGE:
                    client_connected = False
                    print(f'[{addr}] Client disconnected')
                    return

                if 'SESSION_INFO' in content_message:
                    upload_data = threading.Thread(target=send_data, args=([content_message]))
                    upload_data.start()

                if 'TRACK_INFO' in content_message:
                    track_data.append(content_message)

                    if len(track_data) == 60:
                        track_payload['data'] = json.dumps(track_data)
                        requests.post('http://127.0.0.1:8000/api-datahandler/upload-track', data=track_payload)

                        track_data = []
                        track_payload = {}

                        print(f'[{addr}] Track Data was send to the backend')

                if 'CAR_INFO' in content_message:
                    car_data.append(content_message)

                    if len(car_data) == 60:
                        car_payload['data'] = json.dumps(car_data)
                        requests.post('http://127.0.0.1:8000/api-datahandler/upload-car', data=car_payload)

                        car_data = []
                        car_payload = {}

                        print(f'[{addr}] Car Data was send to the backend')

                if 'TIME_INFO' in content_message:
                    time_data.append(content_message)

                    if len(time_data) == 60:
                        time_payload['data'] = json.dumps(time_data)
                        requests.post('http://127.0.0.1:8000/api-datahandler/upload-time', data=time_payload)

                        time_data = []
                        time_payload = {}

                        print(f'[{addr}] Time Data was send to the backend')

                get_lenght = True
                byte_progress = []
                content_message = ''
                bytes_recd = 0
                
    connection.close()

def send_data(data):
    payload = {}
    payload['data'] = json.dumps(data)
    requests.post('http://127.0.0.1:8000/api-datahandler/upload-session', data=payload)

def start():
    server.listen(QUEUE)
    print(f'[LISTENNIG] Server is running on {SERVER}/{PORT}')

    while True:
        client_socket, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(client_socket, addr))
        thread.start()

        print(f'[ACTIVE CONNECTIONS] {threading.active_count() - 1}')

print('Server is starting ...')
start()