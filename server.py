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
        chunks = []
        bytes_recd = 0

        while True:
            if get_lenght:
                chunk = connection.recv(HEADER)

            if get_lenght == True and chunk.decode(FORMAT) != '':
                MESSAGE_LENGHT = int(chunk[:HEADER])
                get_lenght = False
            else:
                while bytes_recd < MESSAGE_LENGHT:
                    chunk = connection.recv(min(MESSAGE_LENGHT - bytes_recd, HEADER))

                    if chunk == b'':
                        break

                    chunks.append(chunk)
                    bytes_recd = bytes_recd + len(chunk)

                content_message = b''.join(chunks)
                content_message = pickle.loads(content_message)

                if content_message == DISCONNECT_MESSAGE:
                    client_connected = False
                    print(f'[{addr}] Client disconnected')
                    return

                if 'SESSION_INFO' in content_message[0]:
                    upload_data = threading.Thread(target=send_data, args=([content_message]))
                    upload_data.start()
                else:

                    upload_data = threading.Thread(target=send_data, args=([content_message]))
                    upload_data.start()

                    print(f'[{addr}] Data was send to the backend')

                get_lenght = True
                chunks = []
                content_message = ''
                bytes_recd = 0
                
    connection.close()

def send_data(data):
    payload = {}
    payload['data'] = json.dumps(data)
    requests.post('http://127.0.0.1:8000/api-datahandler/upload', data=payload)

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