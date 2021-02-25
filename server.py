import socket, threading, pickle
import requests, json

HEADER = 4096
PORT = 8081
SERVER = '192.168.1.22'
ADDR = (SERVER, PORT)
QUEUE = 5
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = '!DISCONNECT'

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def handle_client(connection, addr):
    print(f'[NEW CONNECTION] {addr} connected.')
    client_connected = True
    
    while client_connected:
        header_verify = True
        content_message = b''
        data = []
        payload = {}

        while True:
            received_message = connection.recv(HEADER)

            if header_verify == True and received_message.decode(FORMAT) != '':
                received_message_length = int(received_message[:HEADER])
                header_verify = False
            else:
                header_verify = False

            content_message += received_message

            if len(content_message) - HEADER == received_message_length:
                content_message = pickle.loads(content_message[HEADER:])

                if content_message == DISCONNECT_MESSAGE:
                    client_connected = False
                    print(f'[{addr}] Client disconnected')
                    print(f'[{addr}] {content_message}')
                    break
                else:
                    data += content_message

                    if len(data) == 60*3:
                        data = json.dumps(data)
                        payload['data'] = data

                        r = requests.post('http://192.168.1.22:8080/datahandler/senddata', data=payload)

                        data = []
                        payload = {}
                        print(f'[{addr}] Data was send to the backend')

                header_verify = True
                content_message = b''
    print(client_connected)
    connection.close()

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