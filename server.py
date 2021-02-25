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
    client_connection = True
    
    while client_connection:
        header_verify = True
        content_message = b''
        data = []

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
                print(content_message)
                if content_message == DISCONNECT_MESSAGE:
                    client_connection = False
                    print(f'[{addr}] Client disconnected')
                    print(f'[{addr}] {content_message}')
                else:
                    data += content_message

                    if len(data) == 60*3:
                        payload = json.dumps(data)
                        #r = requests.post('http://192.168.1.22:8080/datahandler/senddata', data=payload)
                        #r.status_code

                        data = []
                        print(f'[{addr}] Data was send to the backend')

                header_verify = True
                content_message = b''

    conn.close()

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