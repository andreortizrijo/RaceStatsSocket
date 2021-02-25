import socket, threading, pickle
HEADER = 4096
SERVER = '192.168.1.22'
PORT = 8081
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = '!DISCONNECT'

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)

def send(msg):
    message = pickle.dumps(msg)
    message_buffer = message    

    message = str(len(message)).encode(FORMAT)
    message += b' ' *(HEADER - len(message)) + message_buffer
    client.send(message)


message = '!DISCONNECT'

send(message)