import errno
import json
import os
import socket
import sys
import threading
import time
import queue


def connect():
    #setting up the connection from server to other servers
    if process_id == 1:
        server1.connect((socket.gethostname(), data["2"]))
        server2.connect((socket.gethostname(), data["3"]))
        server3.connect((socket.gethostname(), data["4"]))
        server4.connect((socket.gethostname(), data["5"]))

    elif process_id == 2:
        server1.connect((socket.gethostname(), data["1"]))
        server2.connect((socket.gethostname(), data["3"]))
        server3.connect((socket.gethostname(), data["4"]))
        server4.connect((socket.gethostname(), data["5"]))

    elif process_id == 3:
        server1.connect((socket.gethostname(), data["1"]))
        server2.connect((socket.gethostname(), data["2"]))
        server3.connect((socket.gethostname(), data["4"]))
        server4.connect((socket.gethostname(), data["5"]))

    elif process_id == 4:
        server1.connect((socket.gethostname(), data["1"]))
        server2.connect((socket.gethostname(), data["2"]))
        server3.connect((socket.gethostname(), data["3"]))
        server4.connect((socket.gethostname(), data["5"]))

    elif process_id == 5:
        server1.connect((socket.gethostname(), data["1"]))
        server2.connect((socket.gethostname(), data["2"]))
        server3.connect((socket.gethostname(), data["3"]))
        server4.connect((socket.gethostname(), data["4"]))

    print("connected to servers!")


def send_between_servers():
    #given the process_id of the current server, you can determine which server to send to by comparing process_id
    #if current process_id is 3, and destination is 2, you would do a server2 send
    #if current process_id is 3, and destination is 4, you would do a server3 send (subtract 1)
    '''
    if (which_server[-1] < str(process_id)):
        if (which_server == 'server1'):
        	server1.sendall(message)
        elif (which_server == 'server2'):
        	server2.sendall(message)
        elif (which_server == 'server3'):
        	server3.sendall(message)
        elif (which_server == 'server4'):
        	server4.sendall(message)
        elif (which_server == 'server5'):
        	server5.sendall(message)
    elif (which_server[-1] > str(process_id)):
        if (which_server == 'server2'):
        	server1.sendall(message)
        elif (which_server == 'server3'):
        	server2.sendall(message)
        elif (which_server == 'server4'):
        	server3.sendall(message)
        elif (which_server == 'server5'):
        	server4.sendall(message)
    '''
    pass

def cmd_input():
    while True:
        try:

            #if input is connect, connect to other servers
            inp = input()
            if inp == 'connect':
                print("server" + str(process_id) + " connecting to servers...")
                connect()

            #if input is broadcast, send to other servers
            elif inp[0:9] == 'broadcast':
                message = inp[10:]
                print("broadcasting message: " + message + ", from server " + str(process_id) + " to all servers")
                message = "server" + str(process_id) + " " + message
                message = message.encode()
                time.sleep(5)
                server1.sendall(message)
                server2.sendall(message)
                server3.sendall(message)
                server4.sendall(message)

            #if input is send, send from one server to another
            elif inp[0:4] == 'send':
                which_server = inp[5:12]
                message = inp[13:]
                print("sending message: " + message + ", from client " + str(process_id) + " to " + which_server)
                message = "client" + str(process_id) + " " + message
                message = message.encode()
                time.sleep(5)
                send_between_servers()

            #if input is exit, close everything
            elif inp == 'exit':
                print("closing all connections...")
                self_socket.close()
                server1.close()
                server2.close()
                server3.close()
                server4.close()
                os._exit(0)
        except EOFError:
            pass

def server_listen(stream, addr):
    while True:
        #listen for a request from other clients
        message = stream.recv(1024)
        if not message:
            break
        message = message.decode()
        sender = message[0:7]
        message = message[8:]
        #send received message from client
        print("message: " + message + ", received from " + sender)


process_id = int(sys.argv[1])

file = open('config.json')
data = json.load(file)
PORT = data[str(process_id)]

q = queue.Queue()
q.put(["get", "a_id"])
key_value = {"key":"value"}
blockchain =    [
                [["get", "a_id"], "hash", "nonce"],
                [["put", "a_id", {"phone_number":"111-222-3333"}], "hash", "nonce"]
                ]

self_socket = socket.socket()
self_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
self_socket.bind((socket.gethostname(), PORT))
self_socket.listen(32)

server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
threading.Thread(target=cmd_input).start()

while True:
    stream, addr = self_socket.accept()
    t = threading.Thread(target=server_listen, args=(stream, addr))
    t.start()
