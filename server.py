import errno
import json
import os
import socket
import sys
import threading
import time
import queue
import random
import string
import hashlib


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

    active_networks[server1] = True
    active_networks[server2] = True
    active_networks[server3] = True
    active_networks[server4] = True

    print("connected to servers!")

def get_correct_server(dest_server):
    #given the process_id of the current server, you can determine which server to send to by comparing process_id
    #if current process_id is 3, and destination is 2, you would do a server2 send
    #if current process_id is 3, and destination is 4, you would do a server3 send (subtract 1)
    if (dest_server[-1] < str(process_id)):
        if (dest_server == 'server1'):
            return server1
        elif (dest_server == 'server2'):
            return server2
        elif (dest_server == 'server3'):
            return server3
        elif (dest_server == 'server4'):
            return server4
        elif (dest_server == 'server5'):
            return server5
    elif (dest_server[-1] > str(process_id)):
        if (dest_server == 'server2'):
            return server1
        elif (dest_server == 'server3'):
            return server2
        elif (dest_server == 'server4'):
            return server3
        elif (dest_server == 'server5'):
            return server4

def send_between_servers(dest_server, msg):
    server = get_correct_server(dest_server)
    server.sendall(message)
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
                dest_server = inp[5:12]
                message = inp[13:]
                print("sending message: " + message + ", from client " + str(process_id) + " to " + dest_server)
                message = "client" + str(process_id) + " " + message
                message = message.encode()
                time.sleep(5)
                send_between_servers(dest_server, message)

            elif inp == 'print blockchain':
                print_blockchain()

            elif inp == 'print kvstore':
                print_kv_store()

            elif inp == 'print queue':
                print_queue()

            #this one isn't necessary for project but this is here to help us if we need to debug
            elif inp == 'print active networks':
                print_active_networks()

            elif inp[:9] == 'fail link':
                server = get_correct_server(inp[10:])
                failLink(server)

            elif inp[:8] == 'fix link':
                server = get_correct_server(inp[9:])
                fixLink(server)

            #if input is fail process, close everything
            elif inp == 'fail process':
                failProcess()

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

#generate hashes and nonces for each block
def generateBlockchain():
    letters = string.ascii_letters
    for i in range(len(blockchain)):
        current_hash = ""
        if i != 0:
            previous_operation = blockchain[i-1][0][0]
            previous_hash = blockchain[i-1][1]
            previous_nonce = blockchain[i-1][2]
            operation_nonce_hash = (previous_operation+previous_nonce+previous_hash).encode()
            current_hash = hashlib.sha256(operation_nonce_hash).hexdigest()

        h = ""
        current_operation = blockchain[i][0][0]
        while len(h) == 0 or (h[len(h)-1] != '0' and h[len(h)-1] != '2'):
            nonce = ''.join(random.choice(letters) for i in range(6))
            operation_nonce = (current_operation+nonce).encode()
            h = hashlib.sha256(operation_nonce).hexdigest()

        print("\n")
        print("hash for previous ptr in blockchain: ", current_hash)
        print("nonce: ", nonce)
        print("hashes generated with nonce that should end in 0 or 2: ", h)
        
        blockchain[i][1] = current_hash
        blockchain[i][2] = nonce
        

def write_blockchain_to_file(filename):
    f = open(filename, "w")
    for i in range(len(blockchain)):
        f.write("operation:" + blockchain[i][0][0]+"\n")
        f.write("key:" + blockchain[i][0][1]+"\n")
        if len(blockchain[i][0]) > 2:
            for key, value in blockchain[i][0][2].items():
                f.write("value_k:" + key +"\n")
                f.write("value_v:" + value +"\n")
        f.write("hash:" + blockchain[i][1]+"\n")
        f.write("nonce:" + blockchain[i][2]+"\n")
    f.close()

def read_blockchain_from_file(filename):
    global blockchain
    blockchain = []
    f = open(filename, "r")
    while True:
        block = [[], "", ""]
        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "operation":
            print("error with parsing operation")
        block[0].append(line[line.find(':')+1:])

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "key":
            print("error with parsing key")
        block[0].append(line[line.find(':')+1:])

        l = f.tell()
        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "value_k":
            f.seek(l)
        else:
            key = line[line.find(':')+1:]
            value = ""
            line = f.readline().rstrip('\n')
            if not line:
                break
            if line[:line.find(':')] == "value_v":
                value = line[line.find(':')+1:]
            kv = {key : value}
            block[0].append(kv)

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "hash":
            print("error with parsing hash")
        block[1] = line[line.find(':')+1:]

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "nonce":
            print("error with parsing nonce")
        block[2] = line[line.find(':')+1:]
        blockchain.append(block)
        
def generate_kv_store():
    global kvStore
    for i in range(len(blockchain)):
        val = {}
        if blockchain[i][0][0] == "put":
            for key, value in blockchain[i][0][2].items():
                val[key] = value
            kvStore[blockchain[i][0][1]] = val

def recover_data():
    read_blockchain_from_file()
    generate_kv_store()

def print_blockchain():
    for i in range(len(blockchain)):
        print(blockchain[i])

def print_kv_store():
    print(kvStore)

def print_queue():
    print(list(q.queue))

def print_active_networks():
    print(active_networks)

def failLink(dest_sock):
    if dest_sock in active_networks:
        active_networks[dest_sock] = False
        return
    print("failLink() error: no connection found")

def fixLink(dest_sock):
    if dest_sock in active_networks:
        active_networks[dest_sock] = True
        return
    print("fixLink() error: no connection found")

def failProcess():
    print("closing all connections...")
    self_socket.close()
    server1.close()
    server2.close()
    server3.close()
    server4.close()

    #store blockchain data into file
    filename = "data_server"+str(process_id)+".txt"
    write_blockchain_to_file(filename)

    os._exit(0)

#initialize blockchain with operation and <k,v> but no hash or nonce
'''blockchain =  [
                [["get", "a_netid"], "", ""],
                [["put", "a_netid", {"phone_number":"111-222-3333"}], "", ""],
                [["put", "bob_netid", {"phone_number":"333-222-1111"}], "", ""],
                [["get", "bob_netid"], "", ""],
                [["get", "cat_netid"], "", ""]
                ]'''

blockchain = []
kvStore = {}
q = queue.Queue()
active_networks = {}


process_id = int(sys.argv[1])

file = open('config.json')
data = json.load(file)
PORT = data[str(process_id)]

#initialize blockchain from written file if there's data in it
filename = "data_server"+str(process_id)+".txt"
if os.path.exists(filename) and os.stat(filename).st_size > 0:
    read_blockchain_from_file(filename)

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
