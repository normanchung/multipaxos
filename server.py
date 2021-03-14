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

def send_between_servers(dest_server, message):
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

def send_proposal(operation):
    global current_index
    global current_num
    
    current_num += 1
    message = "prepare," + str(current_index) + "," + str(current_num) + "," + str(process_id) + "," + operation #more things to add here for local comparison?
    message = message.encode()
    #broadcast message to all servers
    time.sleep(5)
    server1.sendall(message)
    server2.sendall(message)
    server3.sendall(message)
    server4.sendall(message)

def receive_proposal(received_index, received_num, pid, operation):
    global current_index
    global current_num
    global current_pid
    #check if the values are less than current values for rejection
    #otherwise accept and send promise
    #index aka current_index of blockchain, num aka ballot num of current paxos iteration, pid of leader
    old_index = current_index
    old_num = current_num
    old_pid = current_pid
    check_ballot_no(received_index, received_num, pid)
    send_promise(current_index, current_num, current_pid, old_index, old_num, old_pid, operation)
    
def send_promise(current_index, current_num, current_pid, old_index, old_num, old_pid, operation):    
    message = "promise," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + str(old_index) + "," + str(old_num) + "," + str(old_pid) + "," + str(accepted_block) + operation####change accepted_block if i can't send None type
    ########change accepted_block to entire block TODO: change block to serialized string thing
    message = message.encode() ################accept_num is a pair of the previously accepted ballot number and value(block)
    #send to server that sent it to me
    #maybe use get_correct_server to determine what server sent info and make that the leader variable
    time.sleep(5)
    leader.sendall(message)

def receive_promise(received_index, received_num, received_pid, old_index, old_num, old_pid, received_block, operation):
    #need to check for majority through threads?
    #check for if any other message was already accepted, and then start proposing this value
    #Upon receive (“promise”, BallotNum, b, val) from majority
    #if all vals = bottom then myVal = initial value
    #else myVal = received val with highest b 

    send_accept(received_index, received_num, received_pid, old_index, old_num, old_pid, received_block, operation)

def send_accept(received_index, received_num, received_pid, old_index, old_num, old_pid, received_block, operation):
    global current_index
    global current_num
    global current_pid
    global accepted_block
    
    if (operation.split(',')[0] == "put"):
        block_operation = "put"
        key = operation.split(',')[1]
        value = operation.split(',')[2]
    elif (operation.split(',')[0] == "put"):
        block_operation = "put"
        key = operation.split(',')[1]
        value = None

    if(accepted_block == None):
        current_index = received_index
        current_num = received_num
        current_pid = received_pid
        generate_block(block_operation, key, value)
        accepted_block = blockchain[-1]
    else:
        current_index = old_index
        current_num = old_num
        current_pid = old_pid
        accepted_block = received_block

    message = "accept," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + str(accepted_block)
    message = message.encode()
    time.sleep(5)
    leader.sendall(message)

def receive_accept(received_index, received_num, received_pid, received_block):
    global current_index
    global current_num
    global current_pid
    global accepted_block

    #check for majority through threads
    #check if received ballot number is greater than current
    #if yes, change accept_num to received ballot_no, accept_block to my_block
    
    if (check_ballot_no(received_index, received_num, received_pid)):
        accepted_block = received_block
        send_accepted(current_index, current_num, current_pid, accepted_block)
  
def send_accepted(current_index, current_num, current_pid, accepted_block):
    #once this is sent the values are locked and will be decided upon given a majority
    #use a boolean so that future messages won't interrupt this process?
    message = "accepted," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + str(accepted_block)
    message = message.encode()
    time.sleep(5)
    leader.sendall(message)

def receive_accepted(received_index, received_num, received_pid, received_block):
    #check for majority THREADING, and what value was finally decided on
    send_decision(received_index, received_num, received_pid, received_block)
    
def send_decision(final_index, final_num, final_pid, final_block):
    #broadcast final decision value to all servers, add to blockchain
    message = "decide," + str(final_index) + "," + str(final_num) + "," + str(final_pid) + "," + str(final_block)
    message = message.encode()
    #broadcast to all servers
    time.sleep(5)
    server1.sendall(message)
    server2.sendall(message)
    server3.sendall(message)
    server4.sendall(message)

def receive_decision(received_index, received_num, received_pid, received_block):
    #TODO change block boolean to decided to true
    pass
  
def check_ballot_no(index, num, pid): ############TODO: check if value is None
    global current_index
    global current_num
    global current_pid
    print("checking ballot number")
    if index < current_index:
        return False
    elif num < current_num:
        return False
    elif pid < current_id:
        return False
    current_index = index
    current_num = num
    current_pid = pid
    return True
  

def server_listen(stream, addr):
    while True:
        #listen for a request from other clients
        message = stream.recv(1024)
        if not message:
            break
        message = message.decode()
        sender = message[0:7]
        #message = message[8:]
        #potentially make message[0:7] the client or server number
        if message[0:3] == "get":
            #from clients
            if not is_leader:
                message = message.encode()
                leader.sendall(message)
            else:
                #start proposal
                send_proposal(message)
        elif message[0:3] == "put":
            #from clients
            if not is_leader:
                message = message.encode()
                leader.sendall(message)
            else:
                #start proposal
                send_proposal(message)
        elif message[0:6] == "leader":
            #from client
            #start leader election as leader aka SEND proposal
            send_proposal(message)
        elif message[0:7] == "prepare":
            #from leader
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            pid = int(message.split(',')[3])
            operation = int(message.split(',')[4])
            receive_proposal(received_index, received_num, pid, operation)
        elif message[0:7] == "promise":
            #from server
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            old_index = int(message.split(',')[4])
            old_num = int(message.split(',')[5])
            old_pid = int(message.split(',')[6])
            received_block = int(message.split(',')[7]) #TODO CHANGE ALL RECEIVED BLOCKS TO NOT CAST TO INT, BUT TO THE SERIALIZE THING
            operation = int(message.split(',')[8])
            receive_promise(received_index, received_num, received_pid, old_index, old_num, old_pid, received_block, operation)
        elif message[0:8] == "accepted":
            #from server
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            received_block = int(message.split(',')[4])
            receive_accepted(received_index, received_num, received_pid, received_block)
        elif message[0:6] == "accept":
            #from leader
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            received_block = int(message.split(',')[4])
            receive_accept(received_index, received_num, received_pid, received_block)
        elif message[0:6] == "decide":
            #from leader
            receive_decision(message)

        #send received message from client
        print("message: " + message + ", received from " + sender)

#generate hashes and nonces for each block
def generate_block(current_operation, key, value):
    global blockchain
    letters = string.ascii_letters
    current_hash = ""
    i = len(blockchain)
    if i != 0:
        previous_operation = blockchain[i-1][0][0]
        previous_hash = blockchain[i-1][1]
        previous_nonce = blockchain[i-1][2]
        operation_nonce_hash = (previous_operation+previous_nonce+previous_hash).encode()
        current_hash = hashlib.sha256(operation_nonce_hash).hexdigest()

    h = ""
    while len(h) == 0 or (int(h[len(h)-1]) >= '0' and int(h[len(h)-1]) <= '2'):
        nonce = ''.join(random.choice(letters) for i in range(6))
        operation_nonce = (current_operation+nonce).encode()
        h = hashlib.sha256(operation_nonce).hexdigest()

    print("\n")
    print("hash for previous ptr in blockchain: ", current_hash)
    print("nonce: ", nonce)
    print("hashes generated with nonce that should end in 0 to 2: ", h)

    block_op = [current_operation, key]
    if value is not None:
        block_op.append(value)
    blockchain.append([block_op, current_hash, nonce])
        

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

def check_ballot_no(index, num, pid):
    global current_index
    global current_num
    global current_pid
    print("checking ballot number")
    if index < current_index:
        return False
    elif num < current_num:
        return False
    elif pid < current_id:
        return False
    current_index = index
    current_num = num
    current_pid = pid
    return True

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
is_leader = False
leader = None

#for ballot_no comparing
current_index = 0 #depth
current_num = 0 #ballot number
current_pid = 0 #process id

accepted_block = None #accepted block


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
