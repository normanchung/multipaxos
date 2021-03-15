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
        server1.connect((socket.gethostname(), data["server2"]))
        server2.connect((socket.gethostname(), data["server3"]))
        server3.connect((socket.gethostname(), data["server4"]))
        server4.connect((socket.gethostname(), data["server5"]))

    elif process_id == 2:
        server1.connect((socket.gethostname(), data["server1"]))
        server2.connect((socket.gethostname(), data["server3"]))
        server3.connect((socket.gethostname(), data["server4"]))
        server4.connect((socket.gethostname(), data["server5"]))

    elif process_id == 3:
        server1.connect((socket.gethostname(), data["server1"]))
        server2.connect((socket.gethostname(), data["server2"]))
        server3.connect((socket.gethostname(), data["server4"]))
        server4.connect((socket.gethostname(), data["server5"]))

    elif process_id == 4:
        server1.connect((socket.gethostname(), data["server1"]))
        server2.connect((socket.gethostname(), data["server2"]))
        server3.connect((socket.gethostname(), data["server3"]))
        server4.connect((socket.gethostname(), data["server5"]))

    elif process_id == 5:
        server1.connect((socket.gethostname(), data["server1"]))
        server2.connect((socket.gethostname(), data["server2"]))
        server3.connect((socket.gethostname(), data["server3"]))
        server4.connect((socket.gethostname(), data["server4"]))

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
                print(str(process_id) + " connecting to servers...")
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

####TODO: add client to every function until decision as parameter
def send_proposal():
    global current_num
    
    current_num += 1
    message = "prepare," + str(current_index) + "," + str(current_num) + "," + str(process_id) #more things to add here for local comparison?
    message = message.encode()
    #broadcast message to all servers
    time.sleep(5)
    server1.sendall(message)
    server2.sendall(message)
    server3.sendall(message)
    server4.sendall(message)

def receive_proposal(received_index, received_num, proposer_pid):
    #check if the values are less than current values for rejection
    #otherwise accept and send promise
    #index aka current_index of blockchain, num aka ballot num of current paxos iteration, pid of leader

    old_index = current_index
    old_num = current_num
    old_pid = current_pid
    check_ballot_no(received_index, received_num, pid)
    send_promise(current_index, current_num, current_pid, old_index, old_num, old_pid, proposer_pid)
    
def send_promise(current_index, current_num, current_pid, old_index, old_num, old_pid, proposer_pid):
    global is_leader
    global leader
    global received_promise_counter
    global received_promise_counter_dict
    global sent_accept

    received_promise_counter = 0
    received_promise_counter_dict = []
    sent_accept = False
    is_leader = False
    leader = get_correct_server(proposer_pid)

    message = "promise," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + str(old_index) + "," + str(old_num) + "," + str(old_pid) + "," + str(accepted_block)
    ########change accepted_block to entire block TODO: change block to serialized string thing
    #what if accepted_block is none?
    message = message.encode()
    #send to server that sent it to me
    #maybe use get_correct_server to determine what server sent info and make that the leader variable

    time.sleep(5)
    leader.sendall(message)

def receive_promise(received_index, received_num, received_pid, old_index, old_num, old_pid, received_block):
    #check for if any other message was already accepted, and then start proposing this value
    #Upon receive (“promise”, BallotNum, b, val) from majority
    #if all vals = bottom then myVal = initial value
    #else myVal = received val with highest b 
    global current_index
    global current_num
    global current_pid
    global accepted_block
    global make_new_block
    global is_leader
    global received_promise_counter
    global received_promise_counter_dict
    global block_ballot_no_dict
    global sent_accept

    if not sent_accept:
        #todo: not sure if this takes care of if there's a majority acceptVal thing. check later
        received_promise_counter = received_promise_counter + 1

        if received_block not in received_promise_counter_dict:
            received_promise_counter_dict[received_block] = 1
        else:
            received_promise_counter_dict[received_block] = received_promise_counter_dict[received_block] + 1

        block_ballot_no_dict[received_block] = [old_index, old_num, old_pid]

        for block in received_promise_counter_dict:
            if received_promise_counter_dict[block] >= 2:
                accepted_block = block

        if(accepted_block == None): #THIS BOOLEAN OF block_exists DOES NOT WORK
            #current_index = received_index
            #current_num = received_num
            #current_pid = received_pid
            #generate_block(unique_id, block_operation, key, value)
            #accepted_block = blockchain[-1]
            make_new_block = True
        else:
            current_index = block_ballot_no_dict[accepted_block][0]
            current_num = block_ballot_no_dict[accepted_block][1]
            current_pid = block_ballot_no_dict[accepted_block][2]
            make_new_block = False

        #TODO: make this current server the leader variable
        if (received_promise_counter >= 2):
            is_leader = True
            sent_accept = True
            send_accept()

def send_accept():
    global accepted_block
    global received_accepted_counter_dict

    block_operation = ""
    key = ""
    value = {}
    unique_id = ""
    block_exists = False
    received_accepted_counter_dict = []

    operation = q.get()
    if (operation.split(',')[0] == "put"):
        block_operation = "put"
        key = operation.split(',')[1]
        value = operation.split(',')[2] #dictionary
        unique_id = operation.split(',')[3]
    elif (operation.split(',')[0] == "get"):
        block_operation = "get"
        key = operation.split(',')[1]
        value = None
        unique_id = operation.split(',')[2]

    for i in range(len(blockchain)):
        if blockchain[i][0] == unique_id:
            block_exists = True

    if(make_new_block):
        generate_block(unique_id, block_operation, key, value)
        accepted_block = blockchain[-1]
    elif not block_exists:
        blockchain.append(accepted_block)

    block_serialized = json.dumps(accepted_block)
    message = "accept," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + block_serialized
    message = message.encode()
    time.sleep(5)
    server1.sendall(message)
    server2.sendall(message)
    server3.sendall(message)
    server4.sendall(message)

def receive_accept(received_index, received_num, received_pid, received_block):
    global accepted_block

    #check for majority through threads
    #check if received ballot number is greater than current
    #if yes, change accept_num to received ballot_no, accept_block to my_block
    
    if (check_ballot_no(received_index, received_num, received_pid)):
        accepted_block = received_block

        block_exists = False

        for i in range(len(blockchain)):
            if blockchain[i][0] == unique_id:
                block_exists = True
        if not block_exists:
            blockchain.append(accepted_block) #TODO CHANGE THIS TO SEND TO GENERATE_BLOCK
        send_accepted(current_index, current_num, current_pid, accepted_block)
  
def send_accepted(current_index, current_num, current_pid, accepted_block):
    #once this is sent the values are locked and will be decided upon given a majority
    #use a boolean so that future messages won't interrupt this process?
    global received_accepted_counter
    global sent_decision

    received_accepted_counter = 0
    sent_decision = False

    message = "accepted," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + str(accepted_block)
    message = message.encode()
    time.sleep(5)
    leader.sendall(message)

def receive_accepted(received_index, received_num, received_pid, received_block):
    global received_accepted_counter
    global sent_decision
    global received_accepted_counter_dict
    global block_ballot_no_dict

    if not sent_decision:
        if received_block not in received_accepted_counter_dict:
            received_accepted_counter_dict[received_block] = 1
        else:
            received_accepted_counter_dict[received_block] = received_accepted_counter_dict[received_block] + 1

        block_ballot_no_dict[received_block] = [old_index, old_num, old_pid]

        for block in received_promise_counter_dict:
            if received_promise_counter_dict[block] >= 2:
                received_accepted_counter = received_promise_counter_dict[block]
                accepted_block = block
                current_index = block_ballot_no_dict[block][0]
                current_num = block_ballot_no_dict[block][1]
                current_pid = block_ballot_no_dict[block][2]

        if (received_accepted_counter >= 2):
            sent_decision = True
            send_decision(current_index, current_num, current_pid, accepted_block)
    
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
    send_message_to_client(final_block)
    blockchain[-1][-1] = True #set last block to be "decided"

def receive_decision(received_index, received_num, received_pid, received_block):
    if received_block == blockchain[-1]:
        blockchain[-1][-1] = True
        #todo: write blockchain to file?
    pass

def send_message_to_client(client, block):
    client_socket = get_client_socket(client)
    if block[0][0] == "put":
        message = "ack"
        message_bytes = message.encode()
        client_socket.sendall(message_bytes)
    elif block[0][0] == "get":
        value_dict = kv_store[block[0][1]]
        message = json.dumps(value_dict)
        message_bytes = message.encode()
        client_socket.sendall(message_bytes)


#sends in client as a string, i.e. "client1"
def get_client_socket(client):
    if client[-1] == '1':
        return client1
    elif client[-1] == '2':
        return client2
    elif client[-1] == '3':
        return client3
    else:
        print("no correct client found for ", client)
  
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
                q.put(message)
                send_accept()
        elif message[0:3] == "put":
            #from clients
            if not is_leader:
                message = message.encode()
                leader.sendall(message)
            else:
                #start proposal
                q.put(message)
                send_accept()
        elif message[0:6] == "leader":
            #from client
            #start leader election as leader aka SEND proposal
            send_proposal()
        elif message[0:7] == "prepare":
            #from leader
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            proposer_pid = int(message.split(',')[3])
            #operation = int(message.split(',')[4])
            receive_proposal(received_index, received_num, proposer_pid)
        elif message[0:7] == "promise":
            #from server
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            old_index = int(message.split(',')[4])
            old_num = int(message.split(',')[5])
            old_pid = int(message.split(',')[6])
            proposer_pid = int(message.split(',')[7])
            received_block = json.loads(message.split(',')[8]) #TODO CHANGE ALL RECEIVED BLOCKS TO NOT CAST TO INT, BUT TO THE SERIALIZE THING
            #operation = int(message.split(',')[8])
            receive_promise(received_index, received_num, received_pid, old_index, old_num, old_pid, received_block)
        elif message[0:8] == "accepted":
            #from server
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            received_block = json.loads(message.split(',')[4])
            receive_accepted(received_index, received_num, received_pid, received_block)
        elif message[0:6] == "accept":
            #from leader
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            received_block = json.loads(message.split(',')[4])
            receive_accept(received_index, received_num, received_pid, received_block)
        elif message[0:6] == "decide":
            #from leader
            receive_decision(message)

        #send received message from client
        print("message: " + message + ", received from " + sender)

#generate hashes and nonces for each block
def generate_block(unique_id, current_operation, key, value):
    global blockchain
    letters = string.ascii_letters
    current_hash = ""
    i = len(blockchain)
    if i != 0:
        previous_operation = blockchain[i-1][1][0]
        previous_hash = blockchain[i-1][2]
        previous_nonce = blockchain[i-1][3]
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
        block_op.append(value) #TODO add json.loads for value dictionary
    blockchain.append([unique_id, block_op, current_hash, nonce, False])
        

def write_blockchain_to_file(filename):
    f = open(filename, "w")
    for i in range(len(blockchain)):
        f.write("operation:" + blockchain[i][1][0]+"\n")
        f.write("key:" + blockchain[i][1][1]+"\n")
        if len(blockchain[i][1]) > 2:
            for key, value in blockchain[i][1][2].items():
                f.write("value_k:" + key +"\n")
                f.write("value_v:" + value +"\n")
        f.write("hash:" + blockchain[i][2]+"\n")
        f.write("nonce:" + blockchain[i][3]+"\n")
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
    global kv_store
    for i in range(len(blockchain)):
        val = {}
        if blockchain[i][1][0] == "put":
            for key, value in blockchain[i][1][2].items():
                val[key] = value
            kv_store[blockchain[i][1][1]] = val

def recover_data():
    read_blockchain_from_file()
    generate_kv_store()

def print_blockchain():
    for i in range(len(blockchain)):
        print(blockchain[i])

def print_kv_store():
    print(kv_store)

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
                ["unique id", ["get", "a_netid"], "previous block hash", "nonce", boolean(decided)],
                ["", ["put", "a_netid", {"phone_number":"111-222-3333"}], "", "", False],
                ["", ["put", "bob_netid", {"phone_number":"333-222-1111"}], "", "", False],
                ["", ["get", "bob_netid"], "", "", False],
                ["", ["get", "cat_netid"], "", "", False]
                ]'''

blockchain = []
kv_store = {}
q = queue.Queue()
active_networks = {}
is_leader = False
leader = None
received_promise_counter = 0
received_accepted_counter = 0
received_promise_counter_dict = []
received_accepted_counter_dict = []
sent_accept = False
sent_decision = False


#for ballot_no comparing
current_index = 0 #depth
current_num = 0 #ballot number
current_pid = 0 #process id

accepted_block = None #accepted block
make_new_block = False
is_leader = False
leader = None

process_id = int(sys.argv[1])

file = open('config.json')
data = json.load(file)
PORT = data["server" + str(process_id)]

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

if(process_id == 1):
    is_leader = True
else:
    is_leader = False
    leader = server1



threading.Thread(target=cmd_input).start()

while True:
    stream, addr = self_socket.accept()
    t = threading.Thread(target=server_listen, args=(stream, addr))
    t.start()
