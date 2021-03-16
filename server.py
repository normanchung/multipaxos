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
import pickle


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

    client1.connect((socket.gethostname(), data["client1"]))
    client2.connect((socket.gethostname(), data["client2"]))
    client3.connect((socket.gethostname(), data["client3"]))

    active_networks[server1] = True
    active_networks[server2] = True
    active_networks[server3] = True
    active_networks[server4] = True
    #todo: check if i have to keep client connections in active_networks

    print("connected to servers!")

def get_correct_server(dest_server):
    #given the process_id of the current server, you can determine which server to send to by comparing process_id
    #if current process_id is 3, and destination is 2, you would do a server2 send
    #if current process_id is 3, and destination is 4, you would do a server3 send (subtract 1)
    if (dest_server < process_id):
        if (dest_server == 1):
            return server1
        elif (dest_server == 2):
            return server2
        elif (dest_server == 3):
            return server3
        elif (dest_server == 4):
            return server4
        elif (dest_server == 5):
            return server5
    elif (dest_server > process_id):
        if (dest_server == 2):
            return server1
        elif (dest_server == 3):
            return server2
        elif (dest_server == 4):
            return server3
        elif (dest_server == 5):
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
                if active_networks[server1]:
                    try:
                        server1.sendall(message)
                    except RuntimeError:
                        active_networks[server1] = False
                        server1.close()
                if active_networks[server2]:
                    try:
                        server2.sendall(message)
                    except RuntimeError:
                        active_networks[server2] = False
                        server2.close()
                if active_networks[server3]:
                    try:
                        server3.sendall(message)
                    except RuntimeError:
                        active_networks[server3] = False
                        server3.close()
                if active_networks[server4]:
                    try:
                        server4.sendall(message)
                    except RuntimeError:
                        active_networks[server4] = False
                        server4.close()

            #if input is send, send from one server to another
            elif inp[0:4] == 'send':
                dest_server = inp[5:12]
                message = inp[13:]
                print("sending message: " + message + ", from client " + str(process_id) + " to " + dest_server)
                message = "client" + str(process_id) + " " + message
                message = message.encode()
                time.sleep(5)
                send_between_servers(int(dest_server[-1]), message)

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
                server = get_correct_server(int(inp[len(inp)-1:]))
                failLink(server)

            elif inp[:8] == 'fix link':
                server = get_correct_server(int(inp[len(inp)-1:]))
                fixLink(server)

            #if input is fail process, close everything
            elif inp == 'fail process':
                failProcess()

        except EOFError:
            pass

def send_client_election_successful(client):
    client_socket = get_client_socket(client)
    message = "election successful"
    message = message.encode()
    client_socket.sendall(message)

def send_server_election_successful():
    message = "elected,server" + str(process_id)
    message = message.encode()
    if active_networks[server1]:
        try:
            server1.sendall(message)
        except RuntimeError:
            active_networks[server1] = False
            server1.close()
    if active_networks[server2]:
        try:
            server2.sendall(message)
        except RuntimeError:
            active_networks[server2] = False
            server2.close()
    if active_networks[server3]:
        try:
            server3.sendall(message)
        except RuntimeError:
            active_networks[server3] = False
            server3.close()
    if active_networks[server4]:
        try:
            server4.sendall(message)
        except RuntimeError:
            active_networks[server4] = False
            server4.close()


####TODO: add client to every function until decision as parameter
def send_proposal(client, unique_id):
    global received_promise_counter
    global sent_accept

    sent_accept = False
    received_promise_counter = 0
    print("server",str(process_id)," sending proposal")

    message = "prepare," + str(current_index) + "," + str(current_num) + "," + str(process_id) + ","+ str(client)+","+unique_id #more things to add here for local comparison?
    message = message.encode()
    #broadcast message to all servers
    time.sleep(5)
    if active_networks[server1]:
        try:
            server1.sendall(message)
        except RuntimeError:
            active_networks[server1] = False
            server1.close()
    if active_networks[server2]:
        try:
            server2.sendall(message)
        except RuntimeError:
            active_networks[server2] = False
            server2.close()
    if active_networks[server3]:
        try:
            server3.sendall(message)
        except RuntimeError:
            active_networks[server3] = False
            server3.close()
    if active_networks[server4]:
        try:
            server4.sendall(message)
        except RuntimeError:
            active_networks[server4] = False
            server4.close()

def receive_proposal(received_index, received_num, proposer_pid, client, unique_id):
    print("server",str(process_id)," receiving proposal")
    #check if the values are less than current values for rejection
    #otherwise accept and send promise
    #index aka current_index of blockchain, num aka ballot num of current paxos iteration, pid of leader

    old_index = current_index
    old_num = current_num
    old_pid = current_pid
    if(check_ballot_no(received_index, received_num, proposer_pid)):
        send_promise(current_index, current_num, current_pid, client, old_index, old_num, old_pid, proposer_pid, unique_id)

def send_promise(current_index, current_num, current_pid, client, old_index, old_num, old_pid, proposer_pid, unique_id):
    print("server",str(process_id)," sending promise")
    global is_leader
    global leader
    global received_promise_counter
    global received_promise_counter_dict

    received_promise_counter = 0
    received_promise_counter_dict = {}
    is_leader = False
    leader = get_correct_server(proposer_pid)

    block_serialized = pickle.dumps(accepted_block)
    message = "promise," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + str(client) + "," + str(old_index) + "," + str(old_num) + "," + str(old_pid) + "," + block_serialized.decode('latin1') + "," + str(unique_id)
    #TODO: change block to serialized string thing
    #what if accepted_block is none?
    message = message.encode()
    #send to server that sent it to me
    #maybe use get_correct_server to determine what server sent info and make that the leader variable

    time.sleep(5)
    if active_networks[leader]:
        try:
            leader.sendall(message)
        except RuntimeError:
            active_networks[leader] = False
            leader.close()

def receive_promise(received_index, received_num, received_pid, client, old_index, old_num, old_pid, received_block, unique_id):
    print("server",str(process_id)," receiving promise")
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

        if received_block is not None:
            unique_id = received_block[0]
            if unique_id not in received_promise_counter_dict:
                received_promise_counter_dict[unique_id] = 1
            else:
                received_promise_counter_dict[unique_id] = received_promise_counter_dict[unique_id] + 1

            block_ballot_no_dict[unique_id] = [old_index, old_num, old_pid]

        for uni_id in received_promise_counter_dict:
            if received_promise_counter_dict[uni_id] >= 2:
                for i in range(len(blockchain)):
                    if uni_id == blockchain[i][0]:
                        print("got majority promises for block w/ unique id ", uni_id)
                        accepted_block = blockchain[i]
                        current_index = block_ballot_no_dict[accepted_block[0]][0]
                        current_num = block_ballot_no_dict[accepted_block[0]][1]
                        current_pid = block_ballot_no_dict[accepted_block[0]][2]

        if(accepted_block == None): #THIS BOOLEAN OF block_exists DOES NOT WORK
            #current_index = received_index
            #current_num = received_num
            #current_pid = received_pid
            #generate_block(unique_id, block_operation, key, value)
            #accepted_block = blockchain[-1]
            client_sender = client
            make_new_block = True
        else:
            make_new_block = False

        #TODO: make this current server the leader variable
        if (received_promise_counter >= 2):
            is_leader = True
            sent_accept = True
            send_client_election_successful(client, unique_id) #TODO: check if i should send to accept or send ack to client then wait for request
            send_server_election_successful()
def send_accept():
    print("server",str(process_id)," sending accept")
    global accepted_block
    global received_accepted_counter
    global received_accepted_counter_dict
    global sent_decision
    global current_index
    global current_num
    global current_pid
    global currently_sending

    current_num += 1
    sent_decision = False
    currently_sending = True
    block_operation = ""
    key = ""
    value = {}
    unique_id = ""

    block_exists = False
    received_accepted_counter_dict = {}
    received_accepted_counter = 0
    #print(q.queue[0])
    operation = q.get()
    print ("operation: ", operation)
    client_sender = int(operation.split(',')[0][-1])
    if (operation.split(',')[1] == "put"):
        block_operation = "put"
        key = operation.split(',')[2]
        value = operation.split(',')[3] #dictionary
        unique_id = operation.split(',')[4]
        current_pid = int(operation.split(',')[5][-1])
    elif (operation.split(',')[1] == "get"):
        block_operation = "get"
        key = operation.split(',')[2]
        value = None
        unique_id = operation.split(',')[3]
        current_pid = int(operation.split(',')[4][-1])

    for i in range(len(blockchain)):
        if blockchain[i][0] == unique_id:
            accepted_block = blockchain[i]
            block_exists = True

    current_index = len(blockchain)

    if(make_new_block and not block_exists):
        generate_block(unique_id, block_operation, key, value, client_sender)
        accepted_block = blockchain[-1]
        add_block_to_file(accepted_block)
    elif not block_exists:
        blockchain.append(accepted_block)
        add_block_to_file(accepted_block)
    else:
        pass
        #TODO: doesn't this mean that the block has already been added, and should just return to client right away with an ack or a key value?

    block_serialized = pickle.dumps(accepted_block)
    message = "accept," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + block_serialized.decode('latin1')
    print(message)
    message = message.encode()
    time.sleep(5)
    if active_networks[server1]:
        try:
            server1.sendall(message)
        except RuntimeError:
            active_networks[server1] = False
            server1.close()
    if active_networks[server2]:
        try:
            server2.sendall(message)
        except RuntimeError:
            active_networks[server2] = False
            server2.close()
    if active_networks[server3]:
        try:
            server3.sendall(message)
        except RuntimeError:
            active_networks[server3] = False
            server3.close()
    if active_networks[server4]:
        try:
            server4.sendall(message)
        except RuntimeError:
            active_networks[server4] = False
            server4.close()

def receive_accept(received_index, received_num, received_pid, received_block):
    print("server",str(process_id)," receiving accept")
    global accepted_block

    #check for majority through threads
    #check if received ballot number is greater than current
    #if yes, change accept_num to received ballot_no, accept_block to my_block
    if received_block is not None:
        #print(received_block)
        pass
    else:
        print("received block is none")

    if (check_ballot_no(received_index, received_num, received_pid)):
        accepted_block = received_block
        unique_id = accepted_block[0]
        block_exists = False

        for i in range(len(blockchain)):
            if blockchain[i][0] == unique_id:
                block_exists = True
        if not block_exists:
            blockchain.append(accepted_block) #TODO CHANGE THIS TO SEND TO GENERATE_BLOCK
            add_block_to_file(accepted_block)
        send_accepted(current_index, current_num, current_pid, accepted_block)

def send_accepted(current_index, current_num, current_pid, accepted_block):
    print("server",str(process_id)," sending accepted")
    #once this is sent the values are locked and will be decided upon given a majority
    #use a boolean so that future messages won't interrupt this process?
    global received_accepted_counter

    if accepted_block is not None:
        print(accepted_block)
    else:
        print("accepted block is none")


    received_accepted_counter = 0

    block_serialized = pickle.dumps(accepted_block)
    message = "accepted," + str(current_index) + "," + str(current_num) + "," + str(current_pid) + "," + block_serialized.decode('latin1')
    message = message.encode()
    time.sleep(5)
    if active_networks[leader]:
        try:
            leader.sendall(message)
        except RuntimeError:
            active_networks[leader] = False
            leader.close()

def receive_accepted(received_index, received_num, received_pid, received_block):
    print("server",str(process_id)," receiving accepted")
    global received_accepted_counter
    global sent_decision
    global received_accepted_counter_dict
    global block_ballot_no_dict
    global current_index
    global current_num
    global current_pid
    global accepted_block

    if received_block is not None:
        print(received_block)
    else:
        print("received block is none")


    if not sent_decision:
        if received_block is not None:
            unique_id = received_block[0]
            if unique_id not in received_accepted_counter_dict:
                received_accepted_counter_dict[unique_id] = 1
            else:
                received_accepted_counter_dict[unique_id] = received_accepted_counter_dict[unique_id] + 1

            block_ballot_no_dict[unique_id] = [received_index, received_num, received_pid]

        for uni_id in received_accepted_counter_dict:
            if received_accepted_counter_dict[uni_id] >= 2:
                for i in range(len(blockchain)):
                    if uni_id == blockchain[i][0]:
                        accepted_block = blockchain[i]
                        received_accepted_counter = received_accepted_counter_dict[uni_id]
                        current_index = block_ballot_no_dict[accepted_block[0]][0]
                        current_num = block_ballot_no_dict[accepted_block[0]][1]
                        current_pid = block_ballot_no_dict[accepted_block[0]][2]

        if (received_accepted_counter >= 2):
            sent_decision = True
            send_decision(current_index, current_num, current_pid, accepted_block)

def send_decision(final_index, final_num, final_pid, final_block):
    global currently_sending
    if(is_leader):
        print("server",str(process_id)," sending decision")
        #broadcast final decision value to all servers, add to blockchain
        block_serialized = pickle.dumps(final_block)
        message = "decide," + str(final_index) + "," + str(final_num) + "," + str(final_pid) + "," + block_serialized.decode('latin1')
        message = message.encode()
        #broadcast to all servers
        time.sleep(5)
        if active_networks[server1]:
            try:
                server1.sendall(message)
            except RuntimeError:
                active_networks[server1] = False
                server1.close()
        if active_networks[server2]:
            try:
                server2.sendall(message)
            except RuntimeError:
                active_networks[server2] = False
                server2.close()
        if active_networks[server3]:
            try:
                server3.sendall(message)
            except RuntimeError:
                active_networks[server3] = False
                server3.close()
        if active_networks[server4]:
            try:
                server4.sendall(message)
            except RuntimeError:
                active_networks[server4] = False
                server4.close()

        send_message_to_client(final_block[1], final_block) #TODO: needs client as a parameter here, check if final_block should be serialized
        for i in range(len(blockchain)):
            if blockchain[i][0] == final_block[0]:
                blockchain[i][-1] = True

        if final_block[2][0] == "put":
            kv_store[final_block[2][1]] = final_block[2][2]
        current_index = len(blockchain) #depth
        current_num = 0 #ballot number
        current_pid = 0 #process id

        accepted_block = None #accepted block
        currently_sending = False

def receive_decision(received_index, received_num, received_pid, received_block):
    print("server",str(process_id)," receiving decision")

    current_index = len(blockchain) #depth
    current_num = 0 #ballot number
    current_pid = 0 #process id
    accepted_block = None #accepted block

    block_in_blockchain = False
    for i in range(len(blockchain)):
        if blockchain[i][0] == received_block[0]:
            block_in_blockchain = True
            blockchain[i][-1] = True
    if not block_in_blockchain:
        received_block[-1] = True
        blockchain.append(received_block)
    if received_block[2][0] == "put":
        kv_store[received_block[2][1]] = received_block[2][2]

def send_message_to_client(client, block):
    print("server",str(process_id)," sending message to client", str(client))
    client_socket = get_client_socket(client)
    message_bytes = b''
    if block[2][0] == "put":
        message = "ack,"+block[0]
        message_bytes = message.encode()
        client_socket.sendall(message_bytes)
    elif block[2][0] == "get":
        if block[2][1] not in kv_store:
            message = "NO_KEY,"+block[0]
            message_bytes = message.encode()
        else:
            value_dict = kv_store[block[2][1]]
            print(value_dict)
            message_bytes = pickle.dumps(value_dict).decode('latin1').encode()+","+block[0]
        #message_bytes = message.encode()
        client_socket.sendall(message_bytes)


#sends in client as a string, i.e. "client1"
def get_client_socket(client):
    if client == 1:
        return client1
    elif client == 2:
        return client2
    elif client == 3:
        return client3
    else:
        print("no correct client found for ", client)

def check_ballot_no(index, num, pid): ############TODO: check if value is None
    print("server",str(process_id)," checking ballot no")
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

def start_send():
  while True:
    if not q.empty() and not currently_sending and is_leader:
        send_accept()

def server_listen(stream, addr):
    global is_leader
    global leader

    while True:
        #listen for a request from other clients
        message = stream.recv(1024)
        if not message:
            break
        message = message.decode()
        #message = message[8:]
        #potentially make message[0:7] the client or server number
        if message.split(',')[1] == "get":
            client_sender = int(message.split(',')[0][-1])
            #from clients
            if message[len(message)-7:len(message)-1] != "server":
                message = message + ",server"+str(process_id)
            print("server",str(process_id)+" received get request. message: ", message)
            if not is_leader:
                message = message.encode()
                if active_networks[leader]:
                    try:
                        leader.sendall(message)
                    except RuntimeError:
                        active_networks[leader] = False
                        leader.close()
            else:
                #start proposal
                q.put(message)
        elif message.split(',')[1] == "put":
            if message[len(message)-7:len(message)-1] != "server":
                message = message + ",server"+str(process_id)
            print("server",str(process_id)+" received put request. message: ", message)
            client_sender = int(message.split(',')[0][-1])
            #from clients
            if not is_leader:
                message = message.encode()
                if active_networks[leader]:
                    try:
                        leader.sendall(message)
                    except RuntimeError:
                        active_networks[leader] = False
                        leader.close()
            else:
                #start proposal
                q.put(message)
        elif message.split(',')[1] == "leader":
            #from client
            #message = message + ",server"+str(process_id) todo: is this line necessary?
            client_sender = int(message.split(',')[0][-1])
            unique_id = message.split(',')[2]
            if is_leader:
                send_client_election_successful(client_sender, unique_id)
            else:
                #start leader election as leader aka SEND proposal
                send_proposal(client_sender, unique_id)
        elif message.split(',')[0] == "prepare":
            #from leader
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            proposer_pid = int(message.split(',')[3])
            client = int(message.split(',')[4])
            unique_id = message.split(',')[5]
            #operation = int(message.split(',')[4])
            receive_proposal(received_index, received_num, proposer_pid, client, unique_id)
        elif message.split(',')[0] == "promise":
            #from server
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            received_client = int(message.split(',')[4])
            old_index = int(message.split(',')[4])
            old_num = int(message.split(',')[5])
            old_pid = int(message.split(',')[6])
            proposer_pid = int(message.split(',')[7])
            received_block = pickle.loads(message.split(',')[8].encode('latin1'))
            unique_id = message.split(',')[9]
            #operation = int(message.split(',')[8])
            receive_promise(received_index, received_num, received_pid, received_client, old_index, old_num, old_pid, received_block, unique_id)
        elif message.split(',')[0] == "accepted":
            #from server
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            received_block = pickle.loads(message.split(',')[4].encode('latin1'))
            receive_accepted(received_index, received_num, received_pid, received_block)
        elif message.split(',')[0] == "accept":
            #from leader
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            received_block = pickle.loads(message.split(',')[4].encode('latin1'))
            receive_accept(received_index, received_num, received_pid, received_block)
        elif message.split(',')[0] == "decide":
            #from leader
            received_index = int(message.split(',')[1])
            received_num = int(message.split(',')[2])
            received_pid = int(message.split(',')[3])
            received_block = pickle.loads(message.split(',')[4].encode('latin1'))
            receive_decision(received_index, received_num, received_pid, received_block)
        elif message.split(',')[0] == "elected":
            print("received elected message: ", message)
            is_leader = False
            received_server = int(message[-1])
            leader = get_correct_server(received_server)


        #send received message from client
        #print("message: " + message + ", received from " + sender)

#generate hashes and nonces for each block
def generate_block(unique_id, current_operation, key, value, client):
    global blockchain
    letters = string.ascii_letters
    current_hash = ""
    i = len(blockchain)
    if i != 0:
        previous_operation = blockchain[i-1][2][0]
        previous_hash = blockchain[i-1][3]
        previous_nonce = blockchain[i-1][4]
        operation_nonce_hash = (previous_operation+previous_nonce+previous_hash).encode()
        current_hash = hashlib.sha256(operation_nonce_hash).hexdigest()

    h = ""
    while len(h) == 0 or h[len(h)-1] != '0' and h[len(h)-1] != '1' and h[len(h)-1] != '2':
        nonce = ''.join(random.choice(letters) for i in range(6))
        operation_nonce = (current_operation+nonce).encode()
        h = hashlib.sha256(operation_nonce).hexdigest()

    #print("\n")
    #print("hash for previous ptr in blockchain: ", current_hash)
    #print("nonce: ", nonce)
    #print("hashes generated with nonce that should end in 0 to 2: ", h)

    block_op = [current_operation, key]
    if value is not None:
        value_dict = json.loads(value)
        block_op.append(value_dict) #TODO add pickle.loads for value dictionary
    blockchain.append([unique_id, client, block_op, current_hash, nonce, False])

def add_block_to_file(block):
    f = open(filename, "a")
    f.write("unique_id:" + block[0]+"\n")
    f.write("client:" + str(block[1])+"\n")
    f.write("operation:" + block[2][0]+"\n")
    f.write("key:" + block[2][1]+"\n")
    if len(block[2]) > 2:
        for key, value in block[2][2].items():
            f.write("value_k:" + key +"\n")
            f.write("value_v:" + value +"\n")
    f.write("hash:" + block[3]+"\n")
    f.write("nonce:" + block[4]+"\n")
    f.write("decided:" + str(block[5])+"\n")
    f.close()

def write_blockchain_to_file():
    f = open(filename, "w")
    for i in range(len(blockchain)):
        f = open(filename, "a")
        f.write("unique_id:" + blockchain[i][0]+"\n")
        f.write("client:" + blockchain[i][1]+"\n")
        f.write("operation:" + blockchain[i][2][0]+"\n")
        f.write("key:" + blockchain[i][2][1]+"\n")
        if len(block[2]) > 2:
            for key, value in blockchain[i][2][2].items():
                f.write("value_k:" + key +"\n")
                f.write("value_v:" + value +"\n")
        f.write("hash:" + blockchain[i][3]+"\n")
        f.write("nonce:" + blockchain[i][4]+"\n")
        f.write("decided:" + blockchain[i][5]+"\n")
    f.close()

def read_blockchain_from_file(filename):
    global blockchain
    blockchain = []
    f = open(filename, "r")
    while True:
        block = ["", 0,[], "", "", False]

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "unique_id":
            print("error with parsing unique_id")
        block[0] = line[line.find(':')+1:]

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "client":
            print("error with parsing client")
        block[1] = int(line[line.find(':')+1:])

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "operation":
            print("error with parsing operation")
        block[2].append(line[line.find(':')+1:])

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "key":
            print("error with parsing key")
        block[2].append(line[line.find(':')+1:])

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
            block[2].append(kv)

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "hash":
            print("error with parsing hash")
        block[3] = line[line.find(':')+1:]

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "nonce":
            print("error with parsing nonce")
        block[4] = line[line.find(':')+1:]

        line = f.readline().rstrip('\n')
        if not line:
            break
        if line[:line.find(':')] != "decided":
            print("error with parsing decided")
        if (line[line.find(':')+1:] == "True"):
            block[5] = True
        elif (line[line.find(':')+1:] == "False"):
            block[5] = False

        blockchain.append(block)

def generate_kv_store():
    global kv_store
    for i in range(len(blockchain)):
        val = {}
        if blockchain[i][2][0] == "put":
            for key, value in blockchain[i][2][2].items():
                val[key] = value
            kv_store[blockchain[i][2][1]] = val

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
    write_blockchain_to_file(filename)

    os._exit(0)

def check_ballot_no(index, num, pid):
    global current_index
    global current_num
    global current_pid
    print("checking ballot number")
    if not current_index: #checks if current_index is None
        return True

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


blockchain = []
kv_store = {}
q = queue.Queue()
active_networks = {}
is_leader = False
leader = None
received_promise_counter = 0
received_accepted_counter = 0
received_promise_counter_dict = {}
received_accepted_counter_dict = {}
block_ballot_no_dict = {}
sent_accept = False
sent_decision = False
client_sender = 0
currently_sending = False

#for ballot_no comparing
current_index = 0 #depth
current_num = 0 #ballot number
current_pid = 0 #process id

accepted_block = None #accepted block
make_new_block = True

process_id = int(sys.argv[1])
filename = "data_server"+str(process_id)+".txt"

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
client1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

if(process_id == 1):
    print("setting is_leader to True")
    is_leader = True
else:
    is_leader = False
    leader = server1


threading.Thread(target=cmd_input).start()
threading.Thread(target=start_send).start()


while True:
    stream, addr = self_socket.accept()
    t = threading.Thread(target=server_listen, args=(stream, addr))
    t.start()
