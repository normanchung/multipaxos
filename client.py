import errno
import json
import os
import socket
import sys
import time
import threading


def connect():
	#setting up the connection from client to all servers
	server1.connect((socket.gethostname(), data["1"]))
	server2.connect((socket.gethostname(), data["2"]))
	server3.connect((socket.gethostname(), data["3"]))
	server4.connect((socket.gethostname(), data["4"]))
	server5.connect((socket.gethostname(), data["5"]))
	print("connected to servers!")

def cmd_input():
	global response_received
	while True:
		try:
			#if input is connect, connect to other clients
			inp = input()
			if inp == 'connect':
				print("connecting to servers...")
				connect()

			#if input is send, send a message to a server
			elif inp[0:4] == 'send':
				which_server = inp[5:12]
				operation = inp[13:inp.find(' ', 13)]
				response_received = False

				if operation == 'leader':
					send_leader_request()
				#print("sending message: " + message + ", from client " + str(process_id) + " to " + which_server)
				elif operation == 'get':
					message = inp[13:]
					message = "client" + str(process_id) + " " + message
					time.sleep(5)
					send_get_request(message)
				elif operation == 'put':
					message = inp[13:]
					message = "client" + str(process_id) + " " + message
					time.sleep(5)
					send_put_request(message)
				else:
					print("not a valid option")
					break

			#if input is exit, close everything
			elif inp == 'exit':
				print("closing all connections...")
				server1.close()
				server2.close()
				server3.close()
				server4.close()
				server5.close()
				os._exit(0)
		except EOFError:
			pass

def send_get_request(message):
	global response_received
	print("sending get request: ", message)
	#message = "get " + key
	message_bytes = message.encode()
	current_server.sendall(message_bytes)

	threading.Thread(target=check_if_response).start()
	recv_msg_bytes = current_server.recv(1024)
	if not recv_msg_bytes:
		return
	response_received = True
	recv_msg = recv_msg_bytes.decode()
	if recv_msg == "NO_KEY": #todo: print out results for all of these?
		return
	else:
		return #todo: handle when client receives a value


def send_put_request(message):
	global response_received
	#print("sending put request: key:", key, ", value: ", value)
	print("sending put request: ", message)
	#message = "put " + key + " " + value
	message_bytes = message.encode()
	current_server.sendall(message_bytes)

	threading.Thread(target=check_if_response).start()
	recv_msg_bytes = current_server.recv(1024)
	if not recv_msg_bytes:
		return
	response_received = True
	recv_msg = recv_msg_bytes.decode()
	if recv_msg == "ack":
		return

def send_leader_request():
	global response_received
	print("sending leader request")
	message = "leader"
	message_bytes = message.encode()
	current_server.sendall(message_bytes)

	threading.Thread(target=check_if_response).start()
	recv_msg_bytes = current_server.recv(1024)
	if not recv_msg_bytes:
		return
	response_received = True
	recv_msg = recv_msg_bytes.decode()
	if recv_msg == "ok": #todo: change this to what's sent when new leader is determined
		return

def switch_servers():
	print("switching servers")
	global current_server
	if current_server == server1:
		current_server = server2
	elif current_server == server2:
		current_server = server3
	elif current_server == server3:
		current_server = server4
	elif current_server == server4:
		current_server = server5
	elif current_server == server5:
		current_server = server1

def check_if_response():
	time.sleep(20) #todo: change timeout time if needed
	if not response_received:
		handle_no_response()

def handle_no_response():
	print("no response from current server. switching servers and sending leader request")
	switch_servers()
	send_leader_request()

#todo: do i have to do the faillink stuff and print blockchain in the client?


#ask tomorrow if we should expect "connect" input to connect to servers
process_id = int(sys.argv[1])

file = open('config.json')
data = json.load(file)

server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

current_server = server1
response_received = False

threading.Thread(target=cmd_input).start()
