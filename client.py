import errno
import json
import os
import socket
import sys
import time
import threading
import string
import random
import pickle


def connect():
	#setting up the connection from client to all servers
	server1.connect((socket.gethostname(), data["server1"]))
	server2.connect((socket.gethostname(), data["server2"]))
	server3.connect((socket.gethostname(), data["server3"]))
	server4.connect((socket.gethostname(), data["server4"]))
	server5.connect((socket.gethostname(), data["server5"]))
	print("connected to servers!")

def cmd_input():
	global response_received
	global message_to_send
	while True:
		try:
			#if input is connect, connect to other clients
			inp = input()
			if inp == 'connect':
				print("connecting to servers...")
				connect()

			#if input is send, send a message to a server
			elif inp[0:4] == 'send':
				operation = inp[13:inp.find(' ', 13)]
				response_received = False
				unique_id = generate_unique_id()

				if operation == 'leader':
					send_leader_request()
				#print("sending message: " + message + ", from client " + str(process_id) + " to " + which_server)
				elif operation == 'get':
					which_server = inp[5:12]
					change_current_server(int(which_server[-1]))
					message = inp[13:]
					message = "client" + str(process_id) + " " + message
					message = message + " " + unique_id
					message_to_send = message
					time.sleep(5)
					send_get_request(message)
				elif operation == 'put':
					which_server = inp[5:12]
					change_current_server(int(which_server[-1]))
					message = inp[13:]
					message = "client" + str(process_id) + " " + message
					message = message + " " + unique_id
					message_to_send = message
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

def listen_on_port(stream, addr):
	global response_received
	while True:
		print("listening on port")
		recv_msg_bytes = stream.recv(1024)
		print("received message")
		if not recv_msg_bytes:
			return
		response_received = True
		recv_msg = recv_msg_bytes.decode()
		if recv_msg == "NO_KEY":
			print("no key in server's kv_store")
			pass
		elif recv_msg == "ack":
			print("successfully put data in server's kv_store")
			pass
		elif recv_msg == "election successful": #todo: change this to what's sent when new leader is determined
			if (message_to_send[:message_to_send.find(',')] == "get"):
				send_get_request(message)
			elif (message_to_send[:message_to_send.find(',')] == "put"):
				send_put_request(message)
			else:
				print("message_to_send is not a get or put request")
			pass
		else:
			value_dict = pickle.loads(recv_msg.encode('latin1'))
			print("received dict from server: ", value_dict)
			pass

def send_get_request(message):
	global response_received
	message = message.replace(' ', ',')
	print("sending get request: ", message)
	#message = "get " + key
	message_bytes = message.encode()
	current_server.sendall(message_bytes)

	threading.Thread(target=check_if_response).start()


def send_put_request(message):
	global response_received
	#print("sending put request: key:", key, ", value: ", value)
	message = message.replace(' ', ',')
	print("sending put request: ", message)
	#message = "put " + key + " " + value
	message_bytes = message.encode()
	current_server.sendall(message_bytes)

	threading.Thread(target=check_if_response).start()

def send_leader_request():
	global response_received
	print("sending leader request")
	message = "client" + str(process_id) + ",leader"
	message_bytes = message.encode()
	current_server.sendall(message_bytes)

	threading.Thread(target=check_if_response).start()

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
	global message_to_send
	time.sleep(23) #todo: change timeout time if needed
	print("checking if responded now")
	if not response_received:
		handle_no_response()
	else:
		message_to_send = ""

def handle_no_response():
	print("no response from current server. switching servers and sending leader request")
	switch_servers()
	send_leader_request()

def generate_unique_id():
	letters = string.ascii_letters
	unique_id = ''.join(random.choice(letters) for i in range(20))
	return unique_id

def change_current_server(server):
	global current_server
	if server == 1:
		current_server = server1
	elif server == 2:
		current_server = server2
	elif server == 3:
		current_server = server3
	elif server == 4:
		current_server = server4
	elif server == 5:
		current_server = server5

#ask tomorrow if we should expect "connect" input to connect to servers
process_id = int(sys.argv[1])

file = open('config.json')
data = json.load(file)

client_socket = socket.socket()
client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
client_socket.bind((socket.gethostname(), data["client"+str(process_id)]))
client_socket.listen(32)

server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

current_server = server1
response_received = False
message_to_send = ""

threading.Thread(target=cmd_input).start()

while True:
	stream, addr = client_socket.accept()
	t = threading.Thread(target=listen_on_port, args=(stream, addr))
	t.start()
