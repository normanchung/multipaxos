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
				message = inp[13:]
				print("sending message: " + message + ", from client " + str(process_id) + " to " + which_server)
				message = "client" + str(process_id) + " " + message
				message = message.encode()
				time.sleep(5)
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

#ask tomorrow if we should expect "connect" input to connect to servers
process_id = int(sys.argv[1])

file = open('config.json')
data = json.load(file)

server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

threading.Thread(target=cmd_input).start()
