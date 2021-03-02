import random
import string
import hashlib

#generate hashes and nonces for each block
def generateBlockchain():
	letters = string.ascii_letters
	for i in range(len(write_blockchain)):
		current_hash = ""
		if i != 0:
			previous_operation = write_blockchain[i-1][0][0]
			previous_hash = write_blockchain[i-1][1]
			previous_nonce = write_blockchain[i-1][2]
			operation_nonce_hash = (previous_operation+previous_nonce+previous_hash).encode()
			current_hash = hashlib.sha256(operation_nonce_hash).hexdigest()

		h = ""
		current_operation = write_blockchain[i][0][0]
		while len(h) == 0 or (h[len(h)-1] != '0' and h[len(h)-1] != '2'):
			nonce = ''.join(random.choice(letters) for i in range(6))
			operation_nonce = (current_operation+nonce).encode()
			h = hashlib.sha256(operation_nonce).hexdigest()

		write_blockchain[i][1] = current_hash
		write_blockchain[i][2] = nonce
		

def writeToFile():
	f = open("data_write.txt", "w")
	for i in range(len(write_blockchain)):
		f.write("operation:" + write_blockchain[i][0][0]+"\n")
		f.write("key:" + write_blockchain[i][0][1]+"\n")
		if len(write_blockchain[i][0]) > 2:
			for key, value in write_blockchain[i][0][2].items():
				f.write("value_k:" + key +"\n")
				f.write("value_v:" + value +"\n")
		f.write("hash:" + write_blockchain[i][1]+"\n")
		f.write("nonce:" + write_blockchain[i][2]+"\n")
	f.close()

def readFromFile():
	global read_blockchain
	f = open("data_write.txt", "r")
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
		read_blockchain.append(block)
		
def generateKVStore():
	global kvStore
	for i in range(len(read_blockchain)):
		val = {}
		if read_blockchain[i][0][0] == "put":
			for key, value in read_blockchain[i][0][2].items():
				val[key] = value
			kvStore[read_blockchain[i][0][1]] = val

#initialize blockchain with operation and <k,v> but no hash or nonce
write_blockchain = 	[
					[["get", "a_netid"], "", ""],
					[["put", "a_netid", {"phone_number":"111-222-3333"}], "", ""],
					[["put", "bob_netid", {"phone_number":"333-222-1111"}], "", ""],
					[["get", "bob_netid"], "", ""],
					[["get", "cat_netid"], "", ""]
					]

read_blockchain = []

kvStore = {}

generateBlockchain()

print("\n\n")
print("Blockchain generated and written to file")
for i in range(len(write_blockchain)):
	print(write_blockchain[i])

writeToFile()

print("\n\n")
print("read_blockchain before readFromFile()")
for i in range(len(read_blockchain)):
	print(read_blockchain[i])

readFromFile()

print("\n\n")
print("read_blockchain after readFromFile()")
for i in range(len(read_blockchain)):
	print(read_blockchain[i])
print("\n\n")

print("KV Store before generateKVStore()")
print(kvStore)

generateKVStore()

print("\n\n")
print("KV Store after generateKVStore()")
print(kvStore)
print("\n\n")

