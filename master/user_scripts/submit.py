import io
import pickle
import socket
import psutil
import argparse
import sys
sys.path.append("..")
from ...messaging import network_params
from ...messaging import message 
from ...messaging import messageutils
import getpass

parser = argparse.ArgumentParser()
parser.add_argument("-j", "--JobPath")

args = parser.parse_args()
username = getpass.getuser()
print(args.JobPath)


listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listen_address = ('', network_params.SUBMIT_RECV_PORT)
listen_socket.bind(listen_address)
listen_socket.listen(5)

messageutils.send_message_with_file_path(msg_type = "JOB_SUBMIT", file_path = args.JobPath, to = network_params.SERVER_IP, port = network_params.SERVER_RECV_PORT)
		
connection, client_address = listen_socket.accept()

data_list = []
data = connection.recv(network_params.BUFFER_SIZE)
			
# while data:
data_list.append(data)
data = connection.recv(network_params.BUFFER_SIZE)	
data = b''.join(data_list)

msg = pickle.loads(data)
assert isinstance(msg, message.Message), "Can't specify the message type"

if(msg.msg_type == 'ACK_JOB_SUBMIT'):
	print(data)
	print("JOB ID: "+ str(msg.content))







# while(True):
# 	try:
		# listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		# listen_address = ('', network_params.SUBMIT_RECV_PORT)
		# listen_socket.bind(listen_address)
		# listen_socket.listen(5)
		# while True:
		# 	connection, client_address = listen_socket.accept()

		# 	data_list = []
		# 	data = connection.recv(network_params.BUFFER_SIZE)
			
		# 	while data:
		# 		data_list.append(data)
		# 		data = connection.recv(network_params.BUFFER_SIZE)	
		# 		data = b''.join(data_list)

		# 	msg = pickle.loads(data)
		# 	assert isinstance(msg, message.Message), "Can't specify the message type"

		# 	if(msg.msg_type == 'ACK_JOB_SUBMIT'):
		# 		print("JOB ID: "+ msg.content)
		# 		break

	# 	break


	# except BrokenPipeError:
	# 	continue

	# except OSError:
	# 	continue