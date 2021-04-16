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
parser.add_argument("-k", "--Kill")

args = parser.parse_args()
# username = getpass.getuser()


while(True):
	try:
		listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		listen_address = ('', network_params.KILL_RECV_PORT)
		listen_socket.bind(listen_address)
		listen_socket.listen(5)
		messageutils.make_and_send_message(msg_type = "KILL_JOB" ,content = args.Kill, to = network_params.SERVER_IP, port = network_params.SERVER_RECV_PORT, file_path =None, msg_socket=None)
		print('here')
		connection, client_address = listen_socket.accept()

		data_list = []
		data = connection.recv(network_params.BUFFER_SIZE)
			
		while data:
			data_list.append(data)
			data = connection.recv(network_params.BUFFER_SIZE)	
		data = b''.join(data_list)

		msg = pickle.loads(data)
		assert isinstance(msg, message.Message), "Can't specify the message type"

		if(msg.msg_type == 'ACK_JOB_KILL_SUCCESS'):
			print("Job Killed \n")
			print("Job Id : " + str(msg.content))


		elif(msg.msg_type == 'ERR_JOB_KILL'):
			print("Job could not be killed \n")
		
		break



	except BrokenPipeError:
		continue

	except OSError:
		continue