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
from texttable import Texttable

# parser = argparse.ArgumentParser()
# parser.add_argument("-s", "--Stats")

# args = parser.parse_args()
# username = getpass.getuser()

def print_stats(job_list):
	rows = []
	rows.append(["JobID", "Status", "Name", "Running time"])
	for job in job_list:
		rows.append([job['job_receipt_id'], job['status'], job['name'], job['runtime']])

	table = Texttable()
	table.add_rows(rows)
	print(table.draw())
	print()




while(True):
	try:
		listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		listen_address = ('', network_params.STATS_RECV_PORT)
		listen_socket.bind(listen_address)
		listen_socket.listen(5)
		messageutils.make_and_send_message(msg_type = "STATS_JOB" ,content=None, to = network_params.SERVER_IP, port = network_params.SERVER_RECV_PORT, file_path =None, msg_socket=None)
		connection, client_address = listen_socket.accept()

		data_list = []
		data = connection.recv(network_params.BUFFER_SIZE)
			
		while data:
			data_list.append(data)
			data = connection.recv(network_params.BUFFER_SIZE)	
		data = b''.join(data_list)

		msg = pickle.loads(data)
		assert isinstance(msg, message.Message), "Can't specify the message type"

		if(msg.msg_type == "ACK_STATS_RESULT_MSG"):
			print('STATS')
			print_stats(msg.content)
			# print(msg.content)

		elif(msg.msg_type == "ERR_STATS"):
			print('Error will recovering stats')

		break


	except BrokenPipeError:
		continue

	except OSError:
		continue