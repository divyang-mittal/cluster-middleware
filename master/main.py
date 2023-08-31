import argparse
import multiprocessing as mp
import os
import os.path
import pickle
import select
import socket
import time
import copy

from . import message_handlers
from ..messaging import message
from ..messaging import messageutils
from ..messaging import network_params
from . import priorityqueue
from .job import job_parser

# SERVER_SEND_PORT = 5005
# SERVER_RECV_PORT = 5006
BUFFER_SIZE = 1048576

SERVER_START_WAIT_TIME = 5  # seconds
CRASH_ASSUMPTION_TIME = 20  # seconds
CRASH_DETECTOR_SLEEP_TIME = 5  # seconds

SERVER_START_WAIT_TIME = 5  # seconds
BACKUP_SERVER_STATE_PATH = '/home/ubuntu/sharedfolder/backup_state.pkl'


def print_welcome_message():
    """Print a welcome message read from prompt_welcome file to stdout."""
    # prompt_welcome_filepath = \
    #     os.path.dirname(os.path.realpath(__file__)) + "CLUSTER_MIDDLEWARE"
    # with open(prompt_welcome_filepath, 'r') as file:
    #     print(file.read())
    print("WELCOME TO MASTER NODE")


def detect_node_crash(node_last_seen, server_ip):

    while True:
        time.sleep(CRASH_DETECTOR_SLEEP_TIME)
        print('Crash Check')

        current_time = time.time()
        crashed_nodes = set()
        for node_id, last_seen_time in node_last_seen.items():
            time_since_last_heartbeat = current_time - last_seen_time
            if time_since_last_heartbeat > CRASH_ASSUMPTION_TIME:
                crashed_nodes.add(node_id)

        # Make and send a crash message to main process which is listening
        # on SERVER_RECV_PORT for incoming messages.
        if len(crashed_nodes) != 0:
            print('NODE CRASHED')
            print(crashed_nodes)
            messageutils.make_and_send_message(msg_type='NODE_CRASH',
                                               content=crashed_nodes,
                                               file_path=None,
                                               to=server_ip,
                                               msg_socket=None,
                                               port=network_params.SERVER_RECV_PORT)


def main():
    # """Get server ip, backup ip, listen for messages and manage jobs.
    # """
    parser = argparse.ArgumentParser(description='Set up central server.')
    
    backup_ip = None 

    print_welcome_message()

    compute_nodes = {}  # {node_id: status}
    job_queue = priorityqueue.JobQueue()
    running_jobs = {}  # {node_id: [list of jobs]}
    job_executable = {}  # {job_id: executable}
    job_running_node = {} #{job_id: running_node}
    job_sender = {}  # {job_id: sender}

    # In case of backup server taking over on original central server crash
    # gives backup process enough time to terminate
    time.sleep(SERVER_START_WAIT_TIME)
    
    job_receipt_id = 0  # Unique ID assigned to each job from server.
    server_state_order = 0  # Sequence ordering of ServerState sent to backup.
    manager = mp.Manager()
    node_last_seen = manager.dict()  # {node_id: last_seen_time}
    
    # Initialize current server state from backup snapshot
    # Used in case primary backup is taking over as central server
    if os.path.isfile(BACKUP_SERVER_STATE_PATH):
        # server_ip = network_params.BACKUP_NODE_IP
        with open(BACKUP_SERVER_STATE_PATH, 'rb') as backup_server_state:
            server_state = pickle.load(backup_server_state)

        compute_nodes = server_state.compute_nodes
        for node_id, _ in compute_nodes.items():
            print(node_id)
            node_last_seen[node_id] = time.time()

        running_jobs = server_state.running_jobs
        job_receipt_id = server_state.job_receipt_id
        job_sender = server_state.job_sender
        job_executable = server_state.job_executable
        job_queue = priorityqueue.JobQueue()
        for job in server_state.job_queue:
            job_queue.put(job)

    process_crash_detector = mp.Process(
        target=detect_node_crash, args=(node_last_seen, '127.0.0.1',))
    process_crash_detector.start()


    # Creates a TCP/IP socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Binds the socket to the port
    server_address = ('', network_params.SERVER_RECV_PORT)
    print('Starting up on %s port %s' % server_address)
    server.bind(server_address)
    server.listen(5)

    # Sockets for reading and writing
    inputs = [server]
    outputs = []

    while inputs:

        # Wait for at least one of the sockets to be ready for processing
        readable, _, _ = select.select(inputs, outputs, inputs)

        # Handle inputs
        for msg_socket in readable:

            if msg_socket is server:
                # A "readable" server socket is ready to accept a connection
                connection, client_address = msg_socket.accept()
                inputs.append(connection)
                # print("XXXX")
            else:
                data = msg_socket.recv(BUFFER_SIZE)
                if data:

                    data_list = []
                    while data:
                        data_list.append(data)
                        data = msg_socket.recv(BUFFER_SIZE)
                    data = b''.join(data_list)

                    msg = pickle.loads(data)
                    assert isinstance(msg, message.Message), \
                        "Received object on socket not of type Message."

                    if msg.msg_type == 'HEARTBEAT_BACKUP':
                        backup_ip = msg.sender
                        message_handlers.heartbeat_from_backup_handler(
                                received_msg=msg)

                    elif msg.msg_type == 'HEARTBEAT':
                        print("\n\n\n")
                        print(compute_nodes)
                        print("\n\n\n")

                        message_handlers.heartbeat_handler(
                            compute_nodes=compute_nodes,
                            node_last_seen=node_last_seen,
                            running_jobs=running_jobs,
                            job_queue=job_queue,
                            job_sender=job_sender,
                            job_executable=job_executable,
                            job_receipt_id=job_receipt_id,
                            backup_ip=backup_ip,
                            server_state_order=server_state_order,
                            received_msg=msg,
                            job_running_node=job_running_node)

                    elif msg.msg_type == 'JOB_SUBMIT':
                        job_receipt_id += 1
                        server_state_order += 1
                        
                        try:
                            message_handlers.job_submit_handler(
                                job_queue=job_queue,
                                compute_nodes=compute_nodes,
                                running_jobs=running_jobs,
                                job_sender=job_sender,
                                job_running_node=job_running_node,
                                job_executable=job_executable,
                                received_msg=msg,
                                job_receipt_id=job_receipt_id,
                                backup_ip=backup_ip,
                                server_state_order=server_state_order
                            )
                        except:
                            messageutils.make_and_send_message(
                            msg_type='ERR_JOB_SUBMIT',
                            content=None,
                            file_path=None,
                            to="127.0.0.1",
                            port= network_params.SUBMIT_RECV_PORT,
                            msg_socket=None)

                    elif msg.msg_type == 'EXECUTED_JOB':
                        server_state_order += 1
                        print(
                            'RECV: ' + str(msg.content) + ' ' +
                            str(msg.content.completed))
                        job_queue = message_handlers.executed_job_handler(
                            job_queue=job_queue,
                            compute_nodes=compute_nodes,
                            job_receipt_id=job_receipt_id,
                            running_jobs=running_jobs,
                            job_running_node=job_running_node,
                            job_sender=job_sender,
                            job_executable=job_executable,
                            backup_ip=backup_ip,
                            server_state_order=server_state_order,
                            received_msg=msg)

                    elif msg.msg_type == 'KILL_JOB':
                        print('__________')
                        print('msg received\n\n')
                        try:
                            job_queue = message_handlers.kill_job_handler(
                                job_queue=job_queue,
                                compute_nodes=compute_nodes,
                                job_receipt_id=int(msg.content),
                                running_jobs=running_jobs,
                                job_executable=job_executable,
                                job_sender=job_sender,
                                job_running_node=job_running_node,
                                backup_ip=backup_ip,
                                server_state_order=server_state_order
                            )
                        except:
                            print('__________')
                            print('Error')
                            messageutils.make_and_send_message(
                                msg_type='ERR_JOB_KILL',
                                content=None,
                                file_path=None,
                                to="127.0.0.1",
                                port= network_params.KILL_RECV_PORT,
                                msg_socket=None)                            

                    
                        
                    elif msg.msg_type == 'STATS_JOB':
                        try:
                            print("STATS RECEIVED IN SERVER")
                            message_handlers.stats_job_handler(
                                running_jobs= running_jobs,
                                job_queue= job_queue,  
                            )
                        except:
                            messageutils.make_and_send_message(
                                msg_type='ERR_STATS',
                                content=None,
                                file_path=None,
                                to="127.0.0.1",
                                port= network_params.STATS_RECV_PORT,
                                msg_socket=None)

                    elif msg.msg_type == 'ACK_JOB_EXEC':
                        message_handlers.ack_ignore_handler()

                    elif msg.msg_type == 'ACK_JOB_EXEC_PREEMPT':
                        message_handlers.ack_ignore_handler()

                    elif msg.msg_type == 'ACK_SUBMITTED_JOB_COMPLETION':
                        message_handlers.ack_ignore_handler()

                    elif msg.msg_type == 'ACK_JOB_KILL_EXEC':
                        print("KILLED JOB")
                        message_handlers.ack_job_kill_handler(
                            content = msg.content
                        )


                    elif msg.msg_type == 'NODE_CRASH':
                        message_handlers.node_crash_handler(
                            received_msg=msg,
                            compute_nodes=compute_nodes,
                            running_jobs=running_jobs,
                            job_queue=job_queue,
                            node_last_seen=node_last_seen,
                            job_executable=job_executable,
                            job_running_node=job_running_node)
                else:
                    inputs.remove(msg_socket)
                    msg_socket.close()

if __name__ == '__main__':
    main()