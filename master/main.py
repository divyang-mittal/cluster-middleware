"""Script to set up and run central server.

    Responsible for communication with computing nodes, primary backup, job
    scheduling, load balancing, job-node matchmaking decisions etc.

    Messages received from the node:
        - JOB_SUBMIT: The node sends the job to be submitted for execution
            in this message along with the executable file. The server tries to
            schedule the job if possible, else adds it to the job queue.

        - EXECUTED_JOB: This message tells the server that the job given to the
            node has either been completed or preempted(with the help of a
            completed flag). If the job has been completed, the server removes
            it from the job_queue and informs the node which has submitted the
            job. Also, it tries to schedule the jobs in the job queue. On the
            other hand, if the job is a preempted one, the server tries to
            schedule it again.

        - ACK_SUBMITTED_JOB_COMPLETION: The server ignores this.

        - ACK_JOB_EXEC: The server ignores this.

        - ACK_JOB_EXEC_PREEMPT: The server ignores this.

    Messages sent to node:
        - ACK_JOB_SUBMIT: Server sends this message on receiving a JOB_SUBMIT
            message from the node. Includes job's submission id in
            message's content field.

        - ACK_EXECUTED_JOB: Sent in response to EXECUTED_JOB message.

        - JOB_EXEC: Sent by server requesting execution of a job on the node.
            Has job object in content, and executable in file field.

        - JOB_PREEMPT_EXEC: Sent by server requesting preemption of an executing
            job, and execution of a new job. Has (new_job,
            job_to_preempt receipt id) in content, and executable file of new
            job in file.

        - SUBMITTED_JOB_COMPLETION: Server, on receiving EXECUTED_JOB message
            from a node, checks job's 'completed' attribute, and if True,
            sends SUBMITTED_JOB_COMPLETION to submitting node.
"""

import argparse
import multiprocessing as mp
import os
import os.path
import pickle
import select
import socket
import time


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


def print_welcome_message():
    """Print a welcome message read from prompt_welcome file to stdout."""
    # prompt_welcome_filepath = \
    #     os.path.dirname(os.path.realpath(__file__)) + "CLUSTER_MIDDLEWARE"
    # with open(prompt_welcome_filepath, 'r') as file:
    #     print(file.read())
    print("WELCOME TO MASTER NODE")


def main():
    # """Get server ip, backup ip, listen for messages and manage jobs.
    # """
    # parser = argparse.ArgumentParser(description='Set up central server.')
    # parser.add_argument(
    #     '--server-ip',
    #     required=True,
    #     help='IP address of central server (this node).')
    # parser.add_argument(
    #     '--backup-ip',
    #     required=True,
    #     help='IP address of primary backup server.')
    # args = parser.parse_args()
    # backup_ip = args.backup_ip
    # server_ip = args.server_ip

    print_welcome_message()

    compute_nodes = {}  # {node_id: status}
    job_queue = priorityqueue.JobQueue()
    running_jobs = {}  # {node_id: [list of jobs]}
    job_executable = {}  # {job_id: executable}
    job_running_node = {} #{job_id: running_node}
    job_sender = {}  # {job_id: sender}

    job_receipt_id = 0  # Unique ID assigned to each job from server.
    # server_state_order = 0  # Sequence ordering of ServerState sent to backup.

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

                    # print("XYX")
                    # print(msg.sender)
                    # print("XYX")
                    # print(msg.file)
                    # print("XYZ")

                    if msg.msg_type == 'JOB_SUBMIT':
                        job_receipt_id += 1
                        # server_state_order += 1
                        
                        message_handlers.job_submit_handler(
                            job_queue=job_queue,
                            compute_nodes=compute_nodes,
                            running_jobs=running_jobs,
                            job_sender=job_sender,
                            job_running_node=job_running_node,
                            job_executable=job_executable,
                            received_msg=msg,
                            job_receipt_id=job_receipt_id,
                            # backup_ip=backup_ip,
                            # server_state_order=server_state_order
                        )

                    elif msg.msg_type == 'EXECUTED_JOB':
                        # server_state_order += 1
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
                            # backup_ip=backup_ip,
                            # server_state_order=server_state_order,
                            received_msg=msg)

                    elif msg.msg_type == 'KILL_JOB':
                        message_handlers.kill_job_handler(
                            received_msg=msg,
                            running_jobs=running_jobs,
                            job_executable=job_executable,
                            job_sender=job_sender,
                            job_running_node=job_running_node
                        )
                    
                        
                    # elif msg.msg_type == 'JOB_STATS':


                    elif msg.msg_type == 'ACK_JOB_EXEC':
                        message_handlers.ack_ignore_handler()

                    elif msg.msg_type == 'ACK_JOB_EXEC_PREEMPT':
                        message_handlers.ack_ignore_handler()

                    elif msg.msg_type == 'ACK_SUBMITTED_JOB_COMPLETION':
                        message_handlers.ack_ignore_handler()
                else:
                    inputs.remove(msg_socket)
                    msg_socket.close()


if __name__ == '__main__':
    main()