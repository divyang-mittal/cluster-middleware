import argparse
import multiprocessing as mp
import os.path
import pickle
import socket
import signal
import sys
import time
import psutil
from ctypes import c_bool

from . import message_handlers
from ..messaging import message
from ..messaging import messageutils
from ..messaging import network_params

JOB_ARRAY_SIZE = 200  # Size of shared memory array
SERVER_CHANGE_WAIT_TIME = 7


# noinspection PyUnusedLocal
def sigint_handler(signum=signal.SIGINT, frame=None):
    parent_pid = os.getpid()
    try:
        parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
        return
    children = parent.children(recursive=True)
    for process in children:
        process.send_signal(signal.SIGTERM)
    sys.exit(0)



def main():

    # Begin argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("-serverip", help="IP address of central server",
                        type=str, required=False)
    #parser.add_argument("-backupip", help="IP address of backup server",
    #                     type=str, required=True)
    parser.add_argument("-selfip", help="IP address of self",
                       type=str, required=True)
    args = vars(parser.parse_args())

    # Obtain server and backup ip's from the arguments
    server_ip = network_params.SERVER_IP
    #backup_ip = args['backupip']
    self_ip = args['selfip']

    manager = mp.Manager()
    # Set-Dict to store all executed and acknowledged executed jobs' receipt ids
    executed_jobs_receipt_ids = manager.dict()
    preempted_jobs_receipt_ids = manager.dict()
    ack_executed_jobs_receipt_ids = manager.dict()
    # Set-Dict to store receipt id of executing jobs
    executing_jobs_receipt_ids = manager.dict()
    # Dict to store execution begin time of executing
    executing_jobs_begin_times = manager.dict()
    # Dict to store execution required time of executing
    executing_jobs_required_times = manager.dict()
    # Dict to keep job_receipt_id: pid pairs
    execution_jobs_pid_dict = manager.dict()
    num_execution_jobs_recvd = 0
    # Dict to store all completed submitted jobs
    submitted_completed_jobs = manager.dict()


    # Mask SIGINT for cleanup with killing all child processes
    # signal.signal(signal.SIGINT, sigint_handler)

    # Start listening to incoming connections on COMPUTE_NODE_RECV_PORT.
    # Server and child processes connect to this socket
    msg_socket = socket.socket()
    msg_socket.bind(('', network_params.COMPUTE_NODE_RECV_PORT))
    msg_socket.listen(5)

    # Send first heartbeat to server
    messageutils.send_heartbeat(
        to=server_ip, port=network_params.SERVER_RECV_PORT)
    
    debug = server_ip

    while True:
        # Accept an incoming connection
        connection, client_address = msg_socket.accept()
        server_ip = debug
        # Receive the data
        data_list = []
        data = connection.recv(network_params.BUFFER_SIZE)
        while data:
            data_list.append(data)
            data = connection.recv(network_params.BUFFER_SIZE)
        data = b''.join(data_list)

        msg = pickle.loads(data)
        assert isinstance(
            msg, message.Message), "Received object on socket not of type " \
                                   "Message."
        
        # print(msg)
        # print("MSG CONTENT : " + str(msg.content.s))
        # print("MSG TYPE : " + str(msg.msg_type))
        # print("MSG FILE : " + str(msg.file))


        # elif msg.msg_type == 'ACK_JOB_SUBMIT':
        #     message_handlers.ack_job_submit_msg_handler(
        #         msg, shared_acknowledged_jobs_array)

        if msg.msg_type == 'I_AM_NEW_SERVER':
            # Primary server crash detected by backup server
            # switch primary and backup server ips
            
            print('backup is taking over')
            server_ip, backup_ip = msg.sender, None
            debug = msg.sender
            print(server_ip)
            time.sleep(SERVER_CHANGE_WAIT_TIME)
            message_handlers.server_crash_msg_handler(
                # shared_submitted_jobs_array,
                # shared_acknowledged_jobs_array,
                # executed_jobs_receipt_ids,
                # ack_executed_jobs_receipt_ids,
                server_ip)

 #       elif msg.sender == backup_ip:
            # Old message from a server detected to have crashed, ignore
  #          continue
        elif msg.msg_type == 'HEARTBEAT':
            # Removing pycharm's annoying unused warning for shared variable
            # noinspection PyUnusedLocal
                print("HEARTBEAT RECEIVED IN COMPUTE_NODE")
                print("printing serverip received= "+ str(server_ip))
            # shared_last_heartbeat_recv_time.value = \
                message_handlers.heartbeat_msg_handler(
                    # shared_job_array,
                    # shared_submitted_jobs_array,
                    executing_jobs_receipt_ids,
                    executed_jobs_receipt_ids,
                    executing_jobs_required_times,
                    executing_jobs_begin_times,
                    execution_jobs_pid_dict,
                    server_ip)

        elif msg.msg_type == 'JOB_EXEC':
            # TODO: See if num_execution_jobs_recvd is useful anywhere
            new_job_id = msg.content.receipt_id
            try:
                del executed_jobs_receipt_ids[new_job_id]
            except KeyError:
                pass

            num_execution_jobs_recvd += 1
            message_handlers.job_exec_msg_handler(
                current_job=msg.content,
                job_executable=msg.file,
                execution_jobs_pid_dict=execution_jobs_pid_dict,
                executing_jobs_receipt_ids=executing_jobs_receipt_ids,
                executing_jobs_begin_times=executing_jobs_begin_times,
                executing_jobs_required_times=executing_jobs_required_times,
                executed_jobs_receipt_ids=executed_jobs_receipt_ids,
                server_ip=server_ip,
                self_ip=self_ip)
            messageutils.make_and_send_message(
                msg_type='ACK_JOB_EXEC',
                content=None,
                file_path=None,
                to=server_ip,
                msg_socket=None,
                port=network_params.SERVER_RECV_PORT)

        elif msg.msg_type == 'JOB_PREEMPT_EXEC':
            print(
                'Job Preemption for job id =', msg.content[1],
                'received')
            preempted_jobs_receipt_ids[msg.content[1]] = 0
            message_handlers.job_preemption_msg_handler(
                msg=msg,
                execution_jobs_pid_dict=execution_jobs_pid_dict,
                executed_jobs_receipt_ids=executed_jobs_receipt_ids,
                executing_jobs_receipt_ids=executing_jobs_receipt_ids,
                executing_jobs_begin_times=executing_jobs_begin_times,
                executing_jobs_required_times=executing_jobs_required_times,
                server_ip=server_ip,
                self_ip=self_ip)
            messageutils.make_and_send_message(
                msg_type='ACK_JOB_PREEMPT_EXEC',
                content=None,
                file_path=None,
                to=server_ip,
                msg_socket=None,
                port=network_params.SERVER_RECV_PORT)

        elif msg.msg_type == 'KILL_JOB':
            print(
                'Job Kill for job id =', msg.content,
                'received')

            message_handlers.job_kill_msg_handler(
                msg=msg,
                execution_jobs_pid_dict=execution_jobs_pid_dict,
                # executed_jobs_receipt_ids=executed_jobs_receipt_ids,
                executing_jobs_receipt_ids=executing_jobs_receipt_ids,
                # executing_jobs_begin_times=executing_jobs_begin_times,
                # executing_jobs_required_times=executing_jobs_required_times,
                # server_ip=server_ip,
                # self_ip=self_ip
                )
            messageutils.make_and_send_message(
                msg_type='ACK_JOB_KILL_EXEC',
                content=msg.content,
                file_path=None,
                to=server_ip,
                msg_socket=None,
                port=network_params.SERVER_RECV_PORT)

        elif msg.msg_type == 'EXECUTED_JOB_TO_PARENT':
            message_handlers.executed_job_to_parent_msg_handler(
                msg, executed_jobs_receipt_ids, server_ip)

        elif msg.msg_type == 'ACK_EXECUTED_JOB':
            message_handlers.ack_executed_job_msg_handler(
                msg, ack_executed_jobs_receipt_ids)

        connection.close()


if __name__ == '__main__':
    main()
