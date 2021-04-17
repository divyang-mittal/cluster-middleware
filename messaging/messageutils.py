"""File with helper/util functions to support message passing.
"""

import io
import pickle
import psutil
import socket
import time

from . import message
from .network_params import BUFFER_SIZE
from ..master.job import job_parser

HEARTBEAT_REPLY_WAIT_SECONDS = 5
PORT = 5005

def send_message_with_file_path(msg_type, file_path, to, port, msg_socket=None, sender=None):
    content = job_parser.make_job(file_path)
    executable_file_path = content.executable
    make_and_send_message(msg_type, content,executable_file_path, to, msg_socket, port, sender)

def make_and_send_message(msg_type, content, file_path, to, msg_socket, port, sender=None):

    # print("XXXXYYY")
    msg = message.Message(
        msg_type=msg_type, content=content, sender=sender, file_path=file_path)
    # print(msg.sender)
    # print(msg.file)
    # print(msg.content)
    send_message(msg=msg, to=to, msg_socket=msg_socket, port=port)


def wait_send_heartbeat_to_backup(to, port, server_state):

    time.sleep(HEARTBEAT_REPLY_WAIT_SECONDS)
    make_and_send_message(
        msg_type='HEARTBEAT',
        content=server_state,
        file_path=None,
        to=to,
        msg_socket=None,
        port=port)


def wait_send_heartbeat(to, port):

    time.sleep(HEARTBEAT_REPLY_WAIT_SECONDS)
    send_heartbeat(to=to, port=port)


def send_message(msg, to, msg_socket=None, port=PORT):

    # print(msg_socket)
    if msg_socket is None:
        msg_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            print('to:', to)
            print('port:', port)
            msg_socket.connect((to, port))

        except OSError:
            # Raised if endpoint is already connected. No action is needed.
            pass

    print(msg_socket)
    msg.sender = msg_socket.getsockname()[0]
    # if msg.sender == '0.0.0.0':
    #     msg.sender = '172.31.47.176'

    print('Sender: ', msg.sender)
    msg_data = io.BytesIO(pickle.dumps(msg))

    try:
        while True:
            chunk = msg_data.read(BUFFER_SIZE)
            if not chunk:
                break
            msg_socket.send(chunk)
    except BrokenPipeError:
        # Connection with end-point broken due to node crash.
        # Do nothing as crash will be handled by crash detector and handler.
        pass

    try:
        msg_socket.shutdown(socket.SHUT_WR)
        msg_socket.close()
    except OSError:
        # Connection with end-point broken due to node crash.
        # Do nothing as crash will be handled by crash detector and handler.
        pass


def send_heartbeat(to, msg_socket=None, port=PORT, num_executing_jobs=None):

    # 'cpu': Percent CPU available, 'memory': Available memory in MB
    memory = psutil.virtual_memory().available >> 20
    if num_executing_jobs is not None:
        memory = max(300, memory - num_executing_jobs * 300)

    system_resources = {
        'cpu': 100 - psutil.cpu_percent(),
        'memory': memory,
    }

    # Construct the message with system resources and send to server
    make_and_send_message(
        msg_type='HEARTBEAT',
        content=system_resources,
        file_path=None,
        to=to,
        msg_socket=msg_socket,
        port=port)

def send_heartbeat_backup(to, msg_socket=None, port=PORT, num_executing_jobs=None):

    # 'cpu': Percent CPU available, 'memory': Available memory in MB
    memory = psutil.virtual_memory().available >> 20
    if num_executing_jobs is not None:
        memory = max(300, memory - num_executing_jobs * 300)

    system_resources = {
        'cpu': 100 - psutil.cpu_percent(),
        'memory': memory,
    }

    # Construct the message with system resources and send to server
    make_and_send_message(
        msg_type='HEARTBEAT_BACKUP',
        content=system_resources,
        file_path=None,
        to=to,
        msg_socket=msg_socket,
        port=port)