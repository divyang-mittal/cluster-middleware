import argparse
import multiprocessing as mp
import pickle
import socket
import time

from . import message_handlers
from ..messaging import message
from ..messaging import messageutils
from ..messaging import network_params

BUFFER_SIZE = 1048576
CRASH_ASSUMPTION_TIME = 20  # seconds
CRASH_DETECTOR_SLEEP_TIME = 5  # seconds


def detect_server_crash(server_last_seen_time, backup_ip):

    while True:
        time.sleep(CRASH_DETECTOR_SLEEP_TIME)
        print('CHECKING CRASH')

        current_time = time.time()
        time_since_last_heartbeat = current_time - server_last_seen_time.value
        if time_since_last_heartbeat > CRASH_ASSUMPTION_TIME:
            print('NODE CRASHED BACKUP')
            messageutils.make_and_send_message(msg_type='SERVER_CRASH',
                                               content=None,
                                               file_path=None,
                                               to=backup_ip,
                                               msg_socket=None,
                                               port=network_params.BACKUP_RECV_PORT)


def main():
    parser = argparse.ArgumentParser(description='Set up central server.')
    parser.add_argument(
        '--server-ip',
        required=True,
        help='IP address of central server.')
    parser.add_argument(
        '--backup-ip',
        required=True,
        help='IP address of primary backup server (this node).')
    args = parser.parse_args()

    server_ip = args.server_ip
    backup_ip = args.backup_ip
    server_state = None

    # Shared variable storing time of last heartbeat receipt, of type float
    shared_last_heartbeat_recv_time = mp.Value('d', time.time())

    # Creating new process for server crash detection
    process_server_crash_detection = mp.Process(
        target=detect_server_crash,
        args=(shared_last_heartbeat_recv_time, backup_ip, )
    )
    process_server_crash_detection.daemon = 1
    process_server_crash_detection.start()

    # Start listening to incoming connections on CLIENT_RECV_PORT.
    # Server and child processes connect to this socket
    msg_socket = socket.socket()
    msg_socket.bind(('', network_params.BACKUP_RECV_PORT))
    msg_socket.listen(5)
    
    print("first_heartbeat to server")
    # Send first heartbeat to server
    messageutils.send_heartbeat_backup(to=server_ip, port=network_params.SERVER_RECV_PORT)

    while True:
        # Accept an incoming connection
        connection, client_address = msg_socket.accept()

        # Receive the data
        data_list = []
        data = connection.recv(BUFFER_SIZE)
        while data:
            data_list.append(data)
            data = connection.recv(BUFFER_SIZE)
        data = b''.join(data_list)

        msg = pickle.loads(data)
        assert isinstance(msg, message.Message), "Received object on socket " \
                                                 "not of type Message."
        print(msg)

        if msg.msg_type == 'HEARTBEAT':
            shared_last_heartbeat_recv_time.value = time.time()
            message_handlers.heartbeat_handler(received_msg=msg)

        elif msg.msg_type == 'BACKUP_UPDATE':
            server_state = message_handlers.backup_update_handler(
                previous_server_state=server_state,
                received_msg=msg)

        elif msg.msg_type == 'SERVER_CRASH':
            message_handlers.server_crash_handler(
                server_state=server_state,
                crashed_server_ip=server_ip,
                backup_ip=backup_ip,
                child_pid=process_server_crash_detection.pid,
                socket_to_close=msg_socket)


if __name__ == '__main__':
    main()
