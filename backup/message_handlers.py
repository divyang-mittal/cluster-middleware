"""Message handlers for all messages received at backup.

Includes handlers for:
    * Heartbeat message from server.
    * Backup update message from server
    * Notification about server crash.
"""

import os
import pickle
import sys
import signal

from ..messaging import messageutils
from ..messaging import network_params



BACKUP_SERVER_STATE_PATH = '/home/ubuntu/sharedfolder/backup_state.pkl'


def heartbeat_handler(received_msg):
    """Handler function for HEARTBEAT messages from server.

    Responds by sending a heartbeat to the server.

    :param received_msg: message, received message.
    :return: ServerState object received from central server.
    """
    print("heartbeat of backup sent to master")
    messageutils.send_heartbeat_backup(to=received_msg.sender, port=network_params.SERVER_RECV_PORT)


def backup_update_handler(received_msg, previous_server_state):
    """Handler function for BACKUP_UPDATE messages from server

    Saves ServerState to pickle file for use when backup needs to take over as
    central server.

    :param received_msg: message, received message
    :param previous_server_state: previous state of the server
    :return: ServerState updated state of the server
    """
    server_state = received_msg.content
    if previous_server_state is not None and \
            server_state.state_order < previous_server_state.state_order:
        server_state = previous_server_state

    with open(BACKUP_SERVER_STATE_PATH, 'wb') as server_state_file:
        pickle.dump(server_state, server_state_file)

    return server_state


def server_crash_handler(server_state, crashed_server_ip, backup_ip, child_pid,
                         socket_to_close):
    """Handler function for SERVER_CRASH messages from crash detector.

    Informs all computing nodes about new server, takes over as central server,
    and kills backup process.

    :param server_state: ServerState with last known state of central server.
    :param crashed_server_ip: String with IP address of crashed central server.
    :param backup_ip: String with IP address of primary backup (this node).
    :param child_pid: pid of child process
    :param socket_to_close: socket to be closed
    """
    os.kill(child_pid, signal.SIGTERM)
    for node_id, status in server_state.compute_nodes.items():
        messageutils.make_and_send_message(
            msg_type='I_AM_NEW_SERVER',
            content=None,
            file_path=None,
            to=node_id,
            msg_socket=None,
            port=network_params.COMPUTE_NODE_RECV_PORT)

    socket_to_close.close()
    start_server_command = (
        'python3 -m cluster-middleware.master.main ')
    os.system(start_server_command)
    sys.exit()
