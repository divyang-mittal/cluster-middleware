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
    print("heartbeat of backup sent to master")
    messageutils.send_heartbeat_backup(to=received_msg.sender, port=network_params.SERVER_RECV_PORT)


def backup_update_handler(received_msg, previous_server_state):
    
    server_state = received_msg.content
    if previous_server_state is not None and \
            server_state.state_order < previous_server_state.state_order:
        server_state = previous_server_state

    with open(BACKUP_SERVER_STATE_PATH, 'wb') as server_state_file:
        pickle.dump(server_state, server_state_file)

    return server_state


def server_crash_handler(server_state, crashed_server_ip, backup_ip, child_pid,
                         socket_to_close):
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
        'python3 -m cluster-middleware.master.main')
    os.system(start_server_command)
    sys.exit()
