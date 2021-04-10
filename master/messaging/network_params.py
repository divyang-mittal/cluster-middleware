"""Store networking information for all node files to share"""

CLIENT_RECV_PORT = 5005
CLIENT_SEND_PORT = 5006

SUBMIT_RECV_PORT = 1001
STATS_RECV_PORT = 1002
KILL_RECV_PORT = 1003

# Buffer size for socket
BUFFER_SIZE = 1048576

# IP to be used for local system
SERVER_IP = '127.0.0.1'
SERVER_PORT = 8081

# Time (in sec) after which it's assumed that a communicating node has crashed
CRASH_ASSUMPTION_TIME = 10
