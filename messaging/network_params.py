"""Store networking information for all node files to share"""

COMPUTE_NODE_RECV_PORT = 5008
COMPUTE_NODE_SEND_PORT = 5009

SUBMIT_RECV_PORT = 5007
STATS_RECV_PORT = 1002
KILL_RECV_PORT = 1003

# Buffer size for socket
BUFFER_SIZE = 1048576

# IP to be used for local system
SERVER_IP = '127.0.0.1'
SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006

# Time (in sec) after which it's assumed that a communicating node has crashed
CRASH_ASSUMPTION_TIME = 10
