"""Store networking information for all node files to share"""

COMPUTE_NODE_RECV_PORT = 5007
COMPUTE_NODE_SEND_PORT = 5008

SUBMIT_RECV_PORT = 5009
STATS_RECV_PORT = 5010
KILL_RECV_PORT = 5011

# Buffer size for socket
BUFFER_SIZE = 1048576

# IP to be used for local system
SERVER_IP       = '52.66.150.21'
COMPUTE_NODE_IP = '65.0.31.113'
BACKUP_NODE_IP  = '52.66.28.8'

SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006

BACKUP_SEND_PORT = 5003
BACKUP_RECV_PORT = 5004

# Time (in sec) after which it's assumed that a communicating node has crashed
CRASH_ASSUMPTION_TIME = 10
