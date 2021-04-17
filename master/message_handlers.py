import copy
import multiprocessing as mp
import time

from . import matchmaking
from ..messaging import message
from ..messaging import messageutils
from ..messaging import network_params
from . import priorityqueue
from . import serverstate


def heartbeat_from_backup_handler(received_msg):

    print("sent heartbeat to backup")
    # Send heartbeat message to backup server
    # Creating new process to wait and reply to heartbeat messages
    
    process_wait_send_heartbeat_to_backup = mp.Process(
        target=messageutils.wait_send_heartbeat_to_backup,
        args=(received_msg.sender, network_params.BACKUP_RECV_PORT, None,)
    )
    process_wait_send_heartbeat_to_backup.start()

def heartbeat_handler(compute_nodes, 
                    node_last_seen, 
                    running_jobs, 
                    job_queue,
                    job_executable, 
                    job_sender, 
                    server_state_order, 
                    backup_ip,
                    received_msg, 
                    job_receipt_id,
                    job_running_node):
   
    # Node has recovered and needs to be added back to node data structures
    # Update compute node available resources
    if received_msg.sender not in compute_nodes:
        compute_nodes[received_msg.sender] = {}

    if received_msg.sender not in running_jobs:
        running_jobs[received_msg.sender] = []

    if received_msg.sender not in node_last_seen:
        node_last_seen[received_msg.sender] = time.time()

    compute_nodes[received_msg.sender]['cpu'] = received_msg.content['cpu']
    compute_nodes[received_msg.sender]['memory'] = \
        received_msg.content['memory']
    compute_nodes[received_msg.sender]['last_seen'] = time.time()
    node_last_seen[received_msg.sender] = \
        compute_nodes[received_msg.sender]['last_seen']

    # Schedule jobs in job_queue, if possible.

    # List of jobs in job_queue that could not be scheduled
    wait_queue = priorityqueue.JobQueue()
    while not job_queue.empty():
        job = job_queue.get()
        schedule_and_send_job(
            job=job,
            executable=job_executable[job.receipt_id],
            job_queue=wait_queue,
            compute_nodes=compute_nodes,
            running_jobs=running_jobs,
            job_running_node=job_running_node)

    job_queue = wait_queue

    #Update backup server with changed server state data structures
    copy_job_queue = copy.copy(job_queue.queue)
    server_state = serverstate.ServerState(
        compute_nodes=compute_nodes,
        running_jobs=running_jobs,
        job_queue=copy_job_queue,
        job_executable=job_executable,
        job_sender=job_sender,
        job_receipt_id=job_receipt_id,
        state_order=server_state_order)

    if backup_ip is not None:
        messageutils.make_and_send_message(
            msg_type='BACKUP_UPDATE',
            content=server_state,
            file_path=None,
            to=backup_ip,
            msg_socket=None,
            port=network_params.BACKUP_RECV_PORT)

    # Send heartbeat message to computing node
    # Creating new process to wait and reply to heartbeat messages
    process_wait_send_heartbeat = mp.Process(
        target=messageutils.wait_send_heartbeat,
        args=(received_msg.sender, network_params.COMPUTE_NODE_RECV_PORT, )
    )
    process_wait_send_heartbeat.start()


def job_submit_handler(job_queue,
                       compute_nodes,
                       running_jobs,
                       received_msg,
                       job_sender,
                       job_running_node,
                       job_executable,
                       job_receipt_id,
                        backup_ip,
                        server_state_order
                ):

    job = received_msg.content
    job.sender = received_msg.sender

    # Response time records
    if job.receive_time is None:
        job.receive_time = time.time()

    job.receipt_id = job_receipt_id
    job_sender[job_receipt_id] = received_msg.sender
    job_executable[job_receipt_id] = received_msg.file

    schedule_and_send_job(
        job=job,
        executable=received_msg.file,
        job_queue=job_queue,
        compute_nodes=compute_nodes,
        running_jobs=running_jobs,
        job_running_node=job_running_node
        )

    #Update backup server with changed server state data structures
    copy_job_queue = copy.copy(job_queue.queue)

    server_state = serverstate.ServerState(
        compute_nodes=compute_nodes,
        running_jobs=running_jobs,
        job_queue=copy_job_queue,
        job_executable=job_executable,
        job_receipt_id=job_receipt_id,
        job_sender=job_sender,
        state_order=server_state_order)

    if backup_ip is not None:
        messageutils.make_and_send_message(
            msg_type='BACKUP_UPDATE',
            content=server_state,
            file_path=None,
            to=backup_ip,
            msg_socket=None,
            port=network_params.BACKUP_RECV_PORT)


    messageutils.make_and_send_message(
        msg_type='ACK_JOB_SUBMIT',
        content=job.receipt_id,
        file_path=None,
        to="127.0.0.1",
        port= network_params.SUBMIT_RECV_PORT,
        msg_socket=None)


def executed_job_handler(job_queue,
                         compute_nodes,
                         running_jobs,
                         job_sender,
                         job_executable,
                         server_state_order,
                         backup_ip,
                         job_receipt_id,
                         received_msg,
                         job_running_node):

    executed_job = received_msg.content
    try:
        running_jobs[received_msg.sender].remove(executed_job)
    except ValueError:
        # Executed job was not in running jobs, ie. message is from double
        # job scheduling due to server crash. Send ACK and ignore message.
        messageutils.make_and_send_message(
            msg_type='ACK_EXECUTED_JOB',
            content=executed_job.receipt_id,
            file_path=None,
            to=received_msg.sender,
            port=network_params.SERVER_SEND_PORT,
            msg_socket=None)

        return job_queue

    if executed_job.completed:

        del job_sender[executed_job.receipt_id]
        del job_executable[executed_job.receipt_id]

        # Schedule jobs waiting in job_queue

        # List of jobs in job_queue that could not be scheduled
        wait_queue = priorityqueue.JobQueue()
        while not job_queue.empty():
            job = job_queue.get()
            schedule_and_send_job(
                job=job,
                executable=job_executable[job.receipt_id],
                job_queue=wait_queue,
                compute_nodes=compute_nodes,
                running_jobs=running_jobs,
                job_running_node=job_running_node
                )

        job_queue = wait_queue

    # Preempted job has been returned for rescheduling
    else:
        schedule_and_send_job(
            job=executed_job,
            executable=job_executable[executed_job.receipt_id],
            job_queue=job_queue,
            compute_nodes=compute_nodes,
            running_jobs=running_jobs,
            job_running_node=job_running_node)

    #Update backup server with changed server state data structures
    copy_job_queue = copy.copy(job_queue.queue)
    server_state = serverstate.ServerState(
        compute_nodes=compute_nodes,
        running_jobs=running_jobs,
        job_queue=copy_job_queue,
        job_executable=job_executable,
        job_sender=job_sender,
        job_receipt_id=job_receipt_id,
        state_order=server_state_order)

    if backup_ip is not None:
        messageutils.make_and_send_message(
            msg_type='BACKUP_UPDATE',
            content=server_state,
            file_path=None,
            to=backup_ip,
            msg_socket=None,
            port=network_params.BACKUP_RECV_PORT)

    messageutils.make_and_send_message(
        msg_type='ACK_EXECUTED_JOB',
        content=executed_job.receipt_id,
        file_path=None,
        to=received_msg.sender,
        port=network_params.SERVER_SEND_PORT,
        msg_socket=None)

    return job_queue


def ack_ignore_handler():
    """Handler function for ACK messages. Ignores received ack messages.
    """
    pass



# Helper functions

def schedule_and_send_job(job,
                          executable,
                          job_queue,
                          compute_nodes,
                          running_jobs,
                          job_running_node
                        ):

    node_for_job, preempt_job = matchmaking.matchmaking(
        job=job, compute_nodes=compute_nodes, running_jobs=running_jobs)
    # node_for_job = "127.0.0.1"
    # Job cannot be scheduled at the moment
    if node_for_job is None:
        print('Queued')
        job_queue.put(job)
        print(job_queue.queue)
        return

    # Response time records
    if job.first_response is None:
        job.first_response = time.time()

    if preempt_job is not None:
        job_exec_msg = message.Message(
            msg_type='JOB_PREEMPT_EXEC',
            content=(job, preempt_job.receipt_id),
            file=executable)
    else:
        job_exec_msg = message.Message(
            msg_type='JOB_EXEC', content=job, file=executable)

    # job_exec_msg = message.Message(
            # msg_type='JOB_EXEC', content=job, file=executable)


    job_running_node[job.receipt_id] = node_for_job
    messageutils.send_message(
        msg=job_exec_msg, to=node_for_job, port=network_params.COMPUTE_NODE_RECV_PORT)

    print('SENDING JOB_EXEC:', job.submission_id, job.receipt_id)


def kill_job_handler(
                            job_queue,
                            compute_nodes,
                            job_receipt_id,
                            running_jobs,
                            job_executable,
                            job_sender,
                            job_running_node,
                            backup_ip,
                            server_state_order
                    ):

    # job = received_msg.content
    kill_job_msg = message.Message(
        msg_type = 'KILL_JOB', content=job_receipt_id)
    

    print('Entered kill job handler')
    is_avail = 0
    if job_receipt_id not in job_running_node:
        print('Not in job running node')
        temp_queue = job_queue.queue
        for job in temp_queue:
            if job[1].job_receipt_id==job_receipt_id:
                is_avail = 1
                temp_queue.remove(job)
        job_queue.queue = temp_queue
        #modified_queue = priorityqueue.JobQueue()
        #for job in temp_queue:
        #    modified_queue.push(job)
        if is_avail == 0:
            messageutils.make_and_send_message(
                msg_type='ERR_JOB_KILL',
                content=None,
                file_path=None,
                to="127.0.0.1",
                port= network_params.KILL_RECV_PORT,
                msg_socket=None)
    else:
        print('Running job')
        running_node = job_running_node[job_receipt_id]

        del job_sender[job_receipt_id]
        del job_executable[job_receipt_id]
        
        for job in running_jobs[running_node]:
            if job.receipt_id==job_receipt_id:
                running_jobs[running_node].remove(job)
        #modified_queue = job_queue
        messageutils.send_message(msg=kill_job_msg,to=running_node, port=network_params.COMPUTE_NODE_RECV_PORT)
        print('SENDING KILL_JOB:', job_receipt_id)
    
     #Update backup server with changed server state data structures
    copy_job_queue = copy.copy(job_queue.queue)
    server_state = serverstate.ServerState(
        compute_nodes=compute_nodes,
        running_jobs=running_jobs,
        job_queue=copy_job_queue,
        job_executable=job_executable,
        job_sender=job_sender,
        job_receipt_id=job_receipt_id,
        state_order=server_state_order)
    
    if backup_ip is not None:
        messageutils.make_and_send_message(
            msg_type='BACKUP_UPDATE',
            content=server_state,
            file_path=None,
            to=backup_ip,
            msg_socket=None,
            port=network_params.BACKUP_RECV_PORT)
    
    print('Returned from handler')
    
    return job_queue

def ack_job_kill_handler(
                        content
                    ):
    ack_job_kill_msg = message.Message(
        msg_type='ACK_JOB_KILL_SUCCESS', content=content )                
    messageutils.send_message(msg=ack_job_kill_msg, to='127.0.0.1', port=network_params.KILL_RECV_PORT)


def stats_job_handler(
                        running_jobs,
                        job_queue
                    ):

    # job = received_msg.content
    # stats_job_msg = message.Message(
    #     msg_type = 'STATS_JOB', content=job, file=executable)
    # running_node = job_running_node[job.receipt_id]
    
    # messageutils.send_message(msg=stats_job_msg,to=running_node, port=network_params.SERVER_SEND_PORT)
    # print('SENDING STATS_JOB:', job.receipt_id)
    # content = running_jobs
    # content = {'running_jobs' : running_jobs, 'job_queue' : job_queue }
    # content = { 'running_jobs' : ["job1", "job2"], 'job_queue' : ["job3", "job4"]}

    jobs_status = []

    for node_jobs in running_jobs.values():
        for cur_job in node_jobs:
            temp_job_status = {}
            temp_job_status['status'] = 'running'
            temp_job_status['job_receipt_id'] = cur_job.receipt_id
            temp_job_status['name'] = cur_job.name
            temp_job_status['runtime'] = time.time() - cur_job.time
            # print("running time : " + str(cur_job.time_run))
            jobs_status.append(temp_job_status)

    temp_queue = job_queue.queue
    print(temp_queue)
    print(job_queue.queue)

    for x in temp_queue:
        cur_job = x[1]
        temp_job_status = {}
        temp_job_status['status'] = 'queue'
        temp_job_status['job_receipt_id'] = cur_job.receipt_id
        temp_job_status['name'] = cur_job.name
        temp_job_status['runtime'] = 0
        # temp_job_status['runtime'] = cur_job.time_run
        jobs_status.append(temp_job_status)

    ack_stats_result_msg = message.Message(
        msg_type='ACK_STATS_RESULT_MSG', content=jobs_status)
    messageutils.send_message(msg=ack_stats_result_msg, to='127.0.0.1', port=network_params.STATS_RECV_PORT)

def node_crash_handler(compute_nodes,
                       running_jobs,
                       job_queue,
                       job_executable,
                       node_last_seen,
                       received_msg,
                       job_running_node):

    crashed_nodes = received_msg.content
    pre_crash_running_jobs = copy.deepcopy(running_jobs)

    for node_id in crashed_nodes:
        del compute_nodes[node_id]
        del running_jobs[node_id]
        del node_last_seen[node_id]

    for node_id, running_jobs_list in pre_crash_running_jobs.items():
        if node_id in crashed_nodes:
            for job in running_jobs_list:
                schedule_and_send_job(
                    job=job,
                    executable=job_executable[job.receipt_id],
                    job_queue=job_queue,
                    compute_nodes=compute_nodes,
                    running_jobs=running_jobs,
                    job_running_node=job_running_node)