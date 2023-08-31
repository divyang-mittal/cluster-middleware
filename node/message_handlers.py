import errno
import os
import pickle
import signal
import time

from .job import job_execution
from ..messaging import messageutils
from ..messaging import network_params

SUBMITTED_JOB_DIRECTORY_PREFIX = './sharedfolder/submit_job'
EXECUTING_JOB_DIRECTORY_PREFIX = './sharedfolder/exec_job'
JOB_PICKLE_FILE = '/sharedfolder/job.pickle'


def heartbeat_msg_handler(#shared_job_array,
                        #   shared_submitted_jobs_array,
                          executing_jobs_receipt_ids,
                          executed_jobs_receipt_ids,
                          executing_jobs_required_times,
                          executing_jobs_begin_times,
                          execution_jobs_pid_dict,
                          server_ip):

    # Record receive time of heartbeat message
    heartbeat_recv_time = time.time()

    itr = 0
    for job_id in set(executing_jobs_receipt_ids.keys()) - \
                  set(executed_jobs_receipt_ids.keys()):
        if itr >= 2:
            break
        # time_run = time.time() - executing_jobs_begin_times[job_id]
        time_run = time.time() - executing_jobs_begin_times[job_id]
        if time_run >= executing_jobs_required_times[job_id]:
            try:
                executing_child_pid = execution_jobs_pid_dict[job_id]
                os.kill(executing_child_pid, signal.SIGTERM)
                time.sleep(3)
                itr += 1
            except OSError as err:
                if err.errno == errno.ESRCH:
                    # ESRCH: child process no longer exists
                    resend_executed_job_msg(job_id, server_ip)
            finally:
                # Only for safety, not really required.
                executed_jobs_receipt_ids[job_id] = 0

    # Send heartbeat back to the server
    num_executing_jobs = len(executing_jobs_receipt_ids.keys())
    messageutils.send_heartbeat(
        to=server_ip,
        port=network_params.SERVER_RECV_PORT,
        num_executing_jobs=num_executing_jobs)

    return heartbeat_recv_time


def job_exec_msg_handler(current_job, job_executable,
                         execution_jobs_pid_dict,
                         executing_jobs_receipt_ids,
                         executing_jobs_begin_times,
                         executing_jobs_required_times,
                         executed_jobs_receipt_ids,
                         server_ip, self_ip):

    try:
        _ = executing_jobs_receipt_ids[current_job.receipt_id]
        return
    except KeyError:
        pass

    # Make new job directory
    current_job_directory = '%s%d' % (EXECUTING_JOB_DIRECTORY_PREFIX,
                                      current_job.receipt_id)
    if not os.path.exists(current_job_directory):
        os.makedirs(current_job_directory)

    # Book-keeping
    job_id = current_job.receipt_id
    executing_jobs_required_times[job_id] = current_job.time_required - current_job.time_run

    executing_jobs_receipt_ids[current_job.receipt_id] = 0
    executing_jobs_begin_times[current_job.receipt_id] = time.time()

    # Fork, and let the child run the executable
    child_pid = os.fork()
    if child_pid == 0:
        # Child process
        # time.sleep(1)
        job_execution.execute_job(
            current_job, current_job.executable, current_job_directory,
            execution_jobs_pid_dict, executing_jobs_required_times,
            executed_jobs_receipt_ids=executed_jobs_receipt_ids,
            self_ip=self_ip)
    else:
        # Parent process
        pass


def job_preemption_msg_handler(msg,
                               execution_jobs_pid_dict,
                               executed_jobs_receipt_ids,
                               executing_jobs_receipt_ids,
                               executing_jobs_begin_times,
                               executing_jobs_required_times,
                               server_ip,
                               self_ip):

    new_job, job_receipt_id = msg.content
    new_job_executable = msg.file

    # Get process id of child that executed/is executing this job
    executing_child_pid = execution_jobs_pid_dict[job_receipt_id]

    # Send kill signal to child, which will be handled via sigint_handler
    # sigint_handler will send EXECUTED_JOB to central server
    try:
        os.kill(executing_child_pid, signal.SIGTERM)
        time.sleep(5)
        del executing_jobs_receipt_ids[job_receipt_id]
    except OSError as err:
        if err.errno == errno.ESRCH:
           
            resend_executed_job_msg(job_receipt_id, server_ip)
    finally:
        # Only for safety, not really required.
        executed_jobs_receipt_ids[job_receipt_id] = 0

    # Now start new job execution
    job_exec_msg_handler(
        current_job=new_job,
        job_executable=new_job_executable,
        execution_jobs_pid_dict=execution_jobs_pid_dict,
        executing_jobs_receipt_ids=executing_jobs_receipt_ids,
        executing_jobs_begin_times=executing_jobs_begin_times,
        executing_jobs_required_times=executing_jobs_required_times,
        executed_jobs_receipt_ids=executed_jobs_receipt_ids,
        server_ip=server_ip,
        self_ip=self_ip)


def job_kill_msg_handler(msg,
                               execution_jobs_pid_dict,
                               executing_jobs_receipt_ids):

    job_receipt_id = msg.content

    # Get process id of child that executed/is executing this job
    executing_child_pid = execution_jobs_pid_dict[job_receipt_id]

    # Send kill signal to child, which will be handled via sigint_handler
    # sigint_handler will send EXECUTED_JOB to central server
    try:
        os.kill(executing_child_pid, signal.SIGTERM)
        time.sleep(5)
        del executing_jobs_receipt_ids[job_receipt_id]
    except OSError as err:
        if err.errno == errno.ESRCH:
            pass


def executed_job_to_parent_msg_handler(msg, executed_jobs_receipt_ids,
                                       server_ip):
   
    msg.msg_type = 'EXECUTED_JOB'
    executed_jobs_receipt_ids[msg.content.receipt_id] = 0
    print('Executed jobs', server_ip)
    messageutils.send_message(
        msg=msg,
        to=server_ip,
        msg_socket=None,
        port=network_params.SERVER_RECV_PORT)
    print('Sending executed job id=%d' % msg.content.receipt_id,
          end=' ')


def ack_executed_job_msg_handler(msg, ack_executed_jobs_receipt_ids):

    job_receipt_id = msg.content
    ack_executed_jobs_receipt_ids[job_receipt_id] = 0


def server_crash_msg_handler(
                            #  shared_submitted_jobs_array,
                            #  shared_acknowledged_jobs_array,
                            #  executed_jobs_receipt_ids,
                            #  ack_executed_jobs_receipt_ids
                            server_ip
                             ):

    # send first heartbeat to new primary server
    print("first_heartbeat sent to the backup-server")
    messageutils.send_heartbeat(
        to=server_ip, port=network_params.SERVER_RECV_PORT)
    # Replay all non-ack messages
    # replay_non_ack_msgs(shared_submitted_jobs_array,
    #                     shared_acknowledged_jobs_array,
    #                     executed_jobs_receipt_ids,
    #                     ack_executed_jobs_receipt_ids, server_ip)


# Helper Functions


def resend_executed_job_msg(job_receipt_id, server_ip):

    # Load job object into current_job
    job_pickle_file = '%s%d%s' % (EXECUTING_JOB_DIRECTORY_PREFIX,
                                  job_receipt_id, JOB_PICKLE_FILE)
    with open(job_pickle_file, 'rb') as handle:
        current_job = pickle.load(handle)

    # Resend message to server
    messageutils.make_and_send_message(
        msg_type='EXECUTED_JOB',
        content=current_job,
        file_path=None,
        to=server_ip,
        msg_socket=None,
        port=network_params.COMPUTE_NODE_SEND_PORT)


