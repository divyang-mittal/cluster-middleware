"""Message handlers for all messages received at a node

Includes handlers for:
    * Heartbeat message from server
    * Acknowledgement of job submission from server
    * Job execution request from server
    * Job preemption request from server
    * Job execution completion message from child executing the job
    * Acknowledgement of job execution message from server
    * Submitted job completion message from server
    * Server crash message from child executing crash detection
"""

import errno
import os
import pickle
import signal
import time

from .job import job_execution
from ..messaging import messageutils
from ..messaging import network_params

SUBMITTED_JOB_DIRECTORY_PREFIX = './submit_job'
EXECUTING_JOB_DIRECTORY_PREFIX = './exec_job'
JOB_PICKLE_FILE = '/job.pickle'


def job_exec_msg_handler(current_job, job_executable,
                         execution_jobs_pid_dict,
                         executing_jobs_receipt_ids,
                         executing_jobs_begin_times,
                         executing_jobs_required_times,
                         executed_jobs_receipt_ids,
                         server_ip, self_ip):
    """Fork a process to execute the job

    :param current_job: job, to be executed
    :param job_executable: byte stream, of the job's executable
    :param execution_jobs_pid_dict: dict, storing job_receipt_id:pid pairs
    :param executing_jobs_receipt_ids: set, receipt ids of executing jobs
    :param executing_jobs_begin_times: dict, receipt id: approx begin time
    :param executing_jobs_required_times: dict, receipt id:job required time
    :param executed_jobs_receipt_ids: manager.dict
    :param shared_submission_interface_quit: shared mp.Value
    :param server_ip: str, ip address of server
    :param self_ip: str, ip address of this node
    :return: None
    """
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

    # Store a.out in this directory
    executable_file_bytes = job_executable
    execution_dst = current_job_directory + current_job.get_executable_name()

    while os.path.isfile(execution_dst):
        execution_dst = execution_dst + '_preempt'

    with open(execution_dst, 'wb') as file:
        file.write(executable_file_bytes)

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
            current_job, execution_dst, current_job_directory,
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
    """Handle receive of job preemption message

    :param msg: message, received message
    :param execution_jobs_pid_dict: dict, job receipt id:pid pairs
    :param executed_jobs_receipt_ids: set, receipt ids of jobs that are
        done executing
    :param executing_jobs_receipt_ids: set, receipt id of all executing jobs
    :param executing_jobs_begin_times: dict, receipt id: approx begin time
    :param executing_jobs_required_times: dict, receipt id:job required time
    :param shared_submission_interface_quit: Mp.value
    :param server_ip: str, id address of server
    :param self_ip: str
    :return: None
    """
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
            # ESRCH: child process no longer exists
            # This implies that either this job was preempted, and this
            # preemption message is a duplicate from switched server, or
            # the process already completed and server didn't receive
            # completion message before sending preempt request,
            # or the servers switched. In any case,
            # we resend the EXECUTED_JOB msg for safety.
            # Ideally, it should be not be possible to come to this section
            # due to initial check on executed_jobs_receipt_ids
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
    """Handle receive of job preemption message

    :param msg: message, received message
    :param execution_jobs_pid_dict: dict, job receipt id:pid pairs
    :param executed_jobs_receipt_ids: set, receipt ids of jobs that are
        done executing
    :param executing_jobs_receipt_ids: set, receipt id of all executing jobs
    :param executing_jobs_begin_times: dict, receipt id: approx begin time
    :param executing_jobs_required_times: dict, receipt id:job required time
    :param shared_submission_interface_quit: Mp.value
    :param server_ip: str, id address of server
    :param self_ip: str
    :return: None
    """
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
            # ESRCH: child process no longer exists
            # This implies that either this job was preempted, and this
            # preemption message is a duplicate from switched server, or
            # the process already completed and server didn't receive
            # completion message before sending preempt request,
            # or the servers switched. In any case,
            # we resend the EXECUTED_JOB msg for safety.
            # Ideally, it should be not be possible to come to this section
            # due to initial check on executed_jobs_receipt_ids
            resend_executed_job_msg(job_receipt_id, server_ip)


def executed_job_to_parent_msg_handler(msg, executed_jobs_receipt_ids,
                                       server_ip):
    """Handle message from child signifying an executed job

    The message is simply forwarded to server, with updates to book-keeping set

    :param msg: message, received from child process executing the job
    :param executed_jobs_receipt_ids: set, receipt ids of jobs that are
        done executing
    :param server_ip: str, ip address of server
    """
    msg.msg_type = 'EXECUTED_JOB'
    executed_jobs_receipt_ids[msg.content.receipt_id] = 0
    messageutils.send_message(
        msg=msg,
        to=server_ip,
        msg_socket=None,
        port=network_params.CLIENT_SEND_PORT)
    print('Sending executed job r_id=%d\n\n>>>' % msg.content.receipt_id,
          end=' ')


def ack_executed_job_msg_handler(msg, ack_executed_jobs_receipt_ids):
    """Handle the acknowledgement message of EXECUTED_JOB msg.

    :param msg: message, received ack message from server
    :param ack_executed_jobs_receipt_ids: set, receipt ids of jobs that have
        executed on this system and have received ack from server
    """
    job_receipt_id = msg.content
    ack_executed_jobs_receipt_ids[job_receipt_id] = 0


def server_crash_msg_handler(shared_submitted_jobs_array,
                             shared_acknowledged_jobs_array,
                             executed_jobs_receipt_ids,
                             ack_executed_jobs_receipt_ids, server_ip):
    """Handle a message recvd from server fault detecting child process about
    an assumed server crash at server_ip

    :param shared_submitted_jobs_array: mp.Array with type int,
        contains submission id of jobs
    :param shared_acknowledged_jobs_array: mp.Array, acknowledged submitted jobs
    :param executed_jobs_receipt_ids: set, receipt ids of executed jobs
    :param ack_executed_jobs_receipt_ids: set, receipt ids of acknowledged
        executed jobs
    :param server_ip: str, ip address of server
    """
    # send first heartbeat to new primary server
    messageutils.send_heartbeat(
        to=server_ip, port=network_params.CLIENT_SEND_PORT)
    # Replay all non-ack messages
    replay_non_ack_msgs(shared_submitted_jobs_array,
                        shared_acknowledged_jobs_array,
                        executed_jobs_receipt_ids,
                        ack_executed_jobs_receipt_ids, server_ip)


# Helper Functions


def resend_executed_job_msg(job_receipt_id, server_ip):
    """Helper function for job_preemption_msg_handler

    Loads job pickle into job object and sends EXECUTED_JOB msg to server

    :param job_receipt_id: int, receipt id of job
    :param server_ip: str, server's ip address
    """
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
        port=network_params.CLIENT_SEND_PORT)


def replay_non_ack_msgs(shared_submitted_jobs_array,
                        shared_acknowledged_jobs_array,
                        executed_jobs_receipt_ids,
                        ack_executed_jobs_receipt_ids, server_ip):
    """Send all non ack messages to server.

    Use the book-keeping arrays and sets to find the non ack messages

    :param shared_submitted_jobs_array: mp.Array with type int,
        contains submission id of jobs
    :param shared_acknowledged_jobs_array: mp.Array, acknowledged submitted jobs
    :param executed_jobs_receipt_ids: set, receipt ids of executed jobs
    :param ack_executed_jobs_receipt_ids: set, receipt ids of acknowledged
        executed jobs
    :param server_ip: str, id address of server
    """
    for itr, elem in enumerate(shared_submitted_jobs_array):
        if elem and not shared_acknowledged_jobs_array[itr]:
            # Non acknowledged job submission, resend job
            print('Replaying non-acked SUBMIT_JOB for job s_id =', itr,
                  '\n\n>>>', end=' ')
            submit_job(job_id=itr, server_ip=server_ip)

    non_ack_executing_jobs = \
        set(executed_jobs_receipt_ids.keys()) - \
        set(ack_executed_jobs_receipt_ids.keys())

    for receipt_id in non_ack_executing_jobs:
        # Non acknowledged executed job msg, resend message
        print('Replaying non-acked EXECUTED_JOB for job r_id =', receipt_id,
              '\n\n>>>', end=' ')
        resend_executed_job_msg(job_receipt_id=receipt_id, server_ip=server_ip)
