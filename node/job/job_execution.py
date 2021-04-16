"""This module's code is responsible for executing a job, and handling
it's preemption.

The function execute_job is run as a child process of the process running the
__main__.py module's code.
"""

import os
import pickle
import signal
import stat
import time
import subprocess
import resource

from ...messaging import messageutils
from ...messaging import network_params
from threading import Timer
import shlex
import traceback


JOB_PICKLE_FILE = '/job.pickle'


def execute_job(current_job,
                executable,
                current_job_directory,
                execution_jobs_pid_dict,
                executing_jobs_required_times,
                executed_jobs_receipt_ids,
                self_ip):
    """Execute the executable file, and send submission results to self_ip

    :param current_job: job object, to be executed
    :param execution_dst: str, path to executable file
    :param current_job_directory: str, directory storing job's files
    :param execution_jobs_pid_dict: manager.dict
    :param executing_jobs_required_times: manager.dict
    :param executed_jobs_receipt_ids: manager.dict
    :param shared_submission_interface_quit: mp.Value
    :param self_ip: str, ip address of current node.
    :return: None
    """

    # Record start time for job, share the variable with sigint_handler
    start_time = time.time()
    job_id = current_job.receipt_id
    execution_jobs_pid_dict[job_id] = os.getpid()
    executing_jobs_required_times[job_id] = current_job.time_required - current_job.time_run
    # File where updated job object will be stored
    job_pickle_file = current_job_directory + JOB_PICKLE_FILE

    print('Opening output files')
    out_file = open(os.path.join(current_job_directory, f'out{job_id}.out'), 'w')
    err_file = open(os.path.join(current_job_directory, f'err{job_id}.out'), 'w')
    print('Opened output files')
    child_proc = None
    # noinspection PyUnusedLocal
    def sigint_handler(signum, frame):
        """Handle sigint signal sent by parent

        Send ack message with updated job runtime to server, and exit.

        :param signum: signal number
        :param frame: frame object
        :return: None
        """
        print('Closing files')
        out_file.close()
        err_file.close()
        print('Closed files')

        child_proc.terminate()
        
        preemption_end_time = time.time()

        # Update job run time, completion status
        current_preempted_system_time_run = preemption_end_time - start_time
        current_job.time_run += current_preempted_system_time_run

        # Check if job has run for time more than its required time
        # Set completed field if so
        if current_job.time_run >= current_job.time_required:
            current_job.completed = True

        # Update the job's execution list with (machine name, time_run)
        current_job.execution_list.append((os.uname()[1],
                                           current_preempted_system_time_run))

        # Save job object as pickle in it's local directory. This is done
        # so that preempted_job_msg can be replayed if server goes down
        # before receiving/acknowledging it
        # with open(job_pickle_file, 'wb') as _handle:
        #     pickle.dump(current_job, _handle, protocol=pickle.HIGHEST_PROTOCOL)

        # Prepare and send executed job information message to parent
        executed_jobs_receipt_ids[job_id] = 0
        # messageutils.make_and_send_message(
        #     msg_type='EXECUTED_JOB_TO_PARENT',
        #     content=current_job,
        #     file_path=None, to=self_ip,
        #     msg_socket=None,
        #     port=network_params.COMPUTE_NODE_RECV_PORT)
        # Gracefully exit
        # noinspection PyProtectedMember
        os._exit(0)

    # Mask the SIGINT signal with sigint_handler function
    signal.signal(signal.SIGTERM, sigint_handler)

    # Elevate privileges
    # st = os.stat(execution_dst)
    # os.chmod(execution_dst, st.st_mode | stat.S_IEXEC)

    # Begin execution
    # os.system(execution_dst)
    # subprocess.call([execution_dst])
    
    cmd = shlex.split(executable)
    print(cmd)
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        maxsize = current_job.max_memory * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (maxsize, hard))
        print('Running for : ', current_job.time_required)
        # subprocess.run(cmd, stdout=out_file, stderr=err_file, timeout=current_job.time_required)
        child_proc = subprocess.Popen(args=cmd, stdout=out_file, stderr=err_file)
        timer = Timer(current_job.time_required, child_proc.kill)
        timer.start()
        print('Job executed successfully')
    
    except:
        err_file.write('Error: ')
        err_file.write(traceback.format_exc())
        print("Error")


    finally:
        print('Closing files')
        out_file.close()
        err_file.close()
        print('Closed files')
        # Execution call completed
        end_time = time.time()

        # Update job run time
        current_system_time_run = end_time - start_time
        current_job.time_run += current_system_time_run

        # Mark job completion
        current_job.completed = True

        # Update the job's execution list with (machine name, time_run)
        current_job.execution_list.append((os.uname()[1],
                                        current_system_time_run))

        # Save job object as pickle in it's local directory. This is done
        # so that job_completion_msg can be replayed if server goes down
        # before receiving/acknowledging it
        # with open(job_pickle_file, 'wb') as handle:
        #     pickle.dump(current_job, handle, protocol=pickle.HIGHEST_PROTOCOL)

        # Prepare and send job completion message to parent
        # executed_jobs_receipt_ids[job_id] = 0
        executed_jobs_receipt_ids[job_id] = 0
        
        messageutils.make_and_send_message(
            msg_type='EXECUTED_JOB_TO_PARENT',
            content=current_job,
            file_path=None, to=self_ip,
            msg_socket=None,
            port=network_params.COMPUTE_NODE_RECV_PORT)
