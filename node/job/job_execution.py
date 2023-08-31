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

        print('Closing files')
        out_file.close()
        err_file.close()
        print('Closed files')

        # child_proc.terminate()
        
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

       
        executed_jobs_receipt_ids[job_id] = 0
        os._exit(0)

    # Mask the SIGINT signal with sigint_handler function
    signal.signal(signal.SIGTERM, sigint_handler)


    
    cmd = shlex.split(executable)
    print(cmd)
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        maxsize = current_job.max_memory * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (maxsize, hard))
        print('Running for : ', current_job.time_required)
        subprocess.run(cmd, stdout=out_file, stderr=err_file, timeout=current_job.time_required)

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

        executed_jobs_receipt_ids[job_id] = 0
        
        messageutils.make_and_send_message(
            msg_type='EXECUTED_JOB_TO_PARENT',
            content=current_job,
            file_path=None, to=self_ip,
            msg_socket=None,
            port=network_params.COMPUTE_NODE_RECV_PORT)

        os._exit(0)
