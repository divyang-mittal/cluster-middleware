
class ServerState(object):

    def __init__(self, compute_nodes, running_jobs, job_queue, job_executable,
                 job_sender, state_order, job_receipt_id):

        # Convert from priority queue class JobQueue to a simple list
        self.state_order = state_order
        self.job_receipt_id = job_receipt_id
        self.job_queue = job_queue
        # while not job_queue.empty():
        #     job = job_queue.get()
        #     self.job_queue.append(job)

        self.compute_nodes = compute_nodes
        self.running_jobs = running_jobs
        self.job_sender = job_sender
        self.job_executable = job_executable

    def __str__(self):
        return 'SERVER STATE: ' + str(self.compute_nodes)