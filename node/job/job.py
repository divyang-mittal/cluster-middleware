"""File with class for job objects."""

import os


class Job:
   
    def __init__(self, name, executable, priority, time_required, min_memory,
                 min_cores, max_memory):

        self.submission_id = None
        self.receipt_id = None
        self.sender = None
        self.name = name
        self.username = os.uname()[1]
        self.executable = executable
        self.priority = int(priority)
        self.time_required = int(time_required)

        # Requirements
        self.min_memory = int(min_memory)
        self.min_cores = int(min_cores)
        self.max_memory = int(max_memory)

        # Job state
        self.time = 0
        self.time_run = 0
        self.completed = False
        self.execution_list = []
        self.submit_time = None
        self.first_response = None
        self.receive_time = None
        self.submission_completion_time = None

    def get_executable_name(self):

        executable_address_partitions = self.executable.split('/')
        return '/' + executable_address_partitions[-1]

    def __str__(self):

        return 'JOB: ' + str(self.submission_id) + ',' + str(self.receipt_id)

    def __eq__(self, other):

        return (self.receipt_id == other.receipt_id) or (
                self.submission_id == other.submission_id and
                self.sender == other.sender and
                self.sender is not None)

    def __lt__(self, other):

        return self.priority < other.priority

    def __hash__(self):
        return hash((self.submission_id, self.sender))
