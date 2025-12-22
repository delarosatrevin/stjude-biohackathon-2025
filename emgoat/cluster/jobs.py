#
# this class is to define the class related to jobs (LSF/slurm)
#

from abc import ABC, abstractmethod
from emgoat.util import JOB_STATUS_PD, VERY_BIG_NUMBER

class Job:

    """ Structure to store jobs information. """
    def __init__(self, jobid, job_name, submit_time, state, general_state,
                 pending_time, job_remaining_time,
                 start_time, used_time, cpu_used, gpu_used, memory_used,
                 compute_nodes, account_name):
        """
        the job ID is an integer; from LSF/slurm etc. However for easy handling we just treat it as a string

        the state is whether it's pending/running/finished/error/cancelled (five states allowed), the state
        symbol is taken from the scheduler (lsf/slurm)

        the general state is our internal defined state symbol (see macros.py, the JOB_STATUS_PD etc.)

        pending time is how long the job is pending (in unit of minutes)

        job remaining time: how much time the job is used (wall time), in unit of minutes

        start time: the job starting time (dateTime)

        memory used: how much memory used in unit of GB

        cpu/gpu used: how many cores/gpus used for the job

        compute nodes are the nodes that the job are submitted onto.
        """

        self.jobid = jobid
        self.job_name = job_name
        self.submit_time = submit_time
        self.state = state
        self.general_state = general_state
        self.pending_time = pending_time
        self.job_remaining_time = job_remaining_time
        self.start_time = start_time
        self.used_time = used_time
        self.cpu_used = cpu_used
        self.gpu_used = gpu_used
        self.memory_used = memory_used
        self.compute_nodes = compute_nodes
        self.account_name = account_name

    def __str__(self):
        """
        string format information
        """

        # job remaining time
        if self.job_remaining_time == VERY_BIG_NUMBER:
            time_left = "None"
        else:
            time_left = str(self.job_remaining_time)

        # return the string
        return (f"jobID: {self.jobid}\n job_name: {self.job_name}\n submit_time: {self.submit_time}\n "
                f"state: {self.state}\n "
                f"general_state: {self.general_state}\n "
                f"pending_time(minute): {self.pending_time}\n "
                f"job_remaining_time(minutes): {time_left}\n "
                f"start_time: {self.start_time}\n used_time(minutes): {self.used_time}\n "
                f"cpu_used: {self.cpu_used}\n gpu_used: {self.gpu_used}\n "
                f"memory_request(GB): {self.memory_used}\n compute_nodes: {self.compute_nodes}\n "
                f"account_name: {self.account_name}\n")

