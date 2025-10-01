#
# this simple class describe the job information, it basically collect job information from bjobs output
#

from abc import ABC, abstractmethod
from emgoat.util import JOB_STATUS_PD, VERY_BIG_NUMBER


class Cluster(ABC):
    """
    Base class to encapsulate some of the basic functionalities related
    to a cluster. Subclasses should implement the functions to interact
    with specific cluster flavors (e.g., LSF, SLURM, etc.)
    """

    class Node:
        """ Structure to store basic information about cluster nodes. """
        def __init__(self, name, gpu_type):
            """
            as the initial setup, we capture the name and gpu information from the bhost output
            :param gpu_type: gpu type
            """
            self.name = name
            self.gpu_type = gpu_type
            self.ngpus = 0
            self.ncpus = 0
            self.total_mem_in_gb = 0

            # the data below are related to the resources used on the node
            self.njobs = 0
            self.gpus_in_use = 0
            self.cores_in_use = 0
            self.memory_in_use = 0

        def __str__(self):
            return (f"Node name={self.name}, \n"
                f"gpu type={self.gpu_type}, \n"
                f"ngpus={self.ngpus}, \n"
                f"ncpus={self.ncpus}, \n"
                f"mem={self.total_mem_in_gb}, \n"
                f"number of jobs={self.njobs}, \n"
                f"number of gpus in use={self.gpus_in_use}, \n"
                f"number of cores in use={self.cores_in_use}, \n"
                f"current memory usage={self.memory_in_use} ")

        def get_gpus_unused(self):
            """
            get the gpu card number which is currently not used
            """
            return self.ngpus - self.gpus_in_use

        def get_cpus_unused(self):
            """
            get the unused cpu cores number
            """
            return self.ncpus - self.cores_in_use

        def get_memory_unused(self):
            """
            get the unused memory level
            """
            return self.total_mem_in_gb - self.memory_in_use

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
            if self.job_remaining_time == VERY_BIG_NUMBER:
                time_left = "None"
            else:
                time_left = str(self.job_remaining_time)
            return (f"jobID: {self.jobid}\n job_name: {self.job_name}\n submit_time: {self.submit_time}\n "
                    f"state: {self.state}\n "
                    f"general_state: {self.general_state}\n "
                    f"pending_time(minute): {self.pending_time}\n "
                    f"job_remaining_time(minutes): {time_left}\n "
                    f"start_time: {self.start_time}\n used_time(minutes): {self.used_time}\n "
                    f"cpu_used: {self.cpu_used}\n gpu_used: {self.gpu_used}\n "
                    f"memory_request(GB): {self.memory_used}\n compute_nodes: {self.compute_nodes}\n "
                    f"account_name: {self.account_name}\n")

    class Account:
        """
        define the account class associated with the jobs
        """

        def __init__(self, name):
            """
            initialization
            :param name: the account name
            """
            self.account_name = name
            self.njobs = 0
            self.ngpus = 0
            self.ncpus = 0

        def __str__(self):
            return (f"account name={self.account_name}, \n"
                f"number of jobs={self.njobs}, \n"
                f"ngpus={self.ngpus}, \n"
                f"ncpus={self.ncpus} ")

    class JobRequirements:
        """ Simple structure to store requirements for a given job. """
        def __init__(self, **kwargs):
            self.ncpus = kwargs.get('ncpus', 0)
            self.ngpus = kwargs.get('ngpus', 0)

            if not (self.ngpus or self.ncpus):
                raise Exception("Either GPUs or GPUs should be specified for"
                                "the job requirements. ")

            self.commands = kwargs.get('commands', [])

            self.total_memory = kwargs.get('total_memory', None)
            self.gpu_type = kwargs.get('gpu_type', None)
            self.wall_time = kwargs.get('wall_time', None)

        def __str__(self):
            return (f"CPUs: {self.ncpus}\n"
                    f"GPUs: {self.ngpus}\n"
                    f"total_memory: {self.total_memory}\n"
                    f"gpu_type: {self.gpu_type}\n"
                    f"wall_time: {self.wall_time}\n")

    @abstractmethod
    def get_nodes_info(self):
        pass

    @abstractmethod
    def get_jobs_info(self):
        pass

    @abstractmethod
    def get_accounts_info(self):
        pass

    @abstractmethod
    def get_time_interval_for_snapshots(self):
        pass

    @abstractmethod
    def get_data_for_snapshots(self):
        pass

    @abstractmethod
    def generate_job_script(self, requirement, output):
        pass

    @abstractmethod
    def get_cluster_overview(self):
        pass

    @abstractmethod
    def get_job_availability_check(self, requirement):
        pass

    @abstractmethod
    def get_job_estimation_landing(self, requirement):
        pass

    def update_node_with_job_info(self, node_list, job_list):
        """
        this function will further update the node with the job information
        """
        for node in node_list:

            # get the node name
            node_name = node.name

            # get the job landing on the node
            for job in job_list:

                # if the job is pending status, skip it
                if job.general_state == JOB_STATUS_PD:
                    continue

                # update the node data
                nodes = job.compute_nodes
                if node_name in nodes:
                    # usually the resources used are distributed evenly among
                    # the nodes
                    nnodes = len(nodes)
                    if (job.gpu_used/nnodes).is_integer():
                        ngpus_per_node = int(job.gpu_used/nnodes)
                    else:
                        raise RuntimeError("the number of gpus per node for the job should be "
                                           "integer: ".format(job.gpu_used/nnodes))
                    if (job.cpu_used/nnodes).is_integer():
                        ncpus_per_node = int(job.cpu_used/nnodes)
                    else:
                        raise RuntimeError("the number of cpus per node for the job should be "
                                           "integer: ".format(job.cpu_used/nnodes))
                    if (job.memory_used/nnodes).is_integer():
                        mem_per_node = int(job.memory_used/nnodes)
                    else:
                        raise RuntimeError("the memory usage per node for the job should be "
                                           "integer: ".format(job.memory_used/nnodes))
                    node.njobs += 1
                    node.gpus_in_use += ngpus_per_node
                    node.cores_in_use += ncpus_per_node
                    node.memory_in_use += mem_per_node

    def __init__(self, cluster_type):
        """
        initialization of cluster type for the super class

        :param cluster_type: slurm or lsf for the cluster
        """
        self.cluster_type = cluster_type