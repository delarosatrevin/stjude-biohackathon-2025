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
    _name = None  # Should be defined in subclasses

    class Node:
        """
        Structure to store basic information about cluster nodes.

        All of the information are complete when passing into this constructor
        """

        def __init__(self, name, gpu_type, ngpus, ncpus, total_mem_in_gb):
            """
            as the initial setup, all of the input data are corresponding to complete information
            regarding to the node

            the jobs related information will be initialized later (njobs, cores_in_use etc.)
            """
            self.name = name
            self.gpu_type = gpu_type
            self.ngpus = ngpus
            self.ncpus = ncpus
            self.total_mem_in_gb = total_mem_in_gb

            # the data below are related to the resources used on the node
            self.njobs = 0
            self.gpus_in_use = 0
            self.cores_in_use = 0
            self.memory_in_use = 0


        def update_jobs_infor(self, gpus_in_use, cores_in_use, memory_in_use):
            """
            update the corresponding data from one job
            """
            self.njobs += 1
            self.gpus_in_use += gpus_in_use
            self.cores_in_use += cores_in_use
            self.memory_in_use += memory_in_use

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

            compute nodes are the nodes that the job are submitted onto, it's a list of string
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
            self.account_name   = name
            self.n_running_jobs = 0
            self.n_pending_jobs = 0
            self.ngpus = 0
            self.ncpus = 0
            self.nodes_list = []

        def __str__(self):
            nodes_name_list = " ".join(self.nodes_list)
            return (f"account name={self.account_name}, \n"
                f"number of running jobs={self.n_running_jobs}, \n"
                f"number of pending jobs={self.n_pending_jobs}, \n"
                f"ngpus={self.ngpus}, \n"
                f"ncpus={self.ncpus} \n"
                f"compute nodes list= {nodes_name_list}")

        def update_values(self, ncores_used, ngpus_used, job_status, node_name):
            """
            depending on the job status, let's update the values
            """
            if job_status == JOB_STATUS_PD:
                self.n_pending_jobs += 1
            else:
                self.n_running_jobs += 1
                self.ngpus += ngpus_used
                self.ncpus += ncores_used
                if node_name not in self.nodes_list:
                    self.nodes_list.append(node_name)


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

    ################################################################################
    ##### here we define abstraction functions                                 #####
    ################################################################################

    @abstractmethod
    def get_nodes_info(self):
        pass


    @abstractmethod
    def get_jobs_info(self):
        pass


    @abstractmethod
    def get_accounts_info(self):
        pass


    ################################################################################
    ##### here we define functions that working for all of objects (LSF/Slurm) #####
    ################################################################################
    @staticmethod
    def get_pending_job_list(jobs_list):
        """
        this function returns the pending job list from the input list of Jobs

        :return: a new job list only has the pending jobs
        """
        new_list = [x for x in jobs_list if x.general_state == JOB_STATUS_PD]
        return new_list


    @staticmethod
    def get_total_gpu_num(nodes_list):
        """
        for the current node list, get the total number of gpu cards
        :return: the total number of gpu cards
        """
        total_gpu_num = 0
        for node in nodes_list:
            total_gpu_num += node.ngpus
        return total_gpu_num


    @staticmethod
    def update_node_with_job_info(node_list, job_list):
        """
        this function will further update the node with the job information
        """
        for node in node_list:

            # get the node name
            node_name = node.name

            # get the job landing on the node
            for job in job_list:

                # update the node data
                nodes = job.compute_nodes
                if node_name not in nodes:
                    continue

                # if the job is pending status, skip it
                if job.general_state == JOB_STATUS_PD:
                    continue

                # test whether nodes is list >= 1
                nnodes = len(nodes)
                if nnodes == 0:
                    print(job)
                    raise RuntimeError("the number of nodes for the above job is 0 in update_node_with_job_info")

                # update data
                # all of data below should be good for direct compute
                ngpus_per_node = int(job.gpu_used / nnodes)
                ncpus_per_node = int(job.cpu_used / nnodes)
                mem_per_node   = int(job.memory_used / nnodes)
                node.update_jobs_infor(gpus_in_use=ngpus_per_node, cores_in_use=ncpus_per_node, memory_in_use=mem_per_node)


    def form_accounts_infor(self, jobs_list):
        """
        forming the account list based on the job list data
        """

        # firstly form the account list with account name
        account_name_list = []
        for x in jobs_list:
            if x.account_name not in account_name_list:
                account_name_list.append(x.account_name)

        # now forms the account list with empty values
        account_list = [self.Account(x) for x in account_name_list]

        # now loop over the job list
        for acc in account_list:
            for job in jobs_list:
                if job.account_name == acc.account_name:
                    ncores_used = job.cpu_used
                    ngpus_used  = job.gpu_used
                    job_status  = job.general_state
                    nodes_name  = job.compute_nodes
                    for node in nodes_name:
                        acc.update_values(ncores_used, ngpus_used, job_status, node)

        # finally return
        return account_list


    def get_cluster_overview(self, nodes_list):
        """
        this function returns a cluster overview
        :return: a dict that describes the availability of gpu resources
        """
        total_gpu_num = self.get_total_gpu_num(nodes_list)
        gpu_selections = [1, 2, 4, 6, 8]
        result = {}
        for gpu_select in gpu_selections:
            available_slots = 0
            for node in nodes_list:
                gpus_remain = node.get_gpus_unused()
                if gpus_remain >= gpu_select:
                    available_slots += int(gpus_remain/gpu_select)

            # update the result
            percentage = available_slots/total_gpu_num
            result[gpu_select] = (available_slots, percentage)

        # return the result
        return result
