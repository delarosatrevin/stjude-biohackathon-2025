#
# this simple class describe the job information, it basically collect job information from bjobs output
#

from abc import ABC, abstractmethod
from emgoat.util import JOB_STATUS_PD, VERY_BIG_NUMBER
from datetime import datetime


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

            sometimes because the default data can be < 0, therefore we check whether the data is > 0
            """
            self.name = name
            self.gpu_type = gpu_type
            if ngpus > 0:
                self.ngpus = ngpus
            else:
                self.ngpus = 0
            if ncpus > 0:
                self.ncpus = ncpus
            else:
                self.ncpus = 0
            if total_mem_in_gb > 0:
                self.total_mem_in_gb = total_mem_in_gb
            else:
                self.total_mem_in_gb = 0

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

        def to_dict(self):
            """
            this function is to convert this object into a dict
            """
            return {"node_name":self.name, "gpu_type":self.gpu_type, "total_gpus":self.ngpus,
                    "total_cpus":self.ncpus, "total_mem_in_gb":self.total_mem_in_gb,
                    "njobs":self.njobs, "ngpus_in_use":self.gpus_in_use, "ncpus_in_use":self.cores_in_use,
                    "memory_in_use":self.memory_in_use, "available_gpus":self.get_gpus_unused(),
                    "available_cpus":self.get_cpus_unused(), "available_memory":self.get_memory_unused()}

        def __str__(self):
            return (f"Node name={self.name}, \n"
                    f"gpu type={self.gpu_type}, \n"
                    f"ngpus={self.ngpus}, \n"
                    f"ncpus={self.ncpus}, \n"
                    f"mem={self.total_mem_in_gb}, \n"
                    f"number of jobs={self.njobs}, \n"
                    f"number of gpus in use={self.gpus_in_use}, \n"
                    f"number of gpus not in use={self.get_gpus_unused()}, \n"
                    f"number of cores in use={self.cores_in_use}, \n"
                    f"number of cores not in use={self.get_cpus_unused()}, \n"
                    f"current memory usage={self.memory_in_use} \n"
                    f"current memory not in use={self.get_memory_unused()}")

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

        def get_gpus_used(self):
            """
            get the gpu card number which is currently used
            """
            return self.gpus_in_use

        def get_cpus_used(self):
            """
            get the used cpu cores number
            """
            return self.cores_in_use

        def get_memory_used(self):
            """
            get the used memory level
            """
            return self.memory_in_use

    class Job:
        """ 
        Structure to store jobs information. 

        make the type annotations to the input data members
        """

        def __init__(self, jobid: str, job_name: str,
                submit_time: datetime, state: str, general_state: str,
                pending_time: int, job_remaining_time: int,
                start_time: datetime, used_time: int, 
                cpu_used: int, gpu_used: int, memory_used: int,
                compute_nodes: list[str], 
                account_name: str):
            """
            the job ID is from LSF/slurm etc. However for easy handling we just treat it as a string

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

        def to_dict(self):
            """
            this function is to transform the data into a dict
            """
            if self.job_remaining_time == VERY_BIG_NUMBER:
                time_left = "None"
            else:
                time_left = str(self.job_remaining_time)

            # time format for the time - it should go to config finally
            time_format = "%Y-%m-%d %H:%M:%S"

            # double check the starrt time, it could be None
            start_time_str = "N/A"
            if self.start_time is not None:
                start_time_str = self.start_time.strftime(time_format)

            # make compute list into one string
            if len(self.compute_nodes) == 1:
                compute_nodes = self.compute_nodes[0]
            elif len(self.compute_nodes) > 1:
                compute_nodes = " ".join(self.compute_nodes)
            else:
                compute_nodes = " "

            # the dict
            return {"account_name": self.account_name, "jobID": self.jobid, "job_name": self.job_name,
                    "submit_time": self.submit_time.strftime(time_format),
                    "state": self.state, "general_state": self.general_state,
                    "pending_time_in_minutes": self.pending_time,
                    "job_remaining_time_in_minutes": time_left,
                    "start_time": start_time_str,
                    "used_time_in_minutes": self.used_time,
                    "cpu_used": self.cpu_used,
                    "gpu_used": self.gpu_used,
                    "memory_request_in_GB": self.memory_used,
                    "compute_nodes_list": compute_nodes}

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

        def has_any_jobs(self):
            """
            return true if the account has any running/pending jobs
            """
            return self.n_running_jobs + self.n_pending_jobs > 0

        def to_dict(self):
            """this function is to transform the object into dict"""
            nodes_name_list = " ".join(self.nodes_list)
            return {"account_name":self.account_name,
                    "n_running_jobs":self.n_running_jobs,
                    "n_pending_jobs":self.n_pending_jobs,
                    "n_gpus_used":self.ngpus,
                    "n_cpus_used":self.ncpus,
                    "compute_nodes_list": nodes_name_list}

    class Summary:
        """
        This is the summary of the current cluster usage, including the overview for the gpu usage
        """

        def __init__(self, nodes_list, job_list):
            """
            initialize the data for cluster summary
            """

            # initialize all of data
            self.n_total_jobs = 0
            self.n_running_jobs = 0
            self.n_pending_jobs = 0
            self.n_total_gpus = 0
            self.n_used_gpus = 0
            self.n_total_cores = 0
            self.n_used_cores = 0
            self.n_total_mem_in_gb = 0
            self.n_used_mem_in_gb = 0

            # all of job information
            for job in job_list:
                if job.general_state == JOB_STATUS_PD:
                    self.n_pending_jobs += 1
                else:
                    self.n_running_jobs += 1
                self.n_total_jobs += 1

            # resources summary
            for node in nodes_list:
                self.n_total_gpus += node.ngpus
                self.n_total_cores += node.ncpus
                self.n_total_mem_in_gb += node.total_mem_in_gb
                self.n_used_gpus += node.gpus_in_use
                self.n_used_cores += node.cores_in_use
                self.n_used_mem_in_gb += node.memory_in_use

            # generates the overview
            self.gpus_overview = {1: (0, 0.0), 2: (0, 0.0), 4: (0, 0.0), 6: (0, 0.0), 8: (0, 0.0)}
            for gpu_select in self.gpus_overview:
                available_slots = 0
                for node in nodes_list:
                    gpus_remain = node.get_gpus_unused()
                    if gpus_remain >= gpu_select:
                        available_slots += int(gpus_remain / gpu_select)

                # update the result
                percentage = float(available_slots / self.n_total_gpus)
                self.gpus_overview[gpu_select] = (available_slots, percentage)

        def __str__(self):

            # overview
            overview = ""
            for gpu_select in self.gpus_overview:
                overview = overview + ("for {} gpu the number of available slots {} and "
                                       "percentage {:.2f}, \n").format(gpu_select,
                                                                       self.gpus_overview[gpu_select][0],
                                                                       self.gpus_overview[gpu_select][1])

            # result
            return (f"total jobs number={self.n_total_jobs}, \n"
                    f"number of running jobs={self.n_running_jobs}, \n"
                    f"number of pending jobs={self.n_pending_jobs}, \n"
                    f"total number of gpus={self.n_total_gpus}, \n"
                    f"total number of used gpus={self.n_used_gpus}, \n"
                    f"total number of cores={self.n_total_cores}, \n"
                    f"total number of used cores={self.n_used_cores}, \n"
                    f"total capacity of memory in gb={self.n_total_mem_in_gb}, \n"
                    f"total capacity of used memory in gb={self.n_used_mem_in_gb}, \n" +
                    overview)

        def to_dict(self):
            """this function is to transform the data into dict"""

            # convert the overview into a dict
            result = { }
            for gpus in self.gpus_overview:
                v0 = self.gpus_overview[gpus][0]
                v1 = self.gpus_overview[gpus][1]
                key = "proposed_gpu_num_" + str(gpus)
                result[key] = str(v0) + " " + str(v1)

            # update result
            result.update({"total_jobs_number":self.n_total_jobs,
                    "n_running_jobs":self.n_running_jobs,
                    "n_pending_jobs":self.n_pending_jobs,
                    "total_gpus_number":self.n_total_gpus,
                    "total_used_gpus":self.n_used_gpus,
                    "total_cores_number":self.n_total_cores,
                    "total_used_cores":self.n_used_cores,
                    "all_available_memory_in_gb":self.n_total_mem_in_gb,
                    "total_used_memory_in_gb":self.n_used_mem_in_gb})

            # return
            return result

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

    @abstractmethod
    def get_cluster_summary_info(self):
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

    def get_total_unused_gpu_num(self, nodes_list):
        """
        for the current node list, get the total number of unused gpu cards and it's percentage
        :return: the total number of unused gpu cards, and it's percentage
        """
        total_gpu_num = self.get_total_gpu_num(nodes_list)
        total_unused_gpu = 0
        for node in nodes_list:
            total_unused_gpu += node.get_gpus_unused()
        return total_unused_gpu, float(total_unused_gpu / total_gpu_num)

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
                mem_per_node = int(job.memory_used / nnodes)
                node.update_jobs_infor(gpus_in_use=ngpus_per_node, cores_in_use=ncpus_per_node,
                                       memory_in_use=mem_per_node)

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
                    ngpus_used = job.gpu_used
                    job_status = job.general_state
                    nodes_name = job.compute_nodes
                    for node in nodes_name:
                        acc.update_values(ncores_used, ngpus_used, job_status, node)

        # finally return
        return account_list


