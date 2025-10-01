
import os
import json
import copy
import emgoat
from emgoat.util import Config, run_command, LSF_CLUSTER, GPU_TYPE, get_job_general_status
from emgoat.util import JOB_STATUS_PD, JOB_STATUS_RUN, is_str_float, is_str_integer
from emgoat.util import convert_float_to_integer, convert_str_to_integer

from .functions import *
from ..base import Cluster
from datetime import datetime, timedelta


class LSFCluster(Cluster):
    """ Cluster implementation for LSF system. """
    _config = Config(emgoat.config['lsf'])
    _job_snapshots_config = Config(emgoat.config['snapshots'])

    def get_nodes_info(self):
        return self.nodes_list

    def get_jobs_info(self):
        return self.jobs_list

    def get_accounts_info(self):
        return self.accounts_list

    def get_time_interval_for_snapshots(self):
        return self.time_interval_list

    def get_data_for_snapshots(self):
        return self.snapshots_node_list

    def __init__(self):
        """
        initialization of lsf cluster
        """

        # cluster type
        super().__init__(LSF_CLUSTER)

        # get the nodes information
        self.nodes_list = self._set_nodes_info()

        # get the job and accounts info
        self.jobs_list, self.accounts_list = self._set_jobs_and_accounts_info()

        # now we need to update the current node usage
        super().update_node_with_job_info(node_list=self.nodes_list, job_list=self.jobs_list)

        # now let's generate the future job snapshot in the list
        self.time_interval_list, self.snapshots_node_list = self._generate_job_snapshots()

    # ------------- HOST related internal functions ------------------

    def _set_nodes_info(self):

        # firstly get the bhost output
        output = self._run_bhosts_get_gpu_info()

        # get the node list, with gpu information inside
        gpu_node_list = self._parse_bhost_gpu_infor(output)

        # now with the gpu node list let's get the full node list
        cpu_node_list = self._create_cpu_node_list()
        node_list = gpu_node_list + cpu_node_list

        # get the node names for the list and push the cpus and memory
        # information into the node list
        name_list = [x.name for x in node_list]
        output = self._run_lshosts_get_cpu_info(name_list)
        self._parse_lshosts_cpu_infor(output, node_list)

        # finally return the node list
        return node_list

    def _set_jobs_and_accounts_info(self):
        output = self._run_bjobs_get_alljobs()
        job_list, account_list = self._parse_bjobs_output_for_alljobs(output)
        return job_list, account_list

    def _run_bhosts_get_gpu_info(self):
        """
        run the bhosts command to get node list and each node gpu information.
        command line arguments format is derived from the config

        :return: the raw output for the bhost command
        """
        global CONFIG

        # get the argument list
        # we will run from current node
        arg = self._config.get_list('bhosts_gpu_info')
        arg.append(self._config['bhosts_gpu_node_group_name'])
        return run_command(arg)

    def _run_lshosts_get_cpu_info(self, node_name_list):
        """
        run the lshosts command to get cpu information

        :return: the raw output for the lshost command,
        """
        # get the argument list
        arg = ['lshosts'] + node_name_list
        return run_command(arg)

    def _create_cpu_node_list(self):
        """
        :return: the node list without gpu, in default ngpus is 0
        """
        gpu_type = "NONE"
        namelist = self._config.get_list('lshosts_cpu_node_list')
        return [self.Node(name, gpu_type) for name in namelist]

    def _parse_lshosts_cpu_infor(self, output, nodelist):
        """
        update the input node list with cpu information
        :param output: the raw output from lshosts command
        """
        for line in output.splitlines():
            infor = line.split()

            # skip the head line
            if infor[0] == "HOST_NAME":
                continue

            # each line is for one host
            for node in nodelist:
                if node.name == infor[0]:
                    ncpus = infor[4]
                    mem = infor[5]

                    # convert memory
                    # memory should be only in unit of G or T
                    #
                    # the data should be a number with Unit G or T
                    # so trim the last character
                    if mem.find("G") > 0 or mem.find("g") > 0 or mem.find("T") > 0 or mem.find("t") > 0:
                        v0 = mem[:-1]
                        if is_str_float(v0):
                            # in case the value is in unit of tb
                            v1 = v0
                            if mem.find("T") > 0 or mem.find("t") > 0:
                                v1 = str(float(v0)*1024)
                            val = convert_float_to_integer(v1)
                        elif is_str_integer(v0):
                            val = convert_str_to_integer(v0)
                        else:
                            raise RuntimeError("Failed to convert the input memory value: {}".format(mem))
                    else:
                        raise RuntimeError("The input memory value should be in unit of GB: {}".format(mem))

                    # now let's update
                    node.ncpus = int(ncpus)
                    node.total_mem_in_gb = val

    def _get_gpu_type_for_node_from_lsf(self, input):
        """
        check the gpu type from the input
        :param input: input string obtained from lsf command
        :return: the gpu type
        """
        for t in GPU_TYPE:
            t0 = t.split("_")[0]
            mem_label = t.split("_")[1]

            # if the type match
            if input.find(t0) > 0 and input.find(mem_label) > 0:
                return t

        # now if we are here, that means we did not find anything match
        info = "Invalid input GPU type for paring: {}".format(input)
        raise RuntimeError(info)

    def _parse_bhost_gpu_infor(self, output):
        """
        this function will parse the output from run_bhosts_get_gpu_info
        :param output: the raw output from the function run_bhosts_get_gpu_info
        :return: a list of nodes object
        """
        nodelist = []
        current_gpu_number = 1
        for line in output.splitlines():
            infor = line.split()

            # skip the head line
            if infor[0] == "HOST_NAME":
                continue

            # add in the node
            if len(infor) == 9:
                name = infor[0]
                gpu_type = self._get_gpu_type_for_node_from_lsf(infor[2])
                node = self.Node(name, gpu_type)

                # update the gpu number for the last node
                if len(nodelist) > 0:
                    ngpus = current_gpu_number
                    previous_node = nodelist[-1]
                    previous_node.ngpus = ngpus

                # reset the gpu number for current node
                current_gpu_number = 1

                # add in the current node
                nodelist.append(node)

            # this line only has the gpu card information for the node
            elif len(infor) == 8:
                current_gpu_number = current_gpu_number + 1

        # add in the last ngpu for the node
        last_node = nodelist[-1]
        last_node.ngpus = current_gpu_number

        # return the list
        return nodelist

    # ------------- JOBS related internal functions ------------------
    def _run_bjobs_get_alljobs(self):
        """
        run the bjobs command to get all of job information.
        command line arguments format is derived from the config

        :return: the raw output for the bjobs, the output is in json format
        """
        conf = self._config # shortcut
        # get the argument list
        # we will run from current node
        args = (conf.get_list('bjobs'))
        args.extend(["-q", conf['queue_name'], "-o", conf['bjobs_output_format']])
        return run_command(args)

    def _parse_bjobs_output_for_alljobs(self, output):
        """
        parsing the output of bjobs
        :param output: the output data in json format in string
        :return: a list of dict that contains the job name and user names etc. information
        """
        # load in the raw output to json format output for further parsing
        data = json.loads(output)

        # this is the job information
        # each result is a Jobs object
        job_list = []

        # this is the account list currently in use
        account_list = []

        # now let's form the Jobs
        for i, record in enumerate(data['RECORDS']):
            # load in the json data
            jobid = record['JOBID']
            status = record['STAT']
            account_name = record['USER']
            job_name = record['JOB_NAME']
            submit_time = get_time_data_from_lsf_output(record['SUBMIT_TIME'])
            start_time = get_time_data_from_lsf_output(record['START_TIME'])
            pending_time = int(convert_str_to_integer(record['PEND_TIME']) / 60)
            ori_time_left = record['TIME_LEFT']
            ori_running_time = record['RUN_TIME']
            ncpus_request = convert_str_to_integer(record['NREQ_SLOT'])
            ori_mem_request = record['MEMLIMIT']
            gpu_used = convert_str_to_integer(record['GPU_NUM'])
            nhosts = convert_str_to_integer(record['NEXEC_HOST'])
            ori_host_name = record['EXEC_HOST']

            # running time data
            running_time = 0
            if ori_running_time.find("second") > 0:
                running_time = int(convert_str_to_integer(ori_running_time.split()[0]) / 60)

            # remaining time
            # in default if we do not have the data, it's a very big number
            if ori_time_left.find("L") > 0:
                remaining_time = convert_lsf_time_to_minutes(ori_time_left.strip().split()[0])
            else:
                remaining_time = convert_lsf_time_to_minutes(ori_time_left)

            # memory
            # we only handle the GB/TB cases, other cases we will issue an error
            if ori_mem_request.lower().find("g") > 0:
                mem = convert_str_to_integer(ori_mem_request.split()[0])
            elif ori_mem_request.lower().find("t") > 0:
                mem = convert_str_to_integer(ori_mem_request.split()[0]) * 1024
            else:
                raise RuntimeError("Invalid memory requested passed in: {}".format(ori_mem_request))

            # host name
            # if nhosts > 1:
            #     from pprint import pprint
            #     pprint(record)
            #     raise RuntimeError(
            #         "It seems the job used more than one node, currently we do not know how to get the data")

            # now we have everything, building the Jobs object
            job_infor = self.Job(
                jobid=jobid,
                job_name=job_name,
                submit_time=submit_time,
                state=status,
                general_state=get_job_general_status(status),
                pending_time=pending_time,
                job_remaining_time=remaining_time,
                start_time=start_time,
                used_time=running_time,
                cpu_used=ncpus_request,
                gpu_used=gpu_used,
                memory_used=mem,
                compute_nodes=get_hostnames_from_bjobs_output(ori_host_name),
                account_name=account_name
            )

            # add in result
            job_list.append(job_infor)

            # checking the account
            account_name_list = [x.account_name for x in account_list]
            if account_name in account_name_list:
                for acc in account_list:
                    if acc.account_name == account_name:
                        acc.njobs = acc.njobs + 1
                        if gpu_used > 0:
                            acc.ngpus = acc.ngpus + gpu_used
                            acc.ncpus = acc.ncpus + ncpus_request
            else:
                acc = Cluster.Account(account_name)
                acc.njobs = acc.njobs + 1
                if gpu_used > 0:
                    acc.ngpus = acc.ngpus + gpu_used
                    acc.ncpus = acc.ncpus + ncpus_request
                account_list.append(acc)

        # now return
        return job_list, account_list

    # ------------- internal functions for sorting the data ------------------
    def _get_pending_job_list(self):
        """
        this function returns the pending job list

        :return: a new job list only has the pending jobs
        """
        new_list = [x for x in self.jobs_list if x.general_state == JOB_STATUS_PD]
        return new_list

    def _get_finished_jobs_list_in_time_window(self, begin, end):
        """
        from the running job list, check whether there are jobs will finish
        between the input begin and end time window
        :param begin(Datetime): the beginning of the time window
        :param end(Datetime): the end of hte time window
        :return: a new job list
        """
        new_list = []
        current = datetime.now()
        for x in self.jobs_list:
            if x.general_state == JOB_STATUS_RUN:
                job_end_time = current + timedelta(minutes=int(x.job_remaining_time))
                if begin < job_end_time < end:
                    new_list.append(x)
        return new_list

    def _get_total_gpu_num(self):
        """
        for the current node list, get the total number of gpu cards
        :return: the total number of gpu cards
        """
        total_gpu_num = 0
        for node in self.nodes_list:
            total_gpu_num += node.ngpus
        return total_gpu_num

    def _get_number_similar_pending_jobs(self, requirement):
        """
        let's see how many jobs has similar resources requirement

        similar resource requirement means it use same, or smaller resources
        comparing with the input job requirement

        :param requirement: the job requirement
        :return: number of similar jobs
        """
        # check the similar jobs in pending list
        # get the job resources requirement
        ncpus = requirement.ncpus
        ngpus = requirement.ngpus
        mem_required = requirement.total_memory

        # get current pending job list
        pending_jobs = self._get_pending_job_list()

        # let's see how many jobs has similar resources requirement
        # similar resource requirement means it use same, or smaller resources
        # comparing with this job
        n_similar_jobs = 0
        for job in pending_jobs:
            if job.cpu_used <= ncpus and job.gpu_used <= ngpus and job.memory_used <= mem_required:
                n_similar_jobs += 1

        # return the number
        return n_similar_jobs

    def _checking_job_availability_in_the_node_list(self, nodes_list, requirement):
        """
        for the job requirement, let's check how many available slots in the input
        node list. The node list can be reflecting current usage, or it's in the future
        snapshot view (see the self.snapshots_node_list)
        :param nodes_list: the input node list, the data is read only
        :param requirement: job requirement
        :return: number of available slots to fit this job requirement
        """
        gpu_select  = requirement.ngpus
        cpu_select  = requirement.ncpus
        mem_select  = requirement.total_memory
        avail_slots = 0
        for node in nodes_list:
            mem_remain = node.get_memory_unused()
            cpus_remain = node.get_cpus_unused()
            gpus_remain = node.get_gpus_unused()
            if gpu_select > 0:
                if mem_remain >= mem_select and cpus_remain >= cpu_select and gpus_remain >=gpu_select:
                    mem_avail = int(mem_remain/mem_select)
                    cpu_avail = int(cpus_remain/cpu_select)
                    gpu_avail = int(gpus_remain/gpu_select)
                    avail_slots += min(mem_avail, cpu_avail, gpu_avail)
            else:
                if mem_remain >= mem_select and cpus_remain >= cpu_select:
                    mem_avail = int(mem_remain/mem_select)
                    cpu_avail = int(cpus_remain/cpu_select)
                    avail_slots += min(mem_avail, cpu_avail)

        # return results
        return avail_slots


    # ------------- job snapshots related internal functions ------------------
    def _generate_job_snapshots(self):
        """
        based on the finished nodes list, and job list, we are able to form
        the snapshots for the future resources

        :return: two lists, one list contains the starting time for each time window
        another list is list of list, each element in the list is a list of Node objects
        that reflecting the future node status for the given time window
        """

        # this is the result
        result = []

        # this is the time interval list
        time_interval_list = []

        # get the config
        conf = self._job_snapshots_config
        interval = int(conf['job_snapshots_time_interval'])
        num_snapshots = int(conf['number_job_snapshots'])

        # get current time
        # let me trucate it to only minutes
        current = datetime.now().replace(second=0, microsecond=0)

        # firstly generate the time interval list
        # as estimation we take the seconds and microseconds down
        for n in range(num_snapshots):
            begin = current + n*timedelta(minutes=interval)
            time_interval_list.append(begin)

        # loop over the time interval and generate the data
        pos = -1
        for begin in time_interval_list:

            # set begin and end time
            end = begin + timedelta(minutes=interval)

            # set a copy of the node list
            # we use the previous snapshot to make the new one
            if pos < 0:
                new_node_list : list[Cluster.Node] = copy.deepcopy(self.nodes_list)
            else:
                new_node_list : list[Cluster.Node] = copy.deepcopy(result[pos])

            # now let's increment the pos
            pos = pos + 1

            # get the job list that end in this time interval
            new_job_list : list[Cluster.Job] = self._get_finished_jobs_list_in_time_window(begin, end)

            # updating the new node list with the new job list
            for job in new_job_list:
                nodes = job.compute_nodes
                for node in new_node_list:
                    node_name = node.name
                    if node_name in nodes:
                        nnodes = len(nodes)
                        if (job.gpu_used / nnodes).is_integer():
                            ngpus_per_node = int(job.gpu_used / nnodes)
                        else:
                            raise RuntimeError("the number of gpus per node for the job should be "
                                               "integer: ".format(job.gpu_used / nnodes))
                        if (job.cpu_used / nnodes).is_integer():
                            ncpus_per_node = int(job.cpu_used / nnodes)
                        else:
                            raise RuntimeError("the number of cpus per node for the job should be "
                                               "integer: ".format(job.cpu_used / nnodes))
                        if (job.memory_used / nnodes).is_integer():
                            mem_per_node = int(job.memory_used / nnodes)
                        else:
                            raise RuntimeError("the memory usage per node for the job should be "
                                               "integer: ".format(job.memory_used / nnodes))
                        node.njobs -= 1
                        node.gpus_in_use -= ngpus_per_node
                        node.cores_in_use -= ncpus_per_node
                        node.memory_in_use -= mem_per_node

            # the result is formed, push it into the result
            result.append(new_node_list)

        # finally return the results
        return time_interval_list, result

    # ------------- get the cluster overview, external use functions ---------------------
    def get_cluster_overview(self):
        """
        this function returns a cluster overview
        :return: a dict that describes the availability of gpu resources
        """
        total_gpu_num = self._get_total_gpu_num()
        gpu_selections = [1, 2, 4, 6, 8]
        result = {}
        for gpu_select in gpu_selections:
            available_slots = 0
            for node in self.nodes_list:
                gpus_remain = node.get_gpus_unused()
                if gpus_remain >= gpu_select:
                    available_slots += int(gpus_remain/gpu_select)

            # update the result
            percentage = available_slots/total_gpu_num
            result[gpu_select] = (available_slots, percentage)

        # return the result
        return result

    # ------------- get the job availability check, external use functions ---------------------
    def get_job_availability_check(self, requirement):
        """
        for the input job requirement, let's return the availability for the job
        :param requirement: input job requirement
        :return: a dict that for the gpus, memory etc. selection what is the availability in
        cluster
        """

        # set up the result
        result = {}

        # checking gpu
        gpu_availability = 0
        gpu_select = requirement.ngpus
        if gpu_select > 0:
            for node in self.nodes_list:
                gpus_remain = node.get_gpus_unused()
                if gpus_remain >= gpu_select:
                    gpu_availability += int(gpus_remain/gpu_select)

            # update the result
            result["gpu"] = gpu_availability

        # cpu cores
        cpu_availability = 0
        cpu_select = requirement.ncpus
        if cpu_select > 0:
            for node in self.nodes_list:
                cpus_remain = node.get_cpus_unused()
                if cpus_remain >= cpu_select:
                    cpu_availability += int(cpus_remain/cpu_select)

            # update the result
            result["cpu"] = cpu_availability

        # memory availability
        mem_availability = 0
        mem_select = requirement.total_memory
        if mem_select > 0:
            for node in self.nodes_list:
                mem_remain = node.get_memory_unused()
                if mem_remain >= mem_select:
                    mem_availability += int(mem_remain/mem_select)

            # update the result
            result["mem"] = mem_availability

        # update the result for checking the availability for current job
        result['job'] = self._checking_job_availability_in_the_node_list(self.nodes_list,
                                                                         requirement)

        # update the result
        result['similar'] = self._get_number_similar_pending_jobs(requirement)

        # return the result
        return result

    # ------------- get the job estimation, external use functions ---------------------
    def get_job_estimation_landing(self, requirement):
        """
        for the input job requirement, let's return the estimated landing time for the
        job, this will consider other pending jobs, too
        """

        # get the job resources requirement
        ncpus = requirement.ncpus
        ngpus = requirement.ngpus
        mem_required = requirement.total_memory

        # get number of similar jobs in pending list
        n_similar = self._get_number_similar_pending_jobs(requirement)

        # availability slots
        avail_slots = {}
        pos = 0
        for snapshot in self.snapshots_node_list:

            # get the time data
            begin_time = self.time_interval_list[pos]

            # checking the available slots
            avail_slot_num = self._checking_job_availability_in_the_node_list(snapshot,requirement)
            if avail_slot_num > n_similar:
                avail_slots[begin_time] = avail_slot_num

            # increase the pos
            pos = pos + 1

        # finally return the result
        return avail_slots


    # ------------- generate lsf script for job, external use functions ------------------
    def generate_job_script(self, requirement, output):
        """
        this function generate the final output file
        :param requirement job requirement (JobRequirements)
        :param output the result output file
        :return: the result job script will be in the output file
        """

        # we use the queue name from lsf
        conf = self._config
        queue_name = conf['queue_name']

        # Check if the file exists
        if os.path.exists(output):
            info = f"Error: The job script '{output}' already exists. Aborting write operation."
            raise IOError(info)
        else:
            # Open the file for writing
            with open(output, 'w') as f:

                # write the lsf job script
                f.write("#!/bin/bash")
                f.write("\n")

                # cpu cores
                f.write("#BSUB -n {}".format(requirement.ncpus))
                f.write("\n")

                # number of host
                # we assume number of host is 1
                f.write("#BSUB -R \'span[hosts=1]\'")
                f.write("\n")

                # queue information
                f.write("#BSUB -q {}".format(queue_name))
                f.write("\n")

                # gpu
                if requirement.ngpus > 0:
                    gpu_line = "num={}:mode=shared:mps=no:j_exclusive=yes".format(requirement.ngpus)
                    f.write("#BSUB -gpu {}".format(gpu_line))
                    f.write("\n")

                # memory
                mem_per_core = requirement.total_memory/requirement.ncpus
                f.write("#BSUB -R \'rusage[mem={}GB]\'".format(mem_per_core))
                f.write("\n")

            print(f"File '{output}' created and written successfully.")
