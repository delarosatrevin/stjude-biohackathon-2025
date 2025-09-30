
import re
import json

import emgoat
from emgoat.util import Config, run_command, VERY_BIG_NUMBER, LSF_CLUSTER, GPU_TYPE, get_job_general_status, \
    JOB_STATUS_PD, JOB_STATUS_RUN

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

                    # whether this is for GB/TB size of mem?
                    # the value sometimes is a floating number
                    if mem.find("G") > 0 or mem.find("g") > 0:
                        val = re.sub(r"\D", "", mem)
                        v1  = int(float(val))
                    elif mem.find("T") > 0 or mem.find("t") > 0:
                        val = re.sub(r"\D", "", mem)
                        v1 = int(float(val) * 1024)
                    else:
                        raise RuntimeError("The input memory value should be in unit of GB: {}".format(mem))

                    # now let's update
                    mem_value = str(v1)
                    node.ncpus = ncpus
                    node.total_mem_in_gb = mem_value

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
                new_node_list = self.nodes_list.copy()
            else:
                new_node_list = result[pos].copy()

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



