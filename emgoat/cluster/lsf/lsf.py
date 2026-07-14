
import emgoat
from emgoat.util import Config, get_lsf_job_mem_infor_in_mb
from emgoat.util import NOT_AVAILABLE, JOB_STATUS_PD
from emgoat.cluster.lsf.lsf_jobs import *
from emgoat.cluster.lsf.lsf_hosts import *
from ..base import Cluster as BaseCluster
from datetime import datetime

class Cluster(BaseCluster):
    """ Cluster implementation for LSF system. """
    _name = "lsf"
    _config = Config(emgoat.config['lsf'])
    #_job_snapshots_config = Config(emgoat.config['snapshots'])

    def get_lsf_nodes_info(self, queue):
        if queue in self.queues:
            pos = self.queues.index(queue)
        else:
            pos= -1

        # return
        if pos < 0:
            raise RuntimeError('failed to get the queue name'.format(queue))
        return self.nodes_list[pos]

    def get_lsf_jobs_info(self, queue):
        if queue in self.queues:
            pos = self.queues.index(queue)
        else:
            pos = -1

        # return
        if pos < 0:
            raise RuntimeError('failed to get the queue name'.format(queue))
        return self.jobs_list[pos]

    def get_lsf_accounts_info(self, queue):
        if queue in self.queues:
            pos = self.queues.index(queue)
        else:
            pos = -1

        # return
        if pos < 0:
            raise RuntimeError('failed to get the queue name'.format(queue))
        return self.accounts_list[pos]

    def get_lsf_cluster_summary_info(self, queue):
        if queue in self.queues:
            pos = self.queues.index(queue)
        else:
            pos = -1

        # return
        if pos < 0:
            raise RuntimeError('failed to get the queue name'.format(queue))
        return self.summary[pos]

    def __init__(self):
        """
        initialization of lsf cluster
        """

        # cluster type
        super().__init__()

        # loop over the queues
        self.queues = ["cryoem", "cryoem_cpu"]
        self.nodes_list = []
        self.jobs_list = []
        self.accounts_list = []
        self.summary = []
        for queue in self.queues:

            # get the nodes information
            node_list = get_nodes_info(queue)
            new_nodes_list = self._transform_node_list_infor(node_list)

            # get the jobs information
            jobs_list = set_job_info(queue)
            self.jobs_list.append(self._transform_jobs_list_infor(jobs_list))

            # update nodes data with job data
            self._update_node_with_job_info(new_nodes_list,jobs_list)
            self._run_lsload_update_memory_usage(new_nodes_list)
            self.nodes_list.append(new_nodes_list)

            # set up the account list
            self.accounts_list.append(super().form_accounts_infor(self.jobs_list))

            # finally generate the summary based on the output results
            self.summary.append(super().Summary(self.nodes_list, self.jobs_list))

    def _run_lsload_update_memory_usage(self, node_list):
        """
        run the lsload command to get the memory usage data. This will
        update the memory usage in the result
    
        the memory showing in lsload is the left memory level
    
        output example is
    
        HOST_NAME       status  r15s   r1m  r15m   ut    pg  ls    it   tmp   swp   mem
        xxxxxxx          ok     3.0   2.8   3.0   5%   0.0   0 10316   59G 11.9G  2.9T
        yyyyyyy          ok     51.4  71.3  65.2  75%   0.0   0 13100   59G 15.9G  904G
    
        we used the last data field of mem, that is the available memory for the node
    
        sometimes the data is not available, for example:
    
        zzzzzzz  unavailable
    
        then we do not have the data, we will set 0 with this case
        """
    
        # get the node name list
        node_name_list = [x.name for x in node_list]
    
        # run the command
        arg = ['lsload'] + node_name_list
        output = run_command(arg)
    
        # now let's parse the data
        # now update the status information in the result
        for line in output.splitlines():
            if line.find("HOST_NAME")>=0:
                continue
            data = [x for x in line.strip().split()]
    
            # now get the data
            # for some cases we can not get the data correctly, for example:
            # HOST_NAME       status  r15s   r1m  r15m   ut    pg  ls    it   tmp   swp   mem
            # nodelmr12         busy 624.5*658.9 675.4  81%   0.0   0     6   53G 15.9G  2.5T
            # nodelmr16         busy 656.7*613.8 668.5  83%   0.0   0 64984   59G 15.8G  2.4T
            #
            #
            # the normal one is like this:
            # nodegpu209          ok  81.4  85.4  53.8  51%   0.0   0  2807   59G   31G  820G
            #
            # our scheme here is to test whether we have at least 10 data in one line, if so
            # we will pick up the last data
            if len(data)<10:
                continue
    
            # get the data
            name = data[0]
            memory_left = int(get_lsf_job_mem_infor_in_mb(data[-1])/1024)
            if memory_left < 0:
                raise RuntimeError("Something wrong with the mem left with lsload on this line: {}".format(line))
            for node_infor in node_list:
                node_name = node_infor.name
                if node_name.lower() == name.lower():
                    total_mem = node_infor.total_mem_in_gb
                    if memory_left <= total_mem:
                        node_infor.update_memory_in_use(total_mem - memory_left)
                    else:
                        print("The total memory is:{0}, and the memory left is:{1}".format(total_mem, memory_left))
                        node_infor.update_memory_in_use(total_mem)
                    break
    
    def _update_node_with_job_info(self, nodes_list, jobs_list):
        """
        this function will further update the node with the job information
        """
        for node in nodes_list:

            # get the node name
            node_name = node.name

            # get the job landing on the node
            for job in jobs_list:

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
                node.update_jobs_infor(gpus_in_use=ngpus_per_node, cores_in_use=ncpus_per_node)

    def _transform_node_list_infor(self, nodes_infor):
        """
        This function transform the input node information list into the list of Node
        """

        # here each node is a dict, see the form_nodes_infor_list_from_node_names
        # function in lsf_hosts.py for more information
        # for LSF case, it does not have the usage data; so we will set it 0
        gpu_used = 0
        cpu_used = 0
        mem_used = 0
        n_jobs = 0
        with_usage_data = False
        nodes_list = []
        for node in nodes_infor:
            n = self.Node(node['name'], node['gpu_type'], node['status'], node['ngpus'], gpu_used,
                          node['ncpus'], cpu_used, node['mem_in_gb'], mem_used, n_jobs, with_usage_data)
            nodes_list.append(n)

        # return
        return nodes_list

    def _transform_jobs_list_infor(self, jobs_infor):
        """
        This function transform the input jobs information list into the list of Job
        """

        # here each node is a dict, see the form_nodes_infor_list_from_node_names
        # function in lsf_hosts.py for more information
        jobs_list = []
        for job in jobs_infor:

            # firstly generate the datetime
            submit = datetime.fromisoformat(job['submit_time'])
            if job['start_time'] == NOT_AVAILABLE:
                start_time = None
            else:
                start_time = datetime.fromisoformat(job['start_time'])

            # transform the compute nodes into a list
            compute_nodes_list = job['compute_nodes'].split()

            # now create the job
            j = self.Job(job['jobid'], job['job_name'], submit, job['state'], job['general_state'],
                         job['pending_time'], job['job_remaining_time'], start_time,
                         job['used_time'], job['cpu_used'], job['gpu_used'], job['memory_used'],
                         compute_nodes_list, job['account_name'])
            jobs_list.append(j)

        # return
        return jobs_list

    def generate_json_results(self):
        """
        this function is used to output the results into json format
        """
        # this is the json result file
        config = self._config

        # generate the gpu results
        json_result = config['json_gpu_result_path']
        queue = "cryoem"

        # set up the result structure
        node_list = [x.to_dict() for x in self.get_lsf_nodes_info(queue)]
        jobs_list = [x.to_dict() for x in self.get_lsf_jobs_info(queue)]
        acc_list  = [x.to_dict() for x in self.get_lsf_accounts_info(queue) if x.has_any_jobs()]
        summary   = self.get_lsf_cluster_summary_info(queue).to_dict()
        result = {"summary": summary, "nodes": node_list, "accounts": acc_list, "jobs": jobs_list}

        # write it into json file
        with open(json_result, 'w') as infor:
            json.dump(result, infor, indent=4)

        # generate the cpu results
        json_result = config['json_cpu_result_path']
        queue = "cryoem_cpu"

        # set up the result structure
        node_list = [x.to_dict() for x in self.get_lsf_nodes_info(queue)]
        jobs_list = [x.to_dict() for x in self.get_lsf_jobs_info(queue)]
        acc_list  = [x.to_dict() for x in self.get_lsf_accounts_info(queue) if x.has_any_jobs()]
        summary   = self.get_lsf_cluster_summary_info(queue).to_dict()
        result = {"summary": summary, "nodes": node_list, "accounts": acc_list, "jobs": jobs_list}

        # write it into json file
        with open(json_result, 'w') as infor:
            json.dump(result, infor, indent=4)









