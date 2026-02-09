
import emgoat
from emgoat.util import Config
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

    def get_nodes_info(self):
        return self.nodes_list

    def get_jobs_info(self):
        return self.jobs_list

    def get_accounts_info(self):
        return self.accounts_list

    def get_cluster_summary_info(self):
        return self.summary

    def __init__(self):
        """
        initialization of lsf cluster
        """

        # cluster type
        super().__init__()

        # get the nodes information
        node_list = get_nodes_info()
        self.nodes_list = self._transform_node_list_infor(node_list)

        # get the jobs information
        jobs_list = set_job_info()
        self.jobs_list = self._transform_jobs_list_infor(jobs_list)

        # set up the account list
        self.accounts_list = super().form_accounts_infor(self.jobs_list)

        # update the nodes information with jobs
        self._update_node_with_job_info()

        # finally generate the summary based on the output results
        self.summary = super().Summary(self.nodes_list, self.jobs_list)

    def _update_node_with_job_info(self):
        """
        this function will further update the node with the job information
        """
        for node in self.nodes_list:

            # get the node name
            node_name = node.name

            # get the job landing on the node
            for job in self.jobs_list:

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
        json_result = config['json_result_path']

        # set up the result structure
        node_list = [x.to_dict() for x in self.get_nodes_info()]
        jobs_list = [x.to_dict() for x in self.get_jobs_info()]
        acc_list  = [x.to_dict() for x in self.get_accounts_info() if x.has_any_jobs()]
        summary   = self.get_cluster_summary_info().to_dict()
        result = {"summary": summary, "nodes": node_list, "accounts": acc_list, "jobs": jobs_list}

        # write it into json file
        with open(json_result, 'w') as infor:
            json.dump(result, infor, indent=4)









