
import emgoat
from emgoat.util import Config
from emgoat.util import NOT_AVAILABLE
from emgoat.cluster.slurm.slurm_jobs import *
from emgoat.cluster.slurm.slurm_hosts import *
from ..base import Cluster as BaseCluster
from datetime import datetime

class Cluster(BaseCluster):
    """ Cluster implementation for Slurm system. """
    _name = "slurm"
    _config = Config(emgoat.config['slurm'])
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
        initialization of slurm cluster
        """

        # cluster type
        super().__init__()

        # get the nodes information
        node_list = get_nodes_info()
        jobs_list = set_job_info()
        self.nodes_list = self._transform_node_list_infor(node_list, jobs_list)

        # get the jobs information
        self.jobs_list = self._transform_jobs_list_infor(jobs_list)

        # set up the account list
        self.accounts_list = super().form_accounts_infor(self.jobs_list)

        # finally generate the summary based on the output results
        self.summary = super().Summary(self.nodes_list, self.jobs_list)


    def _transform_node_list_infor(self, nodes_infor, jobs_infor):
        """
        This function transform the input node information list into the list of Node
        """

        # set up the job infor list on each node
        # key is the node name, value is the corresponding number of jobs on the node
        job_infor_list = {x['name']: 0 for x in nodes_infor}
        for node_name in job_infor_list:
            for job in jobs_infor:
                node_names_list = job['compute_nodes']
                if node_names_list.find(node_name) >=0:
                    job_infor_list[node_name] += 1

        # here each node is a dict, see the form_nodes_infor_list_from_node_names
        # function in lsf_hosts.py for more information
        nodes_list = []
        for node in nodes_infor:
            name = node['name']
            njobs = job_infor_list[name]
            n = self.Node(name, node['gpu_type'], node['status'], node['ngpus'], node['n_used_gpus'],
                          node['ncpus'], node['n_used_cpus'], node['mem_in_gb'], node['used_mem_in_gb'],
                          njobs,True)
            nodes_list.append(n)

        # return
        return nodes_list

    def _transform_jobs_list_infor(self, jobs_infor):
        """
        This function transform the input jobs information list into the list of Job

        for slurm job output, it uses iso format of time so no time format really needed
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









