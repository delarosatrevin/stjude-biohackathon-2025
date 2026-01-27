
import emgoat
from emgoat.util import Config
from emgoat.util import NOT_AVAILABLE
from emgoat.cluster.lsf.lsf_jobs import *
from emgoat.cluster.lsf.lsf_hosts import *
from ..base import Cluster as BaseCluster
from datetime import datetime


class Cluster(BaseCluster):
    """ Cluster implementation for LSF system. """
    _name = "lsf"
    _config = Config(emgoat.config['lsf'])
    _job_snapshots_config = Config(emgoat.config['snapshots'])

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
        super().update_node_with_job_info(node_list=self.nodes_list, job_list=self.jobs_list)

        # finally generate the summary based on the output results
        self.summary = super().Summary(self.nodes_list, self.jobs_list)


    def _transform_node_list_infor(self, nodes_infor):
        """
        This function transform the input node information list into the list of Node
        """

        # here each node is a dict, see the form_nodes_infor_list_from_node_names
        # function in lsf_hosts.py for more information
        nodes_list = []
        for node in nodes_infor:
            n = self.Node(node['name'], node['gpu_type'], node['ngpus'], node['ncpus'], node['mem_in_gb'])
            nodes_list.append(n)

        # return
        return nodes_list

    def _transform_jobs_list_infor(self, jobs_infor):
        """
        This function transform the input jobs information list into the list of Job
        """
        config = self._config
        time_format = config['time_format']

        # here each node is a dict, see the form_nodes_infor_list_from_node_names
        # function in lsf_hosts.py for more information
        jobs_list = []
        for job in jobs_infor:

            # firstly generate the datetime
            submit = datetime.strptime(job['submit_time'], time_format)
            if job['start_time'] == NOT_AVAILABLE:
                start_time = None
            else:
                start_time = datetime.strptime(job['start_time'], time_format)

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
        global LSF_COFNIG

        # this is the json result file
        json_result = LSF_COFNIG['lsf']['json_result_path']

        # set up the result structure
        node_list = self.get_nodes_info()
        jobs_list = self.get_jobs_info()
        acc_list = self.get_accounts_info()
        summary = self.get_cluster_summary_info()
        result = {"nodes": node_list, "jobs": jobs_list, "accounts": acc_list, "summary": summary}

        # write it into json file
        with open(json_result, 'w') as infor:
            json.dump(result, infor, indent=4)









