"""
This file is to parse the squeue output from slurm, this is the currently running/pending jobs list
"""
import json
from emgoat.config import get_config
from emgoat.util import run_command, whether_job_is_running, whether_job_is_pending, NOT_AVAILABLE
from emgoat.util import get_job_general_status, generate_json_data_file, read_json_data_file, need_newer_data_file
from .slurm_util import parse_slurm_host_names,parse_tres_data_from_json
from datetime import datetime

#
# constants that from configuration
#
slurm_COFNIG = get_config()

def run_squeue_get_alljobs():
    """
    run the squeue command to get all of running/pending job information, this is through the json output

    :return: the raw json output of the job data
    """
    args = ["squeue", "--json"]
    return run_command(args)

def parse_squeue_output_for_alljobs(output: str):
    """
    parsing the output of the above squeue command output
    :param output: the output data in squeue command in json format of string
    :return: a list of dict that contains the job name and user names etc. information
    """

    # load in the raw output to json format output for further parsing
    data = json.loads(output)

    # this is the job information
    # each result is a Jobs object
    job_list = []

    # now let's form the Jobs
    for i, record in enumerate(data['jobs']):

        # only consider the running/pending jobs
        status = record['job_state'][0]
        if not whether_job_is_running(status) and not whether_job_is_pending(status):
            continue

        # load in the json data
        jobid = record['job_id']
        account_name = record['account']
        user_name = record['user_name']
        job_name = record['name']
        submit_time_t = datetime.fromtimestamp(record['submit_time']['number'])
        requested_time = int(record['time_limit']['number'])  # this is in minutes
        tres_request = record['tres_req_str']

        # get the job allocation host list
        host_list = " "
        if whether_job_is_running(status):
            host_list = parse_slurm_host_names(record['job_resources']['nodes'])

        # get the resources data from tres
        data = parse_tres_data_from_json(tres_request)
        num_nodes = data[0]
        ncpus = data[1]
        mem_in_gb = data[2]
        ngpus = data[3]

        # double check the number of nodes with host list
        if len(host_list.split()) != num_nodes:
            raise RuntimeError("the number of nodes we get from tres_req_str is not equal to the number of hosts "
                               "in the host list in job_resources")

        # only running job has start time
        start_time_t = NOT_AVAILABLE
        running_time = 0
        if whether_job_is_running(status):
            start_time = datetime.fromtimestamp(record['start_time']['number'])
            pending_time = (start_time - submit_time_t).total_seconds()/60
            running_time = (datetime.now() - submit_time_t).total_seconds()/60
            start_time_t = start_time.isoformat()
        else:
            pending_time = (datetime.now() - submit_time_t).total_seconds()/60

        # compute time left
        left_time = requested_time - running_time

        # change the time into string
        submit_time = submit_time_t.isoformat()

        # now we have everything, building the dict
        # further change the datetime into string
        # this is to save the result into file
        job_infor = {
            'jobid': jobid,
            'job_name': job_name,
            'submit_time': submit_time,
            'state': status,
            'general_state': get_job_general_status(status),
            'pending_time': pending_time,
            'job_remaining_time': left_time,
            'start_time': start_time_t,
            'used_time': running_time,
            'cpu_used': ncpus,
            'gpu_used': ngpus,
            'memory_used': mem_in_gb,
            'compute_nodes': host_list,
            'account_name': account_name
        }

        # now add the job infor
        job_list.append(job_infor)

    # finally return
    return job_list

def set_job_info():
    """
    this is the driver function for the slurm jobs. It will return the job list, each job
    is a dict as described in the above parse function
    """
    global slurm_COFNIG

    # get the data file name
    file_name = slurm_COFNIG['slurm']['jobs_data_file_name']
    path_name = slurm_COFNIG['slurm']['data_output_dir']
    fname = path_name + "/" + file_name
    time = int(slurm_COFNIG['slurm']['jobs_data_update_time'])

    # whether we have the file
    if not need_newer_data_file(fname, time):
        data = read_json_data_file(fname)
        return data

    # now let's generate the file
    output = run_squeue_get_alljobs()
    jobs_list = parse_squeue_output_for_alljobs(output)

    # save the data
    generate_json_data_file(jobs_list, fname)
    return jobs_list

