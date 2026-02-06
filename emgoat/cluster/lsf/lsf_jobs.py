"""
This file is to parse the bjobs output from lsf, this is the currently running/pending jobs list
"""
import json
from emgoat.config import get_config
from emgoat.util import NOT_AVAILABLE
from emgoat.util import run_command, get_job_general_status, generate_json_data_file, read_json_data_file, need_newer_data_file
from .functions import *

#
# constants that from configuration
#
LSF_COFNIG = get_config()

def run_bjobs_get_alljobs():
    """
    run the bjobs command to get all of job information.
    command line arguments format is derived from the config

    :return: the raw output for the bjobs, the output is in json format
    """
    args = LSF_COFNIG['lsf']['bjobs'].split()
    other_args = ["-q", LSF_COFNIG['lsf']['queue_name'], "-o", LSF_COFNIG['lsf']['bjobs_output_format']]
    args = args + other_args
    return run_command(args)

def parse_bjobs_output_for_alljobs(output):
    """
    parsing the output of bjobs
    :param output: the output data in json format in string
    :return: a list of dict that contains the job name and user names etc. information
    """
    global LSF_COFNIG

    # load in the raw output to json format output for further parsing
    data = json.loads(output)

    # this is the job information
    # each result is a Jobs object
    job_list = []

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

        # the start time could be None
        start_time_str = NOT_AVAILABLE
        if start_time is not None:
            start_time_str = start_time.isoformat()

        # now we have everything, building the dict
        # further change the datetime into string
        job_infor = {
            'jobid': jobid,
            'job_name': job_name,
            'submit_time': submit_time.isoformat(),
            'state': status,
            'general_state': get_job_general_status(status),
            'pending_time': pending_time,
            'job_remaining_time': remaining_time,
            'start_time': start_time_str,
            'used_time': running_time,
            'cpu_used': ncpus_request,
            'gpu_used': gpu_used,
            'memory_used': mem,
            'compute_nodes': get_hostnames_from_bjobs_output(ori_host_name),
            'account_name': account_name
        }

        # add in result
        job_list.append(job_infor)

    # now return
    return job_list

def set_job_info():
    """
    this is the driver function for the lsf_jobs. It will return the job list, each job
    is a dict as described in parse_bjobs_output_for_alljobs
    """
    global LSF_COFNIG

    # get the data file name
    file_name = LSF_COFNIG['lsf']['jobs_data_file_name']
    path_name = LSF_COFNIG['lsf']['data_output_dir']
    fname = path_name + "/" + file_name
    time = int(LSF_COFNIG['lsf']['jobs_data_update_time'])

    # whether we have the file
    if not need_newer_data_file(fname, time):
        data = read_json_data_file(fname)
        return data

    # now let's generate the file
    output = run_bjobs_get_alljobs()
    jobs_list = parse_bjobs_output_for_alljobs(output)

    # save the data
    generate_json_data_file(jobs_list, fname)
    return jobs_list

