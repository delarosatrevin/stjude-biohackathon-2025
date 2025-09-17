"""
This file is used to collect/parse the job information from bjobs command
"""
from util.util import run_command,get_time_data_from_lsf_output
from util.macros import JOB_STATUS_DONE, JOB_STATUS_PD, JOB_STATUS_RUN
from util.util import convert_str_to_integer,get_hostname_from_bjobs_output,convert_lsf_time_to_minutes
from util.util import whether_job_is_running,whether_job_is_pending,whether_job_is_suspending
from util.config import get_config
from cluster.job import Jobs
from cluster.account import Accounts
import json

##########################
# get the global conf
##########################
CONFIG = get_config()

def run_bjobs_get_alljobs():
    """
    run the bjobs command to get all of job information.
    command line arguments format is derived from the config

    :return: the raw output for the bjobs, the output is in json format
    """
    global CONFIG

    # get the argument list
    # we will run from current node
    arg = CONFIG['lsf']['bjobs'].split() + [" -q ", CONFIG['lsf']['queue_name'], "-o", CONFIG['lsf']['bjobs_output_format']]
    output = run_command(arg)
    return output

def parse_bjobs_output_for_alljobs(output):
    """
    parsing the output of bjobs
    :param output: the output data in json format in string
    :return: a list of dict that contains the job name and user names etc. information
    """
    global CONFIG

    # load in the raw output to json format output for further parsing
    data = json.loads(output)

    # number of jobs
    nJobs = int(data['JOBS'])

    # this is the job information
    # each result is a Jobs object
    job_list = []

    # this is the account list currently in use
    account_list = []

    # now let's form the Jobs
    for n in range(nJobs):

        # load in the json data
        jobid            = data['RECORDS'][n]['JOBID']
        status           = data['RECORDS'][n]['STAT']
        account_name     = data['RECORDS'][n]['USER']
        job_name         = data['RECORDS'][n]['JOB_NAME']
        submit_time      = get_time_data_from_lsf_output(data['RECORDS'][n]['SUBMIT_TIME'])
        start_time       = get_time_data_from_lsf_output(data['RECORDS'][n]['START_TIME'])
        pending_time     = int(convert_str_to_integer(data['RECORDS'][n]['PEND_TIME'])/60)
        ori_time_left    = get_time_data_from_lsf_output(data['RECORDS'][n]['TIME_LEFT'])
        ori_running_time = data['RECORDS'][n]['RUN_TIME']
        ncpus_request    = convert_str_to_integer(data['RECORDS'][n]['NREQ_SLOT'])
        ori_mem_request  = data['RECORDS'][n]['MEMLIMIT']
        gpu_used         = convert_str_to_integer(data['RECORDS'][n]['GPU_NUM'])
        nhosts           = convert_str_to_integer(data['RECORDS'][n]['NEXEC_HOST'])
        ori_host_name    = data['RECORDS'][n]['EXEC_HOST']

        # running time data
        running_time = 0
        if ori_running_time.find("second") > 0:
            running_time = int(convert_str_to_integer(ori_running_time.split()[0])/60)

        # remaining time
        # in default if we do not have the data, it's a very big number
        if ori_time_left.find(":") > 0:
            if ori_time_left.find("L") > 0:
                remaining_time = convert_lsf_time_to_minutes(ori_time_left.strip().split()[0])
            else:
                remaining_time = convert_lsf_time_to_minutes(ori_time_left)
        else:
            raise RuntimeError("Invalid time left data from bjobs passed in: {}".format(ori_time_left))

        # memory
        # we only handle the GB/TB cases, other cases we will issue an error
        if ori_mem_request.lower().find("g") > 0:
            mem = convert_str_to_integer(ori_mem_request.split()[0])
        elif ori_mem_request.lower().find("t") > 0:
            mem = convert_str_to_integer(ori_mem_request.split()[0])*1024
        else:
            raise RuntimeError("Invalid memory requested passed in: {}".format(ori_mem_request))

        # get the general status
        general_status = JOB_STATUS_DONE
        if whether_job_is_pending(status) or whether_job_is_suspending(status):
            general_status = JOB_STATUS_PD
        if whether_job_is_running(status):
            general_status = JOB_STATUS_RUN

        # host name
        if nhosts > 1:
            raise RuntimeError("It seems the job used more than one node, currently we do not know how to get the data")
        host_name = get_hostname_from_bjobs_output(ori_host_name)

        # user name, currently it's same with account name
        job_user = account_name

        # now we have everything, building the Jobs object
        job_infor = Jobs(
            jobid = jobid,
            job_name = job_name,
            submit_time = submit_time,
            state = status,
            general_state = general_status,
            pending_time = pending_time,
            job_remaining_time = remaining_time,
            start_time=start_time,
            used_time = running_time,
            cpu_used = ncpus_request,
            gpu_used = gpu_used,
            memory_used = mem,
            compute_nodes = host_name,
            user = job_user,
            account_name = account_name
        )

        # add in result
        job_list.append(job_infor)

    # now return
    return job_list
