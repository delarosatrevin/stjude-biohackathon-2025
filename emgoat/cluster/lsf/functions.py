
"""
This file stores the utility functions 
"""
from datetime import datetime

from emgoat.util import (VERY_BIG_NUMBER, JOB_STATUS_PD, JOB_STATUS_RUN, JOB_STATUS_DONE,
                         convert_str_to_integer, convert_percentage_to_decimal)


def get_job_general_status(status):
    # get the general status
    if whether_job_is_pending(status) or whether_job_is_suspending(status):
        return JOB_STATUS_PD
    elif whether_job_is_running(status):
        return JOB_STATUS_RUN
    else:
        return JOB_STATUS_DONE


def whether_job_is_pending(state):
    """
    testing whether for the given input state for a job, it's in a "pending state"

    pending state symbols are taken from the links below:

    https://slurm.schedmd.com/squeue.html
    https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=execution-about-job-states

    :param state: string that representing a job state
    :return: true if a job state is not in finished state
    """
    status = ["pend", "pending", "pd", "configuring", "cf"]
    return state.lower() in status


def whether_job_is_suspending(state):
    """
    testing whether for the given input state for a job, it's in a "suspending state"

    suspending state symbols are taken from the links below:

    https://slurm.schedmd.com/squeue.html
    https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=bjobs-description

    in slurm requeued jobs considered to be suspending, too

    :param state: string that representing a job state
    :return: true if a job state is not in finished state
    """
    status = ["psusp", "ususp", "ssusp", "prov", "wait", "suspended", "s", "requeued", "rq",
              "requeue_hold", "rh", "resv_del_hold", "rd", "requeue_fed", "rf"]
    return state.lower() in status


def whether_job_is_running(state):
    """
    testing whether for the given input state for a job, it's in a "running state"

    we also use it to test the service state, if it's in running

    state symbols are taken from the links below:

    https://slurm.schedmd.com/squeue.html
    https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=bjobs-description

    :param state: string that representing a job state
    :return: true if a job state is not in finished state
    """
    status = state.lower()
    return status == "r" or status.find("run") >= 0


def whether_job_is_finished(state):
    """
    testing whether for the given input state for a job, it's in a "finished state". The finished state
    could be in cancelled, dead, timeout, or successfully finished (like done)

    :param state: string that representing a job state
    :return: true if a job state is not in finished state
    """
    return not (whether_job_is_running(state) or
                whether_job_is_pending(state) or
                whether_job_is_suspending(state))


def get_time_data_from_lsf_output(data):
    """
    here input data should be in format of "Jul 29 11:30", this is a typical
    time format used in LSF

    if the input is empty string, or space; we return None

    :param data: input time string
    :return: the given datetime object
    """

    # sometimes the input data is empty
    # if so we just return a None value
    if not data.strip():
        return None

    # split the input data into fields
    fields = data.split()
    if len(fields) != 3:
        info = ("Invalid input data for parsing, it should be exactly in the "
                "format like Jul 29 11:30, no year")
        raise RuntimeError(info)

    # now get the time
    format_data = "%b %m %H:%M"
    t = datetime.strptime(data, format_data)
    return t

"""
    this is the old way we implement it

    # constant for the month
    m = {
        'jan': 1,
        'feb': 2,
        'mar': 3,
        'apr':4,
        'may':5,
        'jun':6,
        'jul':7,
        'aug':8,
        'sep':9,
        'oct':10,
        'nov':11,
        'dec':12
    }

    # sometimes the input data is empty
    # if so we just return a None value
    if not data.strip():
        return None

    # split the input data into fields
    fields = data.split()
    if len(fields) != 3:
        info = ("Invalid input data for parsing, it should be exactly in the "
                "format like Jul 29 11:30, no year")
        raise RuntimeError(info)

    # now the data
    ori_month = fields[0]
    ori_day   = fields[1]
    ori_time  = fields[2]

    # day should be the integer
    try:
        day = int(ori_day)
    except ValueError:
        info = "Error to convert the day into integer: {}".format(ori_day)
        raise RuntimeError(info)

    # now let's month
    if ori_month.lower() in m:
        month = m[ori_month.lower()]
    else:
        info = "The input month abbreviation is wrong: {}", ori_month
        raise RuntimeError(info)

    # finally it's time
    if ori_time.find(":") > 0:
        x = ori_time.split(":")
        if not x[0].isnumeric() or not x[1].isnumeric():
            info = "Something wrong with the input time, not numerical data: {}", ori_time
            raise RuntimeError(info)
        hour   = int(x[0])
        minute = int(x[1])
    else:
        info = "Something wrong with the input time format: {}", ori_time
        raise RuntimeError(info)

    # in case the month is for last year
    if datetime.now().month >= month:
        t = datetime(year=datetime.now().year, month=month, day=day, hour=hour, minute=minute)
    else:
        t = datetime(year=datetime.now().year - 1, month=month, day=day, hour=hour, minute=minute)

    # finally return
    return t
"""

def convert_lsf_time_to_minutes(input):
    """
    a simple function to convert the input string to time in minutes
    :param input: the format is strict, must be something like 123:45, first one is hour; second one is minutes
    :return: an integer number representing in minutes
    """
    # if input is empty, return a very big number
    # usually if the data is missing that means we do not have the data
    if not input.strip():
        return VERY_BIG_NUMBER

    # convert
    if input.find(":") > 0:
        s = input.strip().split(":")
        v0 = convert_str_to_integer(s[0])
        v1 = convert_str_to_integer(s[1])
    else:
        info = ("The input string must be in this format: 123:45, first is hour, second is minutes. "
                "Original data is here: {}", input)
        raise RuntimeError(info)

    # now return the number
    return v0 * 60 + v1


def get_hostnames_from_bjobs_output(input):
    """
    get host names from the bjobs output
    :param input: input host name string, from bjobs output
    :return:  the host name
    """
    hostnames = []

    # if input is empty, return None
    if s := input.strip():
        parts = s.split(':') if ':' in s else [s]
        for p in parts:
            if '*' in p:
                hostnames.append(p.split('*')[1])
            else:
                raise RuntimeError(f"Invalid string for paring to get hostname: {input}")

    return hostnames
