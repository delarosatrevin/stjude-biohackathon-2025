
"""
This file stores the utility functions 
"""
from datetime import datetime
from emgoat.util import VERY_BIG_NUMBER, convert_str_to_integer, convert_percentage_to_decimal


def get_time_data_from_lsf_output(data):
    """
    here input data should be in format of "Jul 29 11:30", this is a typical
    time format used in LSF

    if the input is empty string, or space; we return None

    :param data: input time string
    :return: the given datetime object
    """
    
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
    # this is the old way we implement it

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
                # for this case, usually the job only request one cpu
                #raise RuntimeError(f"Invalid string for paring to get hostname: {input}")
                hostnames.append(p)

    return hostnames
