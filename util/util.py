
"""
This file stores the utility functions 
"""
import subprocess
from pwd import getpwnam
from typing import Final
from datetime import datetime
import csv

def run_command(arglist, user_name=None, timeout=60):
    """
    Run a specified command with arguments and return
    standard output decoded to UTF-8.

    The subprocess can also be launched with another user and group. 
    In default if the user and group are None, then the command
    will run in normal way, otherwise it will run under another user's 
    name. Make sure you have permission to run the commands under
    the input user!!!

    :param list(str) arglist: List of command and arguments
    :param user_name(str): run command under another user name
    :param int timeout: set the timeout to this many seconds(default 120)
    :returns: Output of the command decoded to utf-8
    """

    # checking whether the user exists? also get the corresponding uid
    uid = None
    if user_name is not None:
        try:
            uid = getpwnam(user_name).pw_uid
        except:
            info = 'The input user name can not be validated on the OS: {}'.format(user_name)
            raise KeyError(info)

    # all of output and error directed to the pipe
    # no standard input needed
    if user_name is not None:
        proc = subprocess.Popen(
            arglist,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            user=uid
        )
    else:
        proc = subprocess.Popen(
            arglist,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL
        )

    # run the command, and return the output
    out, err = proc.communicate(timeout=timeout)
    if proc.returncode != 0:
        info = 'Error running command {0}: {1}'.format(arglist, err)
        raise IOError(info)

    # return
    return out.decode('utf-8')

def get_time_data_from_lsf_output(data):
    """
    here input data should be in format of "Jul 29 11:30", this is a typical
    time format used in LSF

    if the input is empty string, or space; we return None

    :param data: input time string
    :return: the given datetime object
    """

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

def convert_percentage_to_decimal(input):
    """
    convert the input percentage string into a decimal
    :param input: number in string with percentage sign
    :return: a decimal
    """
    # if input is empty, return 0
    if not input.strip():
        return 0.0

    # if we have something, then percentage sign should be there
    if input.find("%") < 0:
        info = "Failed to find the percentage sign % in the input string for convert"
        raise RuntimeError(info)

    # now everything good, let's do the job
    decimal_value = float(input.strip('%')) / 100
    return decimal_value

def convert_str_to_integer(input):
    """
    a simple function to convert the input string to integer number
    :param input: input string, should be all digit
    :return: an integer number
    """
    # if input is empty, return 0
    if not input.strip():
        return 0

    # convert
    s = input.strip()
    if s.isdigit():
        num = int(s)
    else:
        info = "The input string is not numeric: {}", input
        raise RuntimeError(info)
    return num

def get_hostname_from_bjobs_output(input):
    """
    get the host name from the bjobs output
    :param input: input host name string, from bjobs output
    :return:  the host name
    """
    # if input is empty, return None
    if not input.strip():
        return None

    # the string in format like 3*nodecem123, so node name is nodecem123
    s = input.strip()
    if s.find('*') >=0:
        return s.split('*')[1]
    else:
        info = "Invalid string for paring to get hostname: {}".format(input)
        raise RuntimeError(info)

def read_data_from_csv(file_name):
    """
    reading data from the input csv file

    we note, that the csv file must have header line; and should follow the example below:

    x, y, z,  w (header line)
    a1, a2, a3, a4
    b1, b2, b3, b4
    .........

    the result will be a list of dict, for example
    [ {x: a1, y: a2, z: a3, w: a4}, {x: b1, y: b2, z: b3, w: b4} ... ]

    :param file_name: input csv file name
    :return: a list of dict
    """
    data_list = []  # List to store dictionaries
    with open(file_name, mode='r') as file:
        csv_reader = csv.DictReader(file)  # Create DictReader
        for row in csv_reader:
            data_list.append(row)
    return data_list
