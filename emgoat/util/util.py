
"""
This file stores the utility functions 
"""
import os
import subprocess
import csv
from pwd import getpwnam
import importlib
from importlib.machinery import SourceFileLoader

from datetime import datetime
from .macros import VERY_BIG_NUMBER, GPU_TYPE
from .macros import JOB_STATUS_DONE, JOB_STATUS_PD, JOB_STATUS_RUN


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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            user=uid
        )
    else:
        proc = subprocess.Popen(
            arglist,
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


class Config:
    """ Helper class to parse ConfigParser options"""
    def __init__(self, config):
        self._config = config

    def __getitem__(self, key):
        return self._config[key]

    def get_bool(self, key):
        val = self._config[key].lower()
        if val in ['1', 'true']:
            return True
        elif val in ['0', 'false']:
            return False
        else:
            raise RuntimeError(f"Invalid bool value for option {key} = {self._config['key']}")

    def get_list(self, key):
        return self._config[key].split()

    def print_all(self):
        """ Method to debug printing all values. """
        for k in self._config:
            print(f"{k} = {self._config[k]}")


class Loader:
    @staticmethod
    def load_from_file(module_path):
        """ Load a module from a given file path. """
        if not os.path.exists(module_path):
            raise Exception("Missing file: " + module_path)

        loader = SourceFileLoader("jobfile", module_path)

        if spec := importlib.util.spec_from_loader("jobfile", loader):
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        else:
            raise Exception("Invalid python file: " + module_path)

    @staticmethod
    def load_from_string(import_string):
        """" Load a module or a class from a given import string. """
        return importlib.import_module(import_string)


def get_dict_from_args(args, start_index=2):
    """ Get a dictionary from the parts after shlex.split of
    the command string.
    """
    cmd_dict = {}
    for p in args[start_index:]:
        if p.startswith('--'):
            last_key = p
            cmd_dict[p] = ''
        else:
            v = cmd_dict[last_key]

            if v:
                if isinstance(v, list):
                    v.append(p)
                else:
                    v = [v, p]
            else:
                v = p
            cmd_dict[last_key] = v

    return cmd_dict