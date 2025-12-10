#
# this file records the host related LSF functions and calls, like bhost and lshost
# it will also analyze the output and save them in json format for future use
#
import os
import json
import copy
import emgoat
from emgoat.util import Config, run_command, GPU_TYPE, get_job_general_status
from emgoat.util import JOB_STATUS_PD, JOB_STATUS_RUN, is_str_float, is_str_integer
from emgoat.util import convert_float_to_integer, convert_str_to_integer

from .functions import *
from ..base import Cluster as BaseCluster
from datetime import datetime, timedelta

#
# constants that points to LSF configuration
#
LSF_COFNIG = Config(emgoat.config['lsf'])


def run_bhosts_get_gpu_info():
    """
    run the bhosts command to get node list and each node gpu information.
    command line arguments format is derived from the config

    :return: the raw output for the bhost command
    """
    global LSF_COFNIG

    # get the argument list
    # we will run from current node
    arg = LSF_COFNIG.get_list('bhosts_gpu_info')
    arg.append(LSF_COFNIG.__getitem__('bhosts_gpu_node_group_name'))
    return run_command(arg)


def run_lshosts_get_cpu_info(node_name_list):
    """
    run the lshosts command to get cpu information

    :return: the raw output for the lshost command,
    """
    # get the argument list
    arg = ['lshosts'] + node_name_list
    return run_command(arg)


def create_cpu_node_list(self):
    """
    :return: the node list without gpu, in default ngpus is 0
    """
    gpu_type = "NONE"
    namelist = self._config.get_list('lshosts_cpu_node_list')
    return [self.Node(name, gpu_type) for name in namelist]

def parse_lshosts_cpu_infor(output, nodelist):
    """
    update the input node list with cpu information
    :param output: the raw output from lshosts command
    """

    # this is the result for the json output
    json_result = []

    # loop over the output to get the data
    for line in output.splitlines():
        infor = line.split()

        # skip the head line
        if infor[0] == "HOST_NAME":
            continue

        # each line is for one host
        for node_name in nodelist:
            if node_name == infor[0]:

                # values
                ncpus = infor[4]
                mem = infor[5]

                # convert memory
                # memory should be only in unit of G or T
                #
                # the data should be a number with Unit G or T
                # so trim the last character
                val = 0
                if mem.find("G") > 0 or mem.find("g") > 0 or mem.find("T") > 0 or mem.find("t") > 0:
                    v0 = mem[:-1]
                    if is_str_float(v0):
                        # in case the value is in unit of tb
                        v1 = v0
                        if mem.find("T") > 0 or mem.find("t") > 0:
                            v1 = str(float(v0)*1024)
                            val = convert_float_to_integer(v1)
                        elif is_str_integer(v0):
                            val = convert_str_to_integer(v0)
                        else:
                            raise RuntimeError("Failed to convert the input memory value: {}".format(mem))
                else:
                    raise RuntimeError("The input memory value should be in unit of GB: {}".format(mem))

                # now let's update the result
                node_infor = {'name': node_name, 'ncores': int(ncpus), 'mem_in_gb': val}
                json_result.append(node_infor)

def parse_bhost_gpu_infor(output):
    """
    this function will parse the output from run_bhosts_get_gpu_info
    :param output: the raw output from the function run_bhosts_get_gpu_info
    :return: a list of nodes object
    """
    nodelist = []
    current_gpu_number = 1
    for line in output.splitlines():
        infor = line.split()

        # skip the head line
        if infor[0] == "HOST_NAME":
            continue

        # add in the node
        if len(infor) == 9:
            name = infor[0]
            gpu_type = self._get_gpu_type_for_node_from_lsf(infor[2])
            node = self.Node(name, gpu_type)

            # update the gpu number for the last node
            if len(nodelist) > 0:
                ngpus = current_gpu_number
                previous_node = nodelist[-1]
                previous_node.ngpus = ngpus

            # reset the gpu number for current node
            current_gpu_number = 1

            # add in the current node
            nodelist.append(node)

        # this line only has the gpu card information for the node
        elif len(infor) == 8:
            current_gpu_number = current_gpu_number + 1

    # add in the last ngpu for the node
    last_node = nodelist[-1]
    last_node.ngpus = current_gpu_number

    # return the list
    return nodelist

def _set_nodes_info(self):

        # firstly get the bhost output
        output = self._run_bhosts_get_gpu_info()

        # get the node list, with gpu information inside
        gpu_node_list = self._parse_bhost_gpu_infor(output)

        # now with the gpu node list let's get the full node list
        cpu_node_list = self._create_cpu_node_list()
        node_list = gpu_node_list + cpu_node_list

        # get the node names for the list and push the cpus and memory
        # information into the node list
        name_list = [x.name for x in node_list]
        output = self._run_lshosts_get_cpu_info(name_list)
        self._parse_lshosts_cpu_infor(output, node_list)

        # finally return the node list
        return node_list







    def _get_gpu_type_for_node_from_lsf(self, input):
        """
        check the gpu type from the input
        :param input: input string obtained from lsf command
        :return: the gpu type
        """
        for t in GPU_TYPE:
            t0 = t.split("_")[0]
            mem_label = t.split("_")[1]

            # if the type match
            if input.find(t0) > 0 and input.find(mem_label) > 0:
                return t

        # now if we are here, that means we did not find anything match
        info = "Invalid input GPU type for paring: {}".format(input)
        raise RuntimeError(info)



