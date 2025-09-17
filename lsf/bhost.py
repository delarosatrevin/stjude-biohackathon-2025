"""
This file is used to collect/parse the nodes information from bhosts/lshosts commands
"""
from util.util import run_command,get_gpu_type_for_node_from_lsf,
from util.config import get_config
from cluster.nodes import Node
import re

##########################
# get the global conf
##########################
CONFIG = get_config()

def run_bhosts_get_gpu_info():
    """
    run the bhosts command to get node list and each node gpu information.
    command line arguments format is derived from the config

    :return: the raw output for the bhost command
    """
    global CONFIG

    # get the argument list
    # we will run from current node
    arg = CONFIG['lsf']['bhosts_gpu_info'].split()
    arg.append(CONFIG['lsf']['bhosts_gpu_node_group_name'])
    output = run_command(arg)
    return output

def run_lshosts_get_cpu_info(node_name_list):
    """
    run the lshosts command to get cpu information

    :return: the raw output for the lshost command,
    """
    global CONFIG
    # get the argument list
    arg = ['lshosts'] + node_name_list
    output = run_command(arg)
    return output

def create_cpu_node_list():
    """
    :return: the node list without gpu, in default ngpus is 0
    """
    gpu_type = "NONE"
    namelist = CONFIG['lsf']['lshosts_cpu_node_list'].split()
    list = []
    for name in namelist:
        node = Node(name,gpu_type)
        list.append(node)
    return list

def parse_lshosts_cpu_infor(output, nodelist):
    """
    update the input node list with cpu information
    :param output: the raw output from lshosts command
    """
    lines = output.splitlines()
    for line in lines:
        infor = line.split()

        # skip the head line
        if infor[0] == "HOST_NAME":
            continue

        # each line is for one host
        for node in nodelist:
            if node.name == infor[0]:
                ncpus = infor[4]
                mem = infor[5]

                # whether this is for GB/TB size of mem?
                if mem.find("G") > 0 or mem.find("g") > 0:
                    mem_value = re.sub(r"\D", "", mem)
                elif mem.find("T") > 0 or mem.find("t") > 0:
                    val = re.sub(r"\D", "", mem)
                    v1 = float(val)*1024
                    mem_value = str(v1)
                else:
                    raise RuntimeError("The input memory value should be in unit of GB: {}".format(mem))

                # now let's update
                node.update_cores_mem_info(ncpus,mem_value)

def parse_bhost_gpu_infor(output):
    """
    this function will parse the output from run_bhosts_get_gpu_info
    :param output: the raw output from the function run_bhosts_get_gpu_info
    :return: a list of nodes object
    """
    nodelist = []
    lines = output.splitlines()
    current_gpu_number = 1
    for line in lines:
        infor = line.split()

        # skip the head line
        if infor[0] == "HOST_NAME":
            continue

        # add in the node
        if len(infor) == 9:
            name = infor[0]
            gpu_type = get_gpu_type_for_node_from_lsf(infor[2])
            node = Node(name,gpu_type)

            # update the gpu number for the last node
            if len(nodelist) > 0:
                ngpus = current_gpu_number
                previous_node = nodelist[-1]
                previous_node.update_gpu_number(ngpus)

            # reset the gpu number for current node
            current_gpu_number = 1

            # add in the current node
            nodelist.append(node)

        # this line only has the gpu card information for the node
        elif len(infor) == 8:
            current_gpu_number = current_gpu_number + 1

    # add in the last ngpu for the node
    last_node = nodelist[-1]
    last_node.update_gpu_number(current_gpu_number)

    # return the list
    return nodelist

def get_compute_node_infor():
    """
    this is the driver function
    :return: the node list that contains all of information
    """

    # firstly get the bhost output
    output = run_bhosts_get_gpu_info()

    # get the node list, with gpu information inside
    gpu_node_list = parse_bhost_gpu_infor(output)

    # now with the gpu node list let's get the full node list
    cpu_node_list = create_cpu_node_list()
    node_list = gpu_node_list + cpu_node_list

    # get the node names for the list
    name_list = [x.name for x in node_list]
    output = run_lshosts_get_cpu_info(name_list)
    parse_lshosts_cpu_infor(output, node_list)

    # finally return the node list
    return node_list