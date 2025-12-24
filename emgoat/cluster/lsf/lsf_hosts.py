#
# this file records the host related LSF functions and calls, like bhost and lshost
# it will also analyze the output and save them in json format for future use
#
import os
import json
from emgoat.config import get_config
from emgoat.util import run_command, GPU_TYPE
from emgoat.util import is_str_float, is_str_integer
from emgoat.util import convert_float_to_integer
from .functions import *
from datetime import datetime, timedelta

#
# constants that from configuration
#
LSF_COFNIG = get_config()

def run_bhosts_get_gpu_info():
    """
    run the bhosts command to get node list and each node gpu information.
    command line arguments format is derived from the config

    :return: the raw output for the bhost command
    """
    global LSF_COFNIG

    # get the argument list
    # we will run from current node
    arg = LSF_COFNIG['lsf']['bhosts_gpu_info'].split()
    arg.append(LSF_COFNIG['lsf']['bhosts_gpu_node_group_name'])
    return run_command(arg)


def run_lshosts_get_cpu_info(node_name_list):
    """
    run the lshosts command to get cpu information

    :return: the raw output for the lshost command,
    """
    # get the argument list
    arg = ['lshosts'] + node_name_list
    return run_command(arg)


def get_nodes_name_from_bhost_output(output):
    """
    This function is used to return the nodes names from the bhost commands

    the data from the output will be like below:

    HOST_NAME                GPU_ID       MODEL     MUSED      MRSV  NJOBS    RUN   SUSP    RSV
    nodexxx10                 0  TeslaV100_SXM2_16GB      555M        0M      1      1      0      0
                              1  TeslaV100_SXM2_16GB      1.1G        0M      1      1      0      0
                              2  TeslaV100_SXM2_16GB      753M        0M      1      1      0      0
                              3  TeslaV100_SXM2_16GB      557M        0M      1      1      0      0
    nodexxx11                 0  TeslaV100_SXM2_16GB      239M        0M      1      1      0      0
                              1  TeslaV100_SXM2_16GB      239M        0M      1      1      0      0
                              2  TeslaV100_SXM2_16GB      1.4G        0M      1      1      0      0
                              3  TeslaV100_SXM2_16GB      557M        0M      1      1      0      0
    nodexxx3                  0  TeslaV100_SXM2_32GB      1.4G        0M      1      1      0      0
                              1  TeslaV100_SXM2_32GB      591M        0M      1      1      0      0
                              2  TeslaV100_SXM2_32GB      589M        0M      1      1      0      0
                              3  TeslaV100_SXM2_32GB      1.4G        0M      1      1      0      0
    """
    # set up result
    nodes_name = []

    # loop over lines
    for line in output.splitlines():
        infor = line.split()

        # skip the head line
        if infor[0] == "HOST_NAME":
            continue

        # this is the line we have new data
        if len(infor) == 9:
            new_name = infor[0]
            nodes_name.append(new_name)

    # return
    return nodes_name


def form_nodes_infor_list_from_node_names(node_names):
    """
    through the input node names (list of string) we will return
    a list of dict, and each dict object has the key of 'name'
    """
    result = []
    for name in node_names:
        d = {"name": name, "ncpus": -1, "mem_in_gb": -1, "ngpus": -1, "gpu_type": "none"}
        result.append(d)

    # return
    return result


def parse_lshosts_cpu_infor(output, result):
    """
    update the input node list with cpu information
    :param output: the raw output from lshosts command
    :return result: the result that collects the lshost information

    the result should have the node name information

    the output example is:

    lshosts nodexxx nodeyyy
    HOST_NAME      type    model   cpuf  ncpus maxmem maxswp server RESOURCES
    nodexxx      X86_64  Opteron8  60.0    64 1003.3G  15.9G    Yes (rhel8 hp hp4 mem_1t xl225n epyc rome amd)
    nodeyyy      X86_64  Opteron8  60.0    64 1003.3G  15.9G    Yes (rhel8 hp hp4 mem_1t xl225n epyc rome amd)

    sometimes the cpu and memory is none (shown as "-") in the output, then in this case we will get rid of this node

    the result is like below:
    [node1_infor, node2_infor, ....]

    each node_infor is a dict:
    node_infor['name']: node name
    node_infor['ncpus']: node cpu number for use
    node_infor['mem_in_gb']: the total memory for the node in GB size

    In this function we will update the node infor in terms of the ncpus and mem_in_gb for each node in the result
    """

    # loop over the output to get the data
    for line in output.splitlines():
        infor = line.split()

        # skip the head line
        if infor[0] == "HOST_NAME":
            continue

        # each line is for one host
        for node_infor in result:
            node_name = node_infor['name']
            if node_name.lower() == infor[0].lower():

                # values
                ncpus = infor[4]
                mem = infor[5]

                # ncpus should be a number, if not we will skip this result
                if not is_str_integer(ncpus):
                    continue

                # convert memory
                # memory should be only in unit of G or T
                #
                # the data should be a number with Unit G or T
                # so trim the last character
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
                    raise RuntimeError("The input memory value should be in unit of GB/TB: {}".format(mem))

                # now let's update the result
                node_infor['ncpus'] = int(ncpus)
                node_infor['mem_in_gb'] = val


def get_gpu_type_for_node_from_lsf(input):
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


def parse_bhost_gpu_infor(output, result):
    """
    this function will parse the output from run_bhosts_get_gpu_info
    :param output: the raw output from the function run_bhosts_get_gpu_info
    :return: result list (this result is same with parse_lshosts_cpu_infor

    the data will be updated in the result, too

    the data from the output will be like below:

    HOST_NAME                GPU_ID       MODEL     MUSED      MRSV  NJOBS    RUN   SUSP    RSV
    nodexxx10                 0  TeslaV100_SXM2_16GB      555M        0M      1      1      0      0
                              1  TeslaV100_SXM2_16GB      1.1G        0M      1      1      0      0
                              2  TeslaV100_SXM2_16GB      753M        0M      1      1      0      0
                              3  TeslaV100_SXM2_16GB      557M        0M      1      1      0      0
    nodexxx11                 0  TeslaV100_SXM2_16GB      239M        0M      1      1      0      0
                              1  TeslaV100_SXM2_16GB      239M        0M      1      1      0      0
                              2  TeslaV100_SXM2_16GB      1.4G        0M      1      1      0      0
                              3  TeslaV100_SXM2_16GB      557M        0M      1      1      0      0
    nodexxx3                  0  TeslaV100_SXM2_32GB      1.4G        0M      1      1      0      0
                              1  TeslaV100_SXM2_32GB      591M        0M      1      1      0      0
                              2  TeslaV100_SXM2_32GB      589M        0M      1      1      0      0
                              3  TeslaV100_SXM2_32GB      1.4G        0M      1      1      0      0
    """

    # the data here is recording the current node gpu information
    current_gpu_number = 0
    name = ""
    gpu_type = ""

    # loop over lines
    for line in output.splitlines():
        infor = line.split()

        # skip the head line
        if infor[0] == "HOST_NAME":
            continue

        # this is the line we have new data
        if len(infor) == 9:

            # get data
            new_name = infor[0]
            new_gpu_type = get_gpu_type_for_node_from_lsf(infor[2])

            # update the old data first
            # if this is the first line of input, the current gpu number is 0
            if current_gpu_number > 0:

                # result gpu number for last node
                ngpus = current_gpu_number

                # the name should not be empty
                if not name.strip():
                    raise RuntimeError("The name is not initialized, "
                                       "we failed to analyze the data in parse_bhost_gpu_infor")

                # so as the gpu type, it should has some data
                if not gpu_type.strip():
                    raise RuntimeError(
                        "The gpu type data is not initialized, we failed to analyze the data in parse_bhost_gpu_infor")

                # update the result
                got_data = 0
                for node_infor in result:
                    node_name = node_infor['name']
                    if node_name.lower() == name.lower():
                        node_infor['ngpus'] = ngpus
                        node_infor['gpu_type'] = gpu_type
                        got_data = 1
                        break

                # double check, we should get data updated
                if got_data == 0:
                    raise RuntimeError("Failed to update the node information "
                                       "in terms of the gpu in parse_bhost_gpu_infor")


            # update the gpu type and name and gpu number
            name = new_name
            gpu_type = new_gpu_type
            current_gpu_number = 1

        # this line only has the gpu card information for the node
        elif len(infor) == 8:
            current_gpu_number = current_gpu_number + 1

    # after reading the whole data, we need to update the last node information
    got_data = 0
    for node_infor in result:
        node_name = node_infor['name']
        if node_name.lower() == name.lower():
            node_infor['ngpus'] = current_gpu_number
            node_infor['gpu_type'] = gpu_type
            got_data = 1
            break

    # double check, we should get the last data updated
    if got_data == 0:
        raise RuntimeError("Failed to update the last node information "
                           "in terms of the gpu in parse_bhost_gpu_infor")


def need_new_nodes_infor_text_file():
    """
    This function compares the nodes infor data file time stamp, if it's older than the
    time limit let's return true; that means a new file needed to be generated. If the current
    data file is good then we will return false
    """
    global LSF_COFNIG

    # get the data file name
    file_name = LSF_COFNIG['lsf']['node_data_file_name']
    path_name = LSF_COFNIG['lsf']['data_output_dir']
    time = int(LSF_COFNIG['lsf']['nodes_data_update_time'])
    fname = path_name + "/" + file_name

    # firstly check whether the file exists?
    if not os.path.exists(fname):
        return True

    # now the file exists
    timestamp = os.path.getmtime(fname)
    dt1 = datetime.fromtimestamp(timestamp)
    dt2 = datetime.now()
    if abs(dt2 - dt1) < timedelta(minutes=time):
        return False
    else:
        return True


def generate_json_nodes_info(result):
    """
    This function will generate the json format of results
    :param result: the input result data in terms of node we captured from above function
    """
    global LSF_COFNIG

    # get the data file name
    file_name = LSF_COFNIG['lsf']['node_data_file_name']
    path_name = LSF_COFNIG['lsf']['data_output_dir']
    fname = path_name + "/" + file_name

    # now write the data into the file
    with open(fname, 'w') as node_infor:
        json.dump(result, node_infor, indent=4)


def read_json_nodes_info():
    """
    This function will read in the json format of results and return it
    """
    global LSF_COFNIG

    # get the data file name
    file_name = LSF_COFNIG['lsf']['node_data_file_name']
    path_name = LSF_COFNIG['lsf']['data_output_dir']
    fname = path_name + "/" + file_name

    # now load in the data
    with open(fname, "r") as f:
        data = json.load(f)

    # now return the data
    return data


def get_nodes_info():
    """
    This function is the driver function for this module

    If the node information is outdated, or we do not have the node information; this
    driver function will create the data and output a json data result; also it
    will return the fresh result

    Otherwise if it can find the new data, it will load the json format data and return
    it
    """
    global LSF_COFNIG

    # firstly let's see whether we have the data file and
    # we can read the data from the file
    if not need_new_nodes_infor_text_file():
        data = read_json_nodes_info()
        return data

    # run the bhost command first
    output = run_bhosts_get_gpu_info()
    gpu_node_list = get_nodes_name_from_bhost_output(output)
    cpu_node_list = LSF_COFNIG['lsf']['lshosts_cpu_node_list'].split()
    node_list = gpu_node_list + cpu_node_list
    result = form_nodes_infor_list_from_node_names(node_list)

    # update gpu information in result
    parse_bhost_gpu_infor(output, result)

    # generate cpu information for all nodes
    output = run_lshosts_get_cpu_info(node_list)
    parse_lshosts_cpu_infor(output, result)

    # save it to file
    generate_json_nodes_info(result)

    # finally return result
    return result











