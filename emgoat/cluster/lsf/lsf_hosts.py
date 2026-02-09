#
# this file records the host related LSF functions and calls, like bhost and lshost
# it will also analyze the output and save them in json format for future use
#
from emgoat.config import get_config
from emgoat.util import run_command, GPU_TYPE
from emgoat.util import is_str_float, is_str_integer
from emgoat.util import convert_float_to_integer
from emgoat.util import generate_json_data_file, read_json_data_file, need_newer_data_file
from .functions import *

#
# constants that from configuration
#
LSF_COFNIG = get_config()

def run_bqueues():
    """
    run the bqueues command to get host group information for the given queue

    :return: the raw output for the bqueues command
    """
    global LSF_COFNIG
    args = ['bqueues', '-l', LSF_COFNIG['lsf']['queue_name']]
    return run_command(args)

def run_bmgroup(hostgroup: str):
    """
    for the input host group, let's use bmgroup command to parse it recursively
    until to get all of node list
    """

    # run the command first
    # -r is recursively to expand the group until the host name solved
    # -w is for wide display
    args = ['bmgroup', '-r', '-w', hostgroup]
    output = run_command(args)

    # parse the output
    # should be only two lines
    for line in output.splitlines():
        if line.find("GROUP_NAME")>=0:
            continue

        # this is the error
        if line.find("No such user/host group")>0:
            raise RuntimeError("Run bmgroup command but we see error in the output, please double "
                               "check the grou name: {}".format(hostgroup))

        # this is the data line and we return
        return line

def parse_bqueues_output_to_get_host_list(output: str):
    """
    this function is to parse the output from function of run_bqueues
    so that to get host list information

    in this function we will also call bmgroup function on each output
    host group until we get all of host names

    this function returns a list that contains all of the node names
    corresponding to the given partition
    """

    # this is the result
    host_list = []

    # parse the output
    host_groups= []
    for line in output.splitlines():
        # this is the host group information
        if line.strip().startswith("HOSTS:"):
            host_groups_data = [x for x in line.strip().split() if x != "HOSTS:"]
            for data in host_groups_data:
                if data.find("/")>0:
                    host_groups.append(data.split("/")[0])
                else:
                    # here we treat it as a host name
                    host_list.append(data)


    # double check whether host groups is empty?
    if len(host_groups) == 0:
        raise RuntimeError("In the bqueues output we did not capture any host group data: {}".format(output))

    # now let's expand each host group into the host name
    for group in host_groups:
        data_line = run_bmgroup(group)
        # the first element is the group name
        nodes = data_line.split()[1:]
        for node in nodes:
            if node not in host_list:
                host_list.append(node)

    # finally let's return
    return host_list

def run_bhosts_get_gpu_info(node_list):
    """
    run the bhosts command to get node gpu information.
    command line arguments format is derived from the config

    here we pass the full node list, bhosts will ignore all of non-gpu nodes

    :return: the raw output for the bhost command
    """
    global LSF_COFNIG

    # get the argument list
    arg = LSF_COFNIG['lsf']['bhosts_gpu_info'].split()
    arg = arg + node_list
    return run_command(arg)

def run_bhosts_update_node_status(node_list, result):
    """
    run the bhosts command to get node status data

    we will parse the output of the command directly inside, and update the node status
    in the result directly
    """
    # run the command
    arg = ["bhosts"] + node_list
    output = run_command(arg)

    # now update the status information in the result
    for line in output.splitlines():
        if line.find("HOST_NAME")>=0:
            continue
        data = [x for x in line.strip().split()]
        name = data[0]
        status = data[1]
        for node_infor in result:
            node_name = node_infor['name']
            if node_name.lower() == name.lower():
                node_infor['status'] = status
                break

def run_lshosts_get_cpu_info(node_name_list):
    """
    run the lshosts command to get cpu information

    :return: the raw output for the lshost command,
    """
    # get the argument list
    arg = ['lshosts'] + node_name_list
    return run_command(arg)


def form_nodes_infor_list_from_node_names(node_names):
    """
    through the input node names (list of string) we will return
    a list of dict, and each dict object has the key of 'name'
    """
    result = []
    for name in node_names:
        d = {"name": name, "ncpus": -1, "mem_in_gb": -1, "ngpus": -1, "gpu_type": "none", "status": "none"}
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

def sceen_out_invalid_nodes(result):
    """
    this function check the data inside result, and remove all of the nodes with invalid information;
    like cpu/memory < 0
    """
    new_result = [ node_infor for node_infor in result if node_infor['ncpus'] > 0 and node_infor['mem_in_gb'] > 0 and node_infor['status'] != "none"]
    if len(new_result) != len(result):
        print("all of nodes information is below")
        for node in result:
            print("node_name:{0} ncpu:{1} memory_in_gb:{2} ngpus:{3} gpu_type:{4} status: {5}\n".format(
                node['name'], node['ncpus'], node['mem_in_gb'], node['ngpus'], node['gpu_type'], node['status']
            ))
        print("Warning: There are nodes with invalid node information, like cpu/memory < 0, or the node status is none")
        return new_result
    else:
        return result


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

    # get the data
    file_name = LSF_COFNIG['lsf']['node_data_file_name']
    path_name = LSF_COFNIG['lsf']['data_output_dir']
    time = int(LSF_COFNIG['lsf']['nodes_data_update_time'])
    fname = path_name + "/" + file_name

    # firstly let's see whether we have the data file and
    # we can read the data from the file
    if not need_newer_data_file(fname, time):
        data = read_json_data_file(fname)
        return data

    # get the full node list
    output = run_bqueues()
    node_list = parse_bqueues_output_to_get_host_list(output)

    # initialize the result
    result = form_nodes_infor_list_from_node_names(node_list)

    # fill in the gpu information
    output = run_bhosts_get_gpu_info(node_list)
    parse_bhost_gpu_infor(output, result)

    # generate cpu information for all nodes
    output = run_lshosts_get_cpu_info(node_list)
    parse_lshosts_cpu_infor(output, result)

    # get the node status for the queue
    run_bhosts_update_node_status(node_list, result)

    # finally screen out all of the node information with invalid data
    result = sceen_out_invalid_nodes(result)

    # save it to file
    generate_json_data_file(result, fname)

    # finally return result
    return result











