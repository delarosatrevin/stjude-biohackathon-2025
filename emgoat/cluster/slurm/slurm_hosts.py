from emgoat.config import get_config
from emgoat.util import run_command
from slurm_util import get_gpu_number_from_sinfo_output,get_gpu_type_from_sinfo_output
from emgoat.util import generate_json_data_file, read_json_data_file, need_newer_data_file
#
# constants that from configuration
#
slurm_COFNIG = get_config()

def get_raw_sinfo_data():
    """
    get the output of sinfo data for all of available nodes
    :returns: raw output from sinfo command
    """
    global slurm_COFNIG

    # let's get the output data in form of list of string
    # "-N" makes the result listed every node one line
    format_opts = '--Format=' + '"' + slurm_COFNIG['slurm']['sinfo_format'] + '"'
    args = ['sinfo', '-N', format_opts]
    infor = run_command(args)

    # whether we have any data?
    # let's capture the headline, so that we know the position for the data
    get_headline = False
    label = "NODELIST"
    for line in infor.splitlines():
        if line.find(label) >= 0:
            get_headline = True
            break

    # if data line is empty, we just return the empty result
    if not get_headline:
        raise RuntimeError("We failed to capture the data of sinfo output data. "
                           "The output sinfo command is: {}".format(infor))

    # now everything good, let's return
    return infor

def parse_sinfo_data(infor: str):
    """
    in this function let's parse the sinfo output and return the machine status data for 
    current time.

    :param infor: raw output of sinfo command
    :returns: a dict form of result, recording the nodes/cores etc. data
    """

    # set up the node list, this is the result
    node_list = []

    # let's capture the headline, so that we know the position for the data
    # set the data position
    nodename_pos  = -1
    status_pos    = -1
    ncores_pos    = -1
    used_cores_pos= -1
    avail_gpu_pos = -1
    used_gpu_pos  = -1
    mem_pos       = -1
    used_mem_pos  = -1
    for line in infor.splitlines():

        # this is the headline
        if line.find("NODELIST") >= 0:

            # let's analyze this line
            data_fields = [f.strip() for f in line.split()]
            for p in range(len(data_fields)):
                val = data_fields[p]
                if val == "STATE":
                    status_pos = p
                elif val == "CPUS":
                    ncores_pos = p
                elif val == "CPUS(A/I/O/T)":
                    used_cores_pos= p
                elif val == "GRES":
                    avail_gpu_pos = p
                elif val == "GRES_USED":
                    used_gpu_pos  = p
                elif val == "NODELIST":
                    nodename_pos  = p
                elif val == "MEMORY":
                    mem_pos = p
                elif val == "ALLOCMEM":
                    used_mem_pos = p

            # once it's done break
            break

    # make sure we capture the data
    # if not let's return a copy of result, which contains the np.nan
    if (status_pos < 0 or ncores_pos < 0 or avail_gpu_pos < 0 or used_gpu_pos < 0 or
            used_cores_pos < 0 or nodename_pos < 0 or mem_pos < 0 or used_mem_pos < 0):
        raise RuntimeError("We failed to capture the STATE/CPUS/GPU etc. data fields in the sinfo dataline, "
                           "The output of sinfo command: {}".format(infor))
    
    # now everything good, let's begin to process each data line
    for line in infor.splitlines():

        # all of stuff begins after the headline
        if line.find("NODELIST") >= 0:
            continue

        # now read in each line
        data_fields = [f.strip() for f in line.strip().split()]

        # get the status and ncores
        status = data_fields[status_pos].lower()

        # node name
        node_name = data_fields[nodename_pos]

        # total memory in mb
        total_mem = int(int(data_fields[mem_pos])/1024)

        # used memory in mb
        used_mem = int(int(data_fields[used_mem_pos])/1024)
            
        # the total number of cores
        # if we can not process it, just set it to 0 so we do not add anything
        ncores = int(data_fields[ncores_pos])

        # get the number of used cores
        # the result is in form of (A/I/O/T)
        # the first one is the used cores
        used_core_num = int(data_fields[used_cores_pos].split("/")[0])

        # whether the node has gpu?
        gpu_data = get_gpu_number_from_sinfo_output(data_fields[avail_gpu_pos])

        # how many gpu used
        used_gpu_data = get_gpu_number_from_sinfo_output((data_fields[used_gpu_pos]))

        # gpu type
        gpu_type = get_gpu_type_from_sinfo_output(data_fields[avail_gpu_pos])

        # now let's form the data
        if gpu_data == 0:
            node_infor = {"name": node_name, "ncpus": ncores, "n_used_cpus": used_core_num,
                          "mem_in_gb": total_mem, "used_mem_in_gb": used_mem, "status": status,
                          "ngpus": -1, "gpu_type": "none", "n_used_gpus": -1}
        else:
            node_infor = {"name": node_name, "ncpus": ncores, "n_used_cpus": used_core_num,
                          "mem_in_gb": total_mem, "used_mem_in_gb": used_mem, "status": status,
                          "ngpus": gpu_data, "gpu_type": gpu_type, "n_used_gpus": used_gpu_data}

        # add in the data
        node_list.append(node_infor)
    
    # let's return
    return node_list

def get_nodes_info():
    """
    This function is the driver function for this module

    If the node information is outdated, or we do not have the node information; this
    driver function will create the data and output a json data result; also it
    will return the fresh result

    Otherwise if it can find the new data, it will load the json format data and return
    it
    """
    global slurmF_COFNIG

    # get the data
    file_name = slurm_COFNIG['slurm']['node_data_file_name']
    path_name = slurm_COFNIG['slurm']['data_output_dir']
    time = int(slurm_COFNIG['slurm']['nodes_data_update_time'])
    fname = path_name + "/" + file_name

    # firstly let's see whether we have the data file and
    # we can read the data from the file
    if not need_newer_data_file(fname, time):
        data = read_json_data_file(fname)
        return data

    # run the sinfo command then parse the output
    output = get_raw_sinfo_data()
    node_list_data = parse_sinfo_data(output)

    # save it to file
    generate_json_data_file(node_list_data, fname)

    # finally return result
    return node_list_data

