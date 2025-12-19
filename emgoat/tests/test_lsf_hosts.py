
#
# this is to test the lsf_hosts.py
#
import pytest
import emgoat
from emgoat.util import Config, run_command, GPU_TYPE
from emgoat.cluster.lsf.lsf_hosts import *


def test_parsing_bhost_output():
    """
    test functions related to bhost command running and testing
    """
    LSF_COFNIG = Config(emgoat.config['lsf'])
    output = run_bhosts_get_gpu_info()
    print("raw output for the bhost command")
    print(output)
    gpu_node_list = get_nodes_name_from_bhost_output(output)
    for node in gpu_node_list:
        print("gpu node name: {}".format(node))
    cpu_node_list = LSF_COFNIG.get_list('lshosts_cpu_node_list')
    node_list = gpu_node_list + cpu_node_list
    result = form_nodes_infor_list_from_node_names(node_list)
    print("after initialization of the result")
    for r in result:
        print(r)
    parse_bhost_gpu_infor(output, result)
    print("after parsing the bhost output")
    for r in result:
        print(r)
        


