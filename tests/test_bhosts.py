import pytest
from lsf.bhost import get_compute_node_infor

def test_node_infor():
    """
    test the function of parse_bjobs_output_for_alljobs
    :return:
    """
    node_list = get_compute_node_infor()
    for node in node_list:
        print(node)

