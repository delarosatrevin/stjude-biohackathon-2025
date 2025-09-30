
import pytest
from emgoat.cluster import LSFCluster


def test_jobs_info():
    for job in LSFCluster().get_jobs_info():
        print(job)
        print("\n\n")


def test_nodes_info():
    for node in LSFCluster().get_nodes_info():
        print(node)
        print("\n\n")

def test_accounts_info():
    for account in LSFCluster().get_accounts_info():
        print(account)
        print("\n\n")

def test_future_snapshots():
    lsf = LSFCluster()
    time_interval = lsf.get_time_interval_for_snapshots()
    data_list = lsf.get_data_for_snapshots()
    for n in range(len(time_interval)):
        begin = time_interval[n]
        data = data_list[n]
        print("######################################")
        print("begin time: {}".format(begin))
        print("######################################")
        for node in data:
            print(node)
            print("\n")

