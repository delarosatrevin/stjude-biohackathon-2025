
import pytest
import os
from emgoat.cluster import LSFCluster
from emgoat.cluster import Cluster


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

def test_lsf_script_generation():

    # set the output for testing
    output = "/tmp/emgoat_testing.lsf"
    if os.path.exists(output):
        os.remove(output)

    # generate the job reqirment
    requirement = Cluster.JobRequirements(ncpus=10,ngpus=4,total_memory=128)

    # output the lsf cluster
    lsf = LSFCluster()
    lsf.generate_job_script(requirement, output)


