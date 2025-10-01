
import pytest
import os
import tempfile

from emgoat.cluster.lsf import Cluster as LSFCluster
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
    # generate the job reqirment
    requirement = Cluster.JobRequirements(ncpus=10, ngpus=4, total_memory=100,
                                          commands=['module load relion/v5.0',
                                                    'relion_refine testing'])
    # output the lsf cluster
    lsf = LSFCluster()
    # set the output for testing
    with tempfile.NamedTemporaryFile() as tmpfile:
        os.remove(tmpfile.name)
        lsf.generate_job_script(requirement, tmpfile.name)
        jobID = lsf.launch_job(tmpfile.name)
        print ("the job ID is {}".format(jobID))

        with open(tmpfile.name) as f:
            for line in f:
                print(line.rstrip())

def test_lsf_overview():
    lsf = LSFCluster()
    overview = lsf.get_cluster_overview()
    for key, value in overview.items():
        print("for gpu card: {0} there are {1} available slots and the "
              "available percentage is {2:.3f}\n".format(key, value[0], value[1]))


def test_lsf_job_availability():
    lsf = LSFCluster()
    requirement = Cluster.JobRequirements(ncpus=10, ngpus=4, total_memory=100)
    result = lsf.get_job_availability_check(requirement)
    for key, value in result.items():
        print("Checking the case: {0} there are {1} available "
              "slots\n".format(key, value))


def test_lsf_future_snapshots_availability():
    lsf = LSFCluster()
    requirement = Cluster.JobRequirements(ncpus=10, ngpus=4, total_memory=100)
    result = lsf.get_job_estimation_landing(requirement)
    for key, value in result.items():
        print("Checking the time: {0} that there are number of slots {1} available "
              "slots\n".format(key, value))

