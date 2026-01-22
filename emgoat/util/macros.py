from typing import Final

##########################
# define job status
# suspending is also count
# into pending state
##########################
# JOB_STATUS_PD: Final[str] = "pending/suspending"
# JOB_STATUS_RUN: Final[str] = "running"
# JOB_STATUS_DONE: Final[str] = "done"
#
# ############################
# # available GPU type
# ############################
# GPU_TYPE: Final[list[str]] = ["V100_16G", "V100_32G", "A100_80G"]
#
# ############################
# # set up a very big number
# # representing the infinite
# # large number
# #
# # it's used when we can not
# # capture the time, so in
# # default the time will be
# # this value (seconds or minutes)
# ############################
# VERY_BIG_NUMBER: Final[int] = 1000000000


JOB_STATUS_PD = "pending/suspending"
JOB_STATUS_RUN = "running"
JOB_STATUS_DONE = "done"

############################
# available GPU type
############################
GPU_TYPE = ["V100_16G", "V100_32G", "A100_80G"]

############################
# set up a very big number
# representing the infinite
# large number
#
# it's used when we can not
# capture the time, so in
# default the time will be
# this value (seconds or minutes)
############################
VERY_BIG_NUMBER = 1000000000

#
# not available symbol
#
NOT_AVAILABLE = "N/A"
