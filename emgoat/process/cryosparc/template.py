
PROCESS = {
    "module": "emgoat.process.cryosparc"
}

CLUSTER = {
    "module": "emgoat.cluster.lsf",
    "config": {}
}

MEM = "{{ ram_gb|int }}G"

GPUS = "{{ num_gpu }}"

LICENSE_ID = "9a8c7288-00b0-11ef-864f-ab66d8f93244"

COMMAND = "{{ run_cmd }}"
