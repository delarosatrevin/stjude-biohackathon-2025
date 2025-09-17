#
# this simple class describe the node information
#

class Node:

    def __init__(self, name, gpu_type):
        """
        as the initial setup, we capture the name and gpu information from the bhost output
        :param gpu_type: gpu type
        """
        self.name = name
        self.gpu_type = gpu_type
        self.ngpus = 0
        self.ncpus = 0
        self.total_mem_in_gb = 0

    def update_gpu_number(self, ngpus):
        """
        update the number of gpus
        :param ngpus: number of gpus for that node
        """
        self.ngpus = ngpus

    def update_cores_mem_info(self, ncpus, total_mem):
        """
        add in the information for ncpus and total memory size in GB

        :param ncpus: number of cpu cores
        :param total_mem: the total memory size
        """
        self.ncpus = ncpus
        self.total_mem_in_gb = total_mem

    def __str__(self):
        """
        this is for printing
        """
        return (f"Node name={self.name}, gpu type={self.gpu_type}, ngpus={self.ngpus}, ncpus={self.ncpus}, "
                f"mem={self.total_mem_in_gb} ")
