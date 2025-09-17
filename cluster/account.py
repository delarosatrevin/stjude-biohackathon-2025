"""
this file defines the account information for the jobs
"""

class Accounts:
    """
    define the account class associated with the jobs
    """

    def __init__(self, name):
        """
        initialization
        :param name: the account name
        """
        self.account_name = name
        self.nJobs = 0
        self.nGPUs = 0


