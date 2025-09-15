
class Jobs:

    def __init__(self, jobid, submit_time, state, general_state, pending_time,
                 job_used_time, job_remaining_time, start_time, cpu_used,
                 gpu_used, memory_used, gpu_type, compute_nodes, user, account_name):
        """
        the job ID is an integer; from LSF/slurm etc. However for easy handling we just treat it as a string

        the state is whether it's pending/running/finished/error/cancelled (five states allowed), the state
        symbol is taken from the scheduler (lsf/slurm)

        the general state is our internal defined state symbol (see util.py, the JOB_STATUS_PD etc.)

        pending time is how long the job is pending (in unit of minutes)

        job used time: how much time the job is used (wall time), in unit of minutes

        start time: the job starting time (dateTime)

        memory used: how much memory used in unit of GB

        cpu/gpu used: how many cores/gpus used for the job

        submit time: when the job is submitted, this is a primary key and we use it to sort jobs

        compute nodes are the nodes that the job are submitted onto.
        """
        
        self.jobid              =  jobid              
        self.submit_time        =  submit_time       
        self.state              =  state             
        self.general_state      =  general_state      
        self.pending_time       =  pending_time      
        self.job_used_time      =  job_used_time     
        self.job_remaining_time =  job_remaining_time
        self.start_time         =  start_time        
        self.cpu_used           =  cpu_used          
        self.gpu_used           =  gpu_used          
        self.memory_used        =  memory_used        
        self.gpu_type           =  gpu_type            
        self.compute_nodes      =  compute_nodes     
        self.user               =  user               
        self.account_name       =  account_name

    def is_a_cryo_job(self):
        """
        whether this is a cryosparc job?

        We test the csid and project id, for non-cryosparc job either of them
        are just None.
        :return: true if it's a cryosparc job
        """
        if not self.csid or not self.projectid:
            return False
        else:
            return True

