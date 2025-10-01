
import shlex

from emtools.jobs import Args

from emgoat.cluster import Cluster


class Command:
    def __init__(self, template_module):
        self.template_module = template_module
        self.original_command = template_module.COMMAND.strip()
        parts = shlex.split(self.original_command)
        self.program_name = parts[1].replace('`', '')
        self.args = Args.fromList(parts[2:])

    def get_job_requirements(self, cluster=None):
        """ Depending on the job name and inputs,
        determine the job requirements for execution. """
        base_program_name = self.program_name.replace('_mpi', '')
        rule_func_name = f"_rule_{base_program_name}"
        requirements = None
        rule_func = getattr(self, rule_func_name, None)

        if rule_func is not None:
            requirements = rule_func()

        if requirements is None:
            raise Exception(f"No rule found for {self.program_name} job requirements.")

        return requirements

    def _requirements(self, **kwargs):
        jobr = Cluster.JobRequirements(**kwargs)
        if jobr.ncpus <= 0:
            jobr.ncpus = jobr.ngpus * 10
        if not jobr.total_memory:
            jobr.total_memory = jobr.ncpus * 8  # FIXME: now hard-coded 8Gb per core

        return jobr

    def _rule_relion_run_motioncorr(self):
        if '--use_own' in self.args:
            jobr = self._requirements(ncpus=64)
            mpis = 8
            self.args['--j'] = 8
            args = self.args.toLine()
            jobr.commands = [
                "module load relion/v5.0",
                f"mpirun -n {mpis} `which {self.program_name}` {args}"
            ]
        else:
            jobr = self._requirements(ngpus=4)
        return jobr

    def _rule_relion_refine(self):
        return self._requirements(ngpus=4)

    def _rule_relion_autopick(self):
        return self._requirements(ncpus=16)

