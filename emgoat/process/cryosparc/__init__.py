
import shlex

from emtools.jobs import Args
from emgoat.cluster import Cluster
from emgoat.util import get_dict_from_args

SUFFIX_JOB = "cryosparc_"


class Command:
    def __init__(self, module):
        self.original_command = module.COMMAND.strip()

        self.program_name = ""  # The job_type that is going to be added later
        parts = shlex.split(self.original_command)
        self.args = Args.fromList(parts[2:])
        self.ngpus = int(module.GPUS)
        self.mem_gb = int(module.MEM[:-1])
        self.cli = self.connect_cli_cryosparc(self.args['--master_hostname'],
                                              self.args["--master_command_core_port"][0],
                                              module.LICENSE_ID)

    def get_job_requirements(self, cluster=None):
        """ Depending on the job name and inputs,
        determine the job requirements for execution. """

        self.job_info = self.extract_job_information()
        self.program_name = SUFFIX_JOB + self.job_info["job_type"]
        # base_program_name = self.program_name.replace(SUFFIX_JOB, '')
        rule_func_name = f"_rule_{self.program_name}"
        requirements = None
        rule_func = getattr(self, rule_func_name, None)

        if rule_func is not None:
            requirements = rule_func()

        if requirements is None:
            raise Exception(f"No rule found for {self.program_name} job requirements.")

        return requirements

    def connect_cli_cryosparc(self, host, port, license_id):
        """ Try to connect to cryoSPARC CommandClient. """
        try:
            from cryosparc.tools import CommandClient
            cli = CommandClient(host=host, port=port, headers={"License-ID": license_id})
            return cli
        except Exception as e:
            print(f"[ERROR] Could not connect to cryoSPARC at {host}:{port} - {e}")
            return None

    def extract_job_information(self):
        job_info = self.cli.get_job(self.args["--project"], self.args["--job"])
        job_type = job_info['job_type']
        job_params = job_info['params_base']
        job_resources = job_info['resources_needed']
        # First layer User demands
        job_summ_info = {
            "job_type": job_type,
            "job_params": job_params,
            "job_resources": job_resources
        }
        # Second layer Command input
        job_input_info = self.extract_job_input_information()
        job_summ_info.update(job_input_info)

        # Todo: Third layer Process info

        return job_summ_info

    def extract_job_input_information(self):
        job_info = self.cli.get_job(self.args["--project"], self.args["--job"])
        input_slots = job_info['input_slot_groups']
        import_id = get_imported_particles_uid(input_slots)
        job_info_input = self.cli.get_job(self.args["--project"], import_id)
        input_info = get_blob_shape_and_num_items(job_info_input["output_result_groups"])

        return input_info

    def _requirements(self, **kwargs):
        jobr = Cluster.JobRequirements(**kwargs)
        if jobr.ncpus <= 0:
            jobr.ncpus = jobr.ngpus * 5
        jobr.total_memory = self.mem_gb  # FIXME: now hard-coded 8Gb per core
        return jobr

    def _rule_cryosparc_import_particles(self):
        return self._requirements(ncpus=1)

    def _rule_cryosparc_import_volumes(self):
        return self._requirements(ncpus=1)

    def _rule_cryosparc_class_2D_new(self):
        # num_classes = job_params['class2D_K']['value'] # Specific for 2D
        return self._requirements(ngpus=self.ngpus,
                                       commands=[self.original_command])

    def _rule_nonuniform_refine_new(self):
        return self._requirements(ngpus=4)

    def _rule_homo_abinit(self):
        return self._requirements(ngpus=4)


def get_imported_particles_uid(list_dicts):
    for d in list_dicts:
        connections = d.get("connections", [])
        for conn in connections:
            if conn.get("group_name") == "imported_particles":
                return conn.get("job_uid")
    return None  # if not found

def get_blob_shape_and_num_items(list_dicts):
    for d in list_dicts:
        summary = d.get("summary", {})
        if "blob/shape" in summary and "num_items" in d:
            return {
                "blob/shape": summary["blob/shape"],
                "num_items": d["num_items"]
            }
    return None  # If not found