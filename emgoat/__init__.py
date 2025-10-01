
__version__ = "0.0.1"

import os
from .config import config
from pprint import pprint


class EMGoat:

    def __init__(self, template_file, debug=False):
        from .util import Loader
        self.template_file = template_file
        self.debug = debug
        # Load the template file as a Python module with information
        # where to find the Process module and the Cluster module
        self.template_module = Loader.load_from_file(template_file)

        # Load the Process module
        module_str = self.template_module.PROCESS['module']
        self.process_module = Loader.load_from_string(module_str)

        # Load the Cluster module
        module_str = self.template_module.CLUSTER['module']
        self.cluster_module = Loader.load_from_string(module_str)

        # Build the command
        self.cmd = self.process_module.Command(self.template_module)

    def launch_job(self):
        from emtools.utils import Path, Color

        cluster = self.cluster_module.Cluster()
        script_file = Path.replaceExt(self.template_file,
                                      f'_{cluster._name}_job.sh')
        jobr = self.cmd.get_job_requirements()
        print(">>> Job requirements: ")
        print(jobr)

        print("Writing job file: ", script_file)
        cluster.generate_job_script(jobr, script_file)
        cmd = f"bsub < {script_file}"
        print(Color.green(cmd))
        os.system(cmd)


