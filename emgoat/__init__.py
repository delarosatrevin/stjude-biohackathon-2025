
__version__ = "0.0.1"

from .config import config
from pprint import pprint


def main(template_file, debug=False):
    from .util import Loader

    template = Loader.load_from_file(template_file)
    print(dir(template))

    module_str = template.PROCESS['module']
    process_module = Loader.load_from_string(module_str)
    print(dir(process_module))

    cmd = process_module.Command(template.COMMAND)
    requirements = cmd.get_job_requirements()

    if debug:
        pprint(requirements)
