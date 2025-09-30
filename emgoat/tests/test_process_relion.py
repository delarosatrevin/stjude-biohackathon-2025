
import os
import tempfile
from pprint import pprint
import time

from emtools.utils import Color
import emgoat
from emgoat.process import relion

COMMAND_TOKEN = 'XXXcommandXXX'


def _write_template_file(f, cmd):
    """ Util function to write a template file with a given command. """
    template_path = os.path.join(relion.__path__[0], 'template.py')

    with tempfile.NamedTemporaryFile() as tmpfile:
        print(f"\n>>> Writing to file: {Color.warn(tmpfile.name)}")
        with open(tmpfile.name, 'w') as fOut:
            with open(template_path) as fIn:
                for line in fIn:
                    if COMMAND_TOKEN in line:
                        line = line.replace(COMMAND_TOKEN, cmd)
                    fOut.write(line)

        emg = emgoat.EMGoat(tmpfile.name, debug=True)
        print(f"Program name: {Color.green(emg.cmd.program_name)}")
        print(f"Original command: {Color.bold(emg.cmd.original_command)}")
        for k, v in emg.cmd.command_dict.items():
            print(f"{k}: {v}")
        print(f"Job requirements: \n{str(emg.cmd.get_job_requirements())}")

def test_relion_run_motioncorr():
    _write_template_file(None,
                         f"`which relion_run_motioncorr` "
                         f"--i Import/job001/movies.star "
                         f"--o MotionCorr/job002/ "
                         f"--first_frame_sum 1 --last_frame_sum -1 "
                         f"--use_own  --j 1 --float16 --bin_factor 1 "
                         f"--bfactor 150 --dose_per_frame 1.277 --preexposure 0 "
                         f"--patch_x 5 --patch_y 5 --eer_grouping 32 "
                         f"--gainref Movies/gain.mrc --gain_rot 0 --gain_flip 0 "
                         f"--dose_weighting  --grouping_for_ps 3 "
                         f"--pipeline_control MotionCorr/job002/")

def test_relion_autopick():
    cmd_str = "`which relion_autopick_mpi` --i Select/job005/micrographs_split1.star --odir AutoPick/job006/ --pickname autopick --LoG  --LoG_diam_min 150 --LoG_diam_max 180 --shrink 0 --lowpass 20 --LoG_adjust_threshold 0 --LoG_upper_threshold 5  --pipeline_control AutoPick/job006/"

    _write_template_file(None, cmd_str)


def test_relion_refine():
    cmd_str = '`which relion_refine_mpi` --o Class3D/job016/run --i Select/job014/particles.star --ref InitialModel/job015/initial_model.mrc --ini_high 50 --dont_combine_weights_via_disc --preread_images  --pool 30 --pad 1  --ctf --iter 25 --tau2_fudge 4 --particle_diameter 200 --blush  --K 4 --flatten_solvent --zero_mask --oversampling 1 --healpix_order 2 --offset_range 5 --offset_step 2 --sym C1 --norm --scale  --j 6 --gpu "0:1:2:3"  --pipeline_control Class3D/job016/'

    _write_template_file(None, cmd_str)


