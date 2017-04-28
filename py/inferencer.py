from functools import partial

import subprocess

from py.configurator import Inference
import os


class Inferencer(object):

    def __init__(self, config: Inference, announcer):
        self._cfg = config
        self.announcer = partial(announcer, process="Inferencer")

    def main(self):
        cmd = "/app/Mallet/bin/mallet " \
              "infer-topics " \
              "--inferencer /app/data/spaces/{space_name}/inferencer.gz " \
              "--input {temp_dir}/{target_in} " \
              "--output-doc-topics {temp_dir}/{target_out} " \
              "--num-iterations 100 ".format(space_name=self._cfg.space_name,
                                             target_in=self._cfg.in_file,
                                             target_out=self._cfg.out_file,
                                             temp_dir=self._cfg.temp_dir)
        subprocess.run(["bash", "-c", cmd], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.announcer("Ran inferencer task in Mallet")