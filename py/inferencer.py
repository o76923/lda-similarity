from functools import partial

import subprocess

from py.configurator import InferenceSettings
import os


class Inferencer(object):

    def __init__(self, config: InferenceSettings, announcer):
        self._cfg = config
        self.announcer = partial(announcer, process="Inferencer")

    def main(self):
        # "--input-model /app/data/spaces/{space_name}/topic_state.gz " \
        try:
            os.mkdir("/app/data/temp")
        except FileExistsError:
            pass

        for f in self._cfg.file_list:
            out_file = f[:f.rfind(".")]+".topics"
            cmd = "/app/Mallet/bin/mallet " \
                  "infer-topics " \
                  "--inferencer /app/data/spaces/{space_name}/inferencer.gz " \
                  "--input /app/data/temp/{target_in} " \
                  "--output-doc-topics /app/data/output/{target_out} " \
                  "--num-iterations 100 ".format(space_name=self._cfg.space_name,
                                                 target_in=f[:f.rfind(".")]+".mallet",
                                                 target_out=out_file)
            subprocess.run(cmd.format(self._cfg.space_name), shell=True)