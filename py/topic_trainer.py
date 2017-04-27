from functools import partial

import subprocess

from py.configurator import TrainSettings


class TopicTrainer(object):

    def __init__(self, config: TrainSettings, announcer):
        self._cfg = config
        self.announcer = partial(announcer, process="TopicTrainer")

    def main(self):
        cmd = "/app/Mallet/bin/mallet " \
              "train-topics " \
              "--input /app/data/temp/{space_name}.mallet " \
              "--output-state /app/data/spaces/{space_name}/topic_state.gz " \
              "--inferencer-filename /app/data/spaces/{space_name}/inferencer.gz " \
              "--num-topics {num_topics} " \
              "--num-threads {num_threads} " \
              "--num-iterations {iterations} " \
              "--num-icm-iterations {icm_iterations} " \
              "--optimize-interval {optimize_interval} " \
              "--optimize-burn-in {burn_in} " \
              "--alpha {alpha} " \
              "--beta {beta}".format(space_name=self._cfg.space_name,
                                     num_topics=self._cfg.num_topics,
                                     num_threads=self._cfg.num_cores,
                                     iterations=self._cfg.iterations,
                                     icm_iterations=self._cfg.icm_iterations,
                                     optimize_interval=self._cfg.optimize_interval,
                                     burn_in=self._cfg.burn_in,
                                     alpha=self._cfg.alpha,
                                     beta=self._cfg.beta)
        if self._cfg.symmetric_alpha:
            cmd += " --use-symmetric-alpha true"
        subprocess.run(cmd.format(self._cfg.space_name), shell=True)