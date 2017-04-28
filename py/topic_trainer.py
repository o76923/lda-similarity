import os
from functools import partial
from py.runner import run_cmd
import pandas as pd

from py.configurator import Train


class TopicTrainer(object):

    def __init__(self, config: Train, announcer):
        self._cfg = config
        self.announcer = partial(announcer, process="TopicTrainer")

    def _make_space_dir(self):
        try:
            os.makedirs("/app/data/spaces/{space_name}".format(space_name=self._cfg.space_name))
        except FileExistsError:
            pass
        self.announcer("Created space directory if needed")

    def _make_cmd(self):
        cmd = "Mallet/bin/mallet " \
              "train-topics " \
              "--input {temp_dir}/{space_name}.mallet " \
              "--output-state /app/data/spaces/{space_name}/topic_state.gz " \
              "--inferencer-filename /app/data/spaces/{space_name}/inferencer.gz " \
              "--topic-word-weights-file {temp_dir}/topic_word_weights.txt " \
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
                                     beta=self._cfg.beta,
                                     temp_dir=self._cfg.temp_dir)
        if self._cfg.symmetric_alpha:
            cmd += " --use-symmetric-alpha true"
        self.announcer("Made cmd")
        return cmd

    def tww_to_df(self):
        df = pd.read_csv("{temp_dir}/topic_word_weights.txt".format(temp_dir=self._cfg.temp_dir),
                         header=None,
                         names=("topic_id", "word", "weight"),
                         dtype={
                             "topic_id": int,
                             "word": str,
                             "weight": float
                         },
                         index_col=None,
                         sep="\t")
        pv = df.pivot(index='word', columns='topic_id', values='weight')
        pv.to_pickle("/app/data/spaces/{space_name}/topic_word_weight.pkl".format(space_name=self._cfg.space_name))
        self.announcer("Ran train_topic task in Mallet")

    def main(self):
        self._make_space_dir()
        cmd = self._make_cmd()
        run_cmd(cmd)
        self.announcer("Ran command")
        self.tww_to_df()
