import os
from functools import partial
import subprocess
import pandas as pd

from py.configurator import Train


class TopicTrainer(object):

    def __init__(self, config: Train, announcer):
        self._cfg = config
        self.announcer = partial(announcer, process="TopicTrainer")

    def tww_to_df(self):
        df = pd.read_csv("/app/data/temp/topic_word_weights.txt",
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

    def main(self):
        cmd = "Mallet/bin/mallet " \
              "train-topics " \
              "--input /app/data/temp/{space_name}.mallet " \
              "--output-state /app/data/spaces/{space_name}/topic_state.gz " \
              "--inferencer-filename /app/data/spaces/{space_name}/inferencer.gz " \
              "--topic-word-weights-file /app/data/temp/topic_word_weights.txt " \
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
        subprocess.run(["bash", "-c", cmd], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.tww_to_df()
        self.announcer("Ran train_topic task in Mallet")
