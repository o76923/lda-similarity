import pandas as pd
from sklearn.metrics.pairwise import pairwise_distances
from functools import partial
from py.configurator import Similarity
from itertools import combinations
import numpy as np


class SimilarityCalculator(object):

    def __init__(self, config: Similarity, announcer):
        self._cfg = config
        self.announcer = partial(announcer, process="SimCalculator")

    def _load_sentence_distributions(self):
        df = pd.read_csv("/app/data/temp/item.topics",
                         skiprows=1,
                         header=None,
                         index_col=0,
                         sep="\t")
        df.columns = ["sentence_id", ] + ["topic-{}".format(i) for i in range(df.shape[1]-1)]
        df.set_index("sentence_id", drop=True, append=False, inplace=True)
        return df

    def get_pair_distance(self, sentence_distributions):
        dist_matrix = pairwise_distances(sentence_distributions.as_matrix(), metric='cosine', n_jobs=self._cfg.num_cores)
        dist_matrix = np.triu(np.ones(shape=dist_matrix.shape) - dist_matrix, 1)
        return dist_matrix

    def write_sims_to_file(self, index, dist_matrix):
        indices = np.transpose(np.triu_indices(len(index), 1))
        with open("/app/data/output/{}".format(self._cfg.out_file), "w") as out_file:
            for (left, right), (li, ri) in zip(combinations(index, 2), indices):
                out_file.write("{},{},{:0.4f}\n".format(left, right, dist_matrix[li, ri]))
        self.announcer("Wrote to file")

    def main(self):
        sentence_distributions = self._load_sentence_distributions()
        self.announcer("Loaded distributions")
        keys = sentence_distributions.index.tolist()
        sims = self.get_pair_distance(sentence_distributions)
        self.announcer("Calculated distances")
        self.write_sims_to_file(keys, sims)
