import multiprocessing as mp
import os
import warnings
import yaml
from uuid import uuid4
from typing import List

CONVERT_TASK = 1
TRAIN_TASK = 2
INFERENCE_TASK = 3
SIMILARITY_TASK = 4
CONFIG_FILE = "/app/data/"+os.environ.get("CONFIG_FILE", "config.yml")


class Task(object):
    num_cores: int
    type: int
    space_name: str

    def __init__(self, num_cores, temp_dir):
        self.num_cores = num_cores
        self.temp_dir = temp_dir


class Convert(Task):
    headers: bool
    numbered: bool
    file_list: List[str]
    stopword_file: str
    out_file: str
    pipe_from_space: bool

    def __init__(self, num_cores, temp_dir):
        super().__init__(num_cores, temp_dir)
        self.type = CONVERT_TASK
        self.pipe_from_space = False


class Train(Task):
    num_topics: int
    iterations: int
    icm_iterations: int
    optimize_interval: int
    burn_in: int
    symmetric_alpha: bool
    alpha: float
    beta: float

    def __init__(self, num_cores, temp_dir):
        super().__init__(num_cores, temp_dir)
        self.type = TRAIN_TASK


class Inference(Task):
    in_file: str
    out_file: str

    def __init__(self, num_cores, temp_dir):
        super().__init__(num_cores, temp_dir)
        self.type = INFERENCE_TASK


class Similarity(Task):
    out_file: str

    def __init__(self, num_cores, temp_dir):
        super().__init__(num_cores, temp_dir)
        self.type = SIMILARITY_TASK


class ConfigSettings(object):
    tasks: List['Task']
    num_cores: int
    temp_dir: str

    def __init__(self):
        self._read_config()
        self._load_global()
        self.temp_dir = "/tmp/lda_calc_{}".format(uuid4())
        self.tasks = []
        for t in self._cfg['tasks']:
            self._load_task(t)

    def _read_config(self):
        with open(CONFIG_FILE) as in_file:
            self._cfg = yaml.load(in_file.read())

    def _load_global(self):
        try:
            self.num_cores = int(self._cfg["options"]["cores"])
        except KeyError:
            self.num_cores = mp.cpu_count() - 1
            raise Warning("Number of cores not specified, defaulting to one less than max")
        except TypeError:
            self.num_cores = mp.cpu_count() - 1
            raise Warning("The number of cores must be an int, defaulting to one less than max instead")

    def _initialize_convert(self, t):
        task = Convert(self.num_cores, self.temp_dir)
        try:
            task.space_name = t["space"]
        except KeyError:
            raise Exception("You must specify the name of the topic space you are creating")

        try:
            task.file_list = t["from"]["files"]
        except KeyError:
            raise Exception("You must specify some files to read from")

        try:
            task.headers = t["from"]["headers"]
        except KeyError:
            task.headers = False

        try:
            task.numbered = t["from"]["numbered"]
        except KeyError:
            task.numbered = False

        try:
            task.stopword_file = t["options"]["stopwords"]
        except KeyError:
            task.stopword_file = None
            warnings.warn("No stopwords list specified, using default from Mallet")

        task.out_file = "item.mallet"

        self.tasks.append(task)

    def _initialize_train(self, t):
        task = Train(self.num_cores, self.temp_dir)

        try:
            task.space_name = t["space"]
        except KeyError:
            raise Exception("You must specify the space to train topics on")

        try:
            task.num_topics = int(t["options"]["topics"])
        except KeyError:
            task.num_topics = 10
        except TypeError:
            raise Exception("The number of topics option must be an integer")

        try:
            task.iterations = int(t["options"]["iterations"])
        except KeyError:
            task.iterations = 1000
        except TypeError:
            raise Exception("The number of iterations option must be an integer")

        try:
            task.icm_iterations = int(t["options"]["icm"])
        except KeyError:
            task.icm_iterations = 0
        except TypeError:
            raise Exception("The number of icm iterations option must be an integer")

        try:
            task.alpha = float(t["hyperparameters"]["alpha"])
        except KeyError:
            task.alpha = 5.0
        except TypeError:
            raise Exception("The hyper parameter alpha must be a float")

        try:
            task.beta = float(t["hyperparameters"]["beta"])
        except KeyError:
            task.beta = 0.01
        except TypeError:
            raise Exception("The hyper parameter beta must be a float")

        try:
            task.optimize_interval = int(t["hyperparameters"]["interval"])
        except KeyError:
            task.optimize_interval = 0
        except TypeError:
            raise Exception("The hyper parameter interval must be an int")

        try:
            task.burn_in = int(t["hyperparameters"]["burn_in"])
        except KeyError:
            task.burn_in = 2 * task.optimize_interval
        except TypeError:
            raise Exception("The hyper parameter burn_in must be an int")

        try:
            task.symmetric_alpha = int(t["hyperparameters"]["symmetric_alpha"])
        except KeyError:
            task.symmetric_alpha = False
        except TypeError:
            raise Exception("The hyper parameter symmetric_alpha must be a boolean")

        self._initialize_convert(t)
        self.tasks[-1].out_file = "{space_name}.mallet".format(space_name=task.space_name)

        self.tasks.append(task)

    def _initialize_inference(self, t):
        task = Inference(self.num_cores, self.temp_dir)
        try:
            task.space_name = t["space"]
        except KeyError:
            raise Exception("You must specify the space to train topics on")

        task.in_file = "item.mallet"
        task.out_file = "item.topics"

        self._initialize_convert(t)
        self.tasks[-1].output_file = task.in_file
        self.tasks.append(task)

    def _initialize_similarity(self, t):
        task = Similarity(self.num_cores, self.temp_dir)

        try:
            task.space_name = t["space"]
        except KeyError:
            raise Exception("You must specify the space to train topics on")

        try:
            task.out_file = t["output"]["filename"]
        except KeyError:
            task.out_file = "sims.csv"

        self._initialize_inference(t)
        # self.tasks[-1].in_file, self.tasks[-1].out_file = "item.mallet", "item.topics"
        self.tasks.append(task)

    def _load_task(self, t):
        try:
            if t["type"] == "file_convert":
                return self._initialize_convert(t)
            elif t["type"] == "train_topics":
                return self._initialize_train(t)
            elif t["type"] == "infer_topics":
                return self._initialize_inference(t)
            elif t["type"] == "calculate_similarity":
                return self._initialize_similarity(t)
            else:
                raise Exception("Only the file_convert and train_topics tasks are supported at this time.")
        except KeyError:
            raise Exception("Task type must be specified")