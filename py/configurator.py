import multiprocessing as mp
import warnings
import yaml
from typing import List

CONVERT_TASK = 1
TRAIN_TASK = 2
INFERENCE_TASK = 3


class TaskSettings(object):
    num_cores: int
    type: int
    space_name: str
    subtasks: List['TaskSettings']

    def __init__(self, num_cores):
        self.num_cores = num_cores
        self.subtasks = []


class ConvertSettings(TaskSettings):
    headers: bool
    numbered: bool
    file_list: List[str]
    stopword_file: str
    output_file: str
    pipe_from_space: bool

    def __init__(self, num_cores):
        self.type = CONVERT_TASK
        self.pipe_from_space = False
        super().__init__(num_cores)


class TrainSettings(TaskSettings):
    num_topics: int
    iterations: int
    icm_iterations: int
    optimize_interval: int
    burn_in: int
    symmetric_alpha: bool
    alpha: float
    beta: float

    def __init__(self, num_cores):
        self.type = TRAIN_TASK
        super().__init__(num_cores)


class InferenceSettings(TaskSettings):
    file_list: List[str]

    def __init__(self, num_cores):
        self.type = INFERENCE_TASK
        super().__init__(num_cores)


class ConfigSettings(object):
    tasks: List['TaskSettings']
    num_cores: int

    def __init__(self, filename="/app/data/config.yml"):
        self._read_config(filename)
        self._load_global()
        self.tasks = []

        for t in self._cfg['tasks']:
            self.tasks.append(self._load_task(t))

    def _read_config(self, filename):
        with open(filename) as in_file:
            self._cfg = yaml.load(in_file.read())

    def _load_global(self):
        try:
            self.num_cores = int(self._cfg["options"]["cores"])
        except KeyError:
            self.num_cores = mp.cpu_count() - 1
            raise Warning("Number of cores not specified, defaulting to one less than max")
        except TypeError:
            self.num_cores = mp.cpu_count() - 1
            raise Warning("The number of cores must be an int, defaulting to one less than max insetad")

    def _initialize_convert(self, t):
        task = ConvertSettings(self.num_cores)
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

        return task

    def _initialize_train(self, t):
        task = TrainSettings(self.num_cores)

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

        st = self._initialize_convert(t)
        st.output_file = "{space_name}.mallet".format(space_name=task.space_name)
        task.subtasks.append(st)

        return task

    def _initialize_inference(self, t):
        task = InferenceSettings(self.num_cores)
        try:
            task.space_name = t["space"]
        except KeyError:
            raise Exception("You must specify the space to train topics on")

        try:
            task.file_list = t["from"]["files"]
        except KeyError:
            raise Exception("You must specify documents to infer topics on")

        for f in task.file_list:
            st = self._initialize_convert(t)
            st.file_list = [f, ]
            st.output_file = f[:f.rfind(".")]+".mallet"
            task.subtasks.append(st)

        return task

    def _load_task(self, t):
        try:
            if t["type"] == "file_convert":
                return self._initialize_convert(t)
            elif t["type"] == "train_topics":
                return self._initialize_train(t)
            elif t["type"] == "infer_topics":
                return self._initialize_inference(t)
            else:
                raise Exception("Only the file_convert and train_topics tasks are supported at this time.")
        except KeyError:
            raise Exception("Task type must be specified")