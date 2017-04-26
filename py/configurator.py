import multiprocessing as mp
import warnings
import yaml

CONVERT_TASK = 1
TRAIN_TASK = 2


class TaskSettings(object):
    num_cores: int
    type: int

    def __init__(self, num_cores):
        self.num_cores = num_cores


class ConvertSettings(TaskSettings):
    headers: bool
    numbered: bool
    space_name: str
    file_list: list
    stopword_file: str

    def __init__(self, num_cores):
        self.type = CONVERT_TASK
        super().__init__(num_cores)


class TrainSettings(TaskSettings):
    space_name: str

    def __init__(self, num_cores):
        self.type = TRAIN_TASK
        super().__init__(num_cores)


class CalculateSettings(TaskSettings):
    sentence_files: list
    pair_mode: str
    headers: bool
    output_file: str
    output_null: str

    def __init__(self, num_cores):
        super().__init__(num_cores)


class ConfigSettings(object):
    tasks: list
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
            task.stopword_file = t["stopwords"]
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

    def _load_task(self, t):
        try:
            if t["type"] == "file_convert":
                return self._initialize_convert(t)
            else:
                raise Exception("Only the file_convert task is supported at this time.")
        except KeyError:
            raise Exception("Task type must be specified")