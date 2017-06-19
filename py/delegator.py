import os
from datetime import datetime
from functools import partial
from shutil import rmtree
from py.utils import *
from py.configurator import ConfigSettings
from py.file_converter import FileConverter
from py.inferencer import Inferencer
from py.topic_trainer import TopicTrainer
from py.sim_calculator import LDASim

start_time = datetime.now()
announcer = partial(announcer, process="Delegator", start=start_time)

cfg = ConfigSettings()
announcer("Loaded Configuration")

os.makedirs(cfg.temp_dir)
announcer("Created temp directory at {}".format(cfg.temp_dir))

try:
    for task in cfg.tasks:
        if task.type == TASK_TYPE.CONVERT:
            t = FileConverter(task, partial(announcer, start=start_time))
        elif task.type == TASK_TYPE.INFER:
            t = Inferencer(task, partial(announcer, start=start_time))
        elif task.type == TASK_TYPE.TRAIN:
            t = TopicTrainer(task, partial(announcer, start=start_time))
        elif task.type == TASK_TYPE.SIMILARITY:
            t = LDASim(task, partial(announcer, start=start_time))
        else:
            raise Exception("Task was an invalid type")
        t.main()
        announcer("Finished Task")
    announcer("Finished all tasks")
finally:
    rmtree(cfg.temp_dir)
    announcer("Removed temp directory")
announcer("Done")
