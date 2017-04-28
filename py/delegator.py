import os
from shutil import rmtree
from datetime import datetime
from functools import partial

import subprocess

from py.configurator import ConfigSettings, CONVERT_TASK, TRAIN_TASK, INFERENCE_TASK, SIMILARITY_TASK
from py.file_converter import FileConverter
from py.topic_trainer import TopicTrainer
from py.inferencer import Inferencer
from py.similarity_calculator import SimilarityCalculator


def echo_message(msg, process, start):
    diff = datetime.now()-start
    days = diff.days
    hours = diff.seconds//3600
    minutes = diff.seconds//60 - hours * 60
    seconds = diff.seconds - minutes * 60
    print("{process:<20}{ts:<20}{msg:<39}".format(process=process, ts="{:02d}d {:02d}h {:02d}m {:02d}s".format(days, hours, minutes, seconds), msg=msg))


if __name__ == "__main__":
    start_time = datetime.now()
    announcer = partial(echo_message, process="Delegator", start=start_time)
    cfg = ConfigSettings()
    announcer("Loaded Configuration")

    os.makedirs(cfg.temp_dir)
    announcer("Created temp directory at {}".format(cfg.temp_dir))

    for task in cfg.tasks:
        if task.type == CONVERT_TASK:
            t = FileConverter(task, partial(echo_message, start=start_time))
        elif task.type == INFERENCE_TASK:
            t = Inferencer(task, partial(echo_message, start=start_time))
        elif task.type == TRAIN_TASK:
            t = TopicTrainer(task, partial(echo_message, start=start_time))
        elif task.type == SIMILARITY_TASK:
            t = SimilarityCalculator(task, partial(echo_message, start=start_time))
        else:
            raise Exception("Task was an invalid type")
        t.main()
        announcer("Finished Task")
    announcer("Finished all tasks")

    rmtree(cfg.temp_dir)
    announcer("Removed temp directory")

    announcer("Done")
