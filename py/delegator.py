from datetime import datetime
from functools import partial

from py.configurator import ConfigSettings, CONVERT_TASK, TRAIN_TASK, INFERENCE_TASK
from py.file_converter import FileConverter
from py.topic_trainer import TopicTrainer
from py.inferencer import Inferencer


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
    for task in cfg.tasks:
        for st in task.subtasks:
            if st.type == CONVERT_TASK:
                c = FileConverter(st, partial(echo_message, start=start_time))
                c.main()
        echo_message("Ran all subtasks", process="Delegator", start=start_time)
        if task.type == TRAIN_TASK:
            print("train task")
            t = TopicTrainer(task, partial(echo_message, start=start_time))
        elif task.type == INFERENCE_TASK:
            print("inference task")
            t = Inferencer(task, partial(echo_message, start=start_time))
        try:
            t.main()
        except NameError:
            raise Exception("Task was of an invalid type")
        announcer("Finished Task")
    announcer("Done")
