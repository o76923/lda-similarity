import shutil
import warnings
import subprocess
from py.configurator import ConvertSettings
from functools import partial
import os


class FileConverter(object):

    def __init__(self, config: ConvertSettings, announcer):
        self._cfg = config
        self.announcer = partial(announcer, process="FileConverter")

    def _load_files(self, file_list):
        paragraph_id = 0
        for file_name in file_list:
            with open("/app/data/{}".format(file_name)) as in_file:
                if self._cfg.headers:
                    in_file.readline()
                if self._cfg.numbered:
                    for line in in_file:
                        paragraph_id, text = line[:-1].split("\t")
                        yield paragraph_id, text
                else:
                    for line in in_file:
                        text = line[:-1]
                        yield paragraph_id, text
                        paragraph_id += 1
            self.announcer("Finished File {}".format(file_name))

    def _import_file(self):
        cmd = "/app/Mallet/bin/mallet " \
              "import-file " \
              "--input /app/data/temp/mallet_input.txt " \
              "--output /app/data/temp/{output_file} " \
              "--keep-sequence ".format(output_file=self._cfg.output_file)
        if self._cfg.pipe_from_space:
            cmd += "--use-pipe-from /app/data/spaces/{space_name}/topic_state.gz ".format(space_name=self._cfg.space_name)
        if self._cfg.stopword_file:
            try:
                shutil.copyfile("/app/data/{}".format(self._cfg.stopword_file), "/app/data/spaces/{0}/stopwords.txt".format(self._cfg.space_name))
            except FileExistsError:
                warnings.warn("Stopwords file already exists for this space. Using existing file instead of provided one.")
            except FileNotFoundError:
                raise Exception("The file for stopwords was not found.")
            except shutil.SameFileError:
                warnings.warn("That stopwords file is already present in the space.")
            cmd += "--stoplist-file /app/data/spaces/{0}/stopwords.txt".format(self._cfg.space_name)
        else:
            cmd += "--remove-stopwords"
        subprocess.run(cmd, shell=True)
        self.announcer("Ran file_convert task in Mallet")

    def main(self):
        try:
            os.mkdir("/app/data/temp")
        except FileExistsError:
            pass

        with open("/app/data/temp/mallet_input.txt".format(self._cfg.space_name, self._cfg.output_file), "w") as out_file:
            for paragraph_id, text in self._load_files(self._cfg.file_list):
                out_file.write("{}\ten\t{}\n".format(paragraph_id, text))
        self._import_file()
