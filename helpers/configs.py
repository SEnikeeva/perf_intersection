import json
import logging
import sys

from dateutil import parser

from helpers.utils import replace_slash


def read(config_path):
    with open(config_path, 'r', encoding='utf-8') as f:
        try:
            conf = json.load(f)
        except BaseException as e:
            logging.error(f"Error loading config file. {str(e)}")
            sys.exit()
        return conf


class Config:
    def __init__(self, config_path, input_folder):
        self.input_folder = input_folder
        self.conf = read(config_path)
        self.base_perf_path = self.load(self.conf, "base_perf_path")
        self.model_perf_path = self.load(self.conf, "model_perf_path")
        self.min_date = self.load(self.conf, "min_date")
        self.max_date = self.load(self.conf, "max_date")
        self.grid_delta = self.load(self.conf, "grid_delta")

        self.grid_delta = 0 if self.grid_delta is None else self.grid_delta

    def load(self, conf, param):
        if 'paths' in param:
            paths = conf.get(param)
            if paths is not None:
                if len(paths) == 0:
                    return None
                paths = [replace_slash(f"{self.input_folder}\\{path}") for path in paths]
            return paths
        elif 'path' in param:
            path = conf.get(param)
            if path is not None:
                if path == '':
                    return None
                path = replace_slash(f"{self.input_folder}\\{path}")
            return path
        elif 'date' in param:
            param = conf.get(param)
            if param is None or param == '':
                return None
            else:
                str_date = parser.parse(param, dayfirst=True).date()
                return str_date
        else:
            param = conf.get(param)
            if hasattr(param, '__len__') and len(param) == 0:
                return None
            return None if param == '' else param
