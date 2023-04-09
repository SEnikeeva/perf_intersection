import logging
import os

from diff import diff_detection
from helpers.configs import Config
from helpers.utils import replace_slash, clear_out_folder
from reader import base_reader, model_reader

if __name__ == '__main__':
    input_folder = "input_data"
    out_folder = "output_data"

    if not os.path.exists(out_folder):
        os.makedirs(out_folder)
    else:
        clear_out_folder(out_folder)

    out_path_log = replace_slash(f"{out_folder}\\Report.txt")
    logging.basicConfig(format=u'%(levelname)-8s : %(message)s',
                        filename=out_path_log, filemode='w')

    cfg = Config(replace_slash(f"{input_folder}\\config.json"), input_folder)
    base_perf = base_reader(cfg.base_perf_path)
    model_perf = model_reader(cfg.model_perf_path)

    diff_detection(model_perf, base_perf, out_folder, cfg)
