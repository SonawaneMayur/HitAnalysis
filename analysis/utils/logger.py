import logging

from analysis import config


class Logger:
    file_name = config.log_file_name  # '/Users/mayursonawane/PycharmProjects/hit_analysis/logs/app.log'
    filemode = config.log_filemode  # 'w'
    format = config.log_format  # '%(name)s - %(levelname)s - %(message)s'
    level = config.log_level  # "INFO"

    def __init__(self):
        logging.basicConfig(filename=Logger.file_name, filemode=Logger.filemode, format=Logger.format,
                            level=Logger.level)

    def get_logger(self, name="app"):
        return logging.getLogger(name)

# log = Logger().get_logger("abc")
# log.info("abc")
#
# logging.basicConfig(filename='/Users/mayursonawane/PycharmProjects/hit_analysis/logs/app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger("app")
# logger.warning('This will get logged to a file')
