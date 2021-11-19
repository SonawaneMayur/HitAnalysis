import logging

from analysis import config


class Logger:
    file_name = config.log_file_name
    filemode = config.log_filemode
    format = config.log_format
    level = config.log_level

    def __init__(self):
        logging.basicConfig(filename=Logger.file_name, filemode=Logger.filemode, format=Logger.format,
                            level=Logger.level)
        # set up logging to console
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        # set a format which is simpler for console use
        formatter = logging.Formatter(Logger.format)
        console.setFormatter(formatter)
        # add the handler to the root logger
        logging.getLogger('').addHandler(console)

    def get_logger(self, name):
        return logging.getLogger(name)

    def down(self):
        logging.shutdown()
