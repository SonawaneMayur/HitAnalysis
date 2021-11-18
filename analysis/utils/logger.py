"""
    File name: logger.py
    Author: Mayur Sonawane
    Date Created: 11/14/2021
    Date Updated: 11/16/2021
    Python Version: 3.9
    Description: Logger class
"""
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

    def get_logger(self, name="app"):
        return logging.getLogger(name)

# log = Logger().get_logger("abc")
# log.info("abc")
#
# logging.basicConfig(filename='/Users/mayursonawane/PycharmProjects/hit_analysis/logs/app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger("app")
# logger.warning('This will get logged to a file')
