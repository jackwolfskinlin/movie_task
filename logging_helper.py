# -*- coding: utf-8 -*-

import os
import logging
import logging.handlers


class MovieLog():
    """
    Args:
      log_dir       - 日志目录，若不存在会自动创建
                      warn/error级别的日志会输出到error目录
      level         - 日志最低输出级别
      when          - 时间间隔，天级('D')/小时级('H')
      backuptimes   - 日志保留份数(times）
      maxbytes      - 日志文件大小
      bactuprolls   - 日志保留份数()
      fmt           - 日志格式()
      datefmt       - 日期时间格式

    Raises:
        OSError: fail to create log directories
        IOError: fail to open log file
    """

    def __init__(
        self, log_dir="./log", level=logging.INFO,
        when="H", backuptimes=72,
        fmt="%(levelname)s: %(asctime)s: %(filename)s:%(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ):
        self.when = when
        self.backuptimes = backuptimes

        formatter = logging.Formatter(fmt, datefmt)
        error_dir = os.path.join(log_dir, "error/")

        self.error_log = self.get_logger("", error_dir, "error.log", logging.WARNING, formatter)

    def get_logger(self, name, log_dir, filename, level, formatter):
        logger = logging.getLogger(name)
        self.mkdirsifnotexit(log_dir)
        handler = logging.handlers.TimedRotatingFileHandler(
            filename=os.path.join(log_dir, filename),
            when=self.when,
            backupCount=self.backuptimes)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def mkdirsifnotexit(self, log_dir):
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)