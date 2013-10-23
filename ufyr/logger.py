#!/usr/bin/env python

import os
import sys
import logging
import logging.handlers

class Logger():
    
    _logger=None
    
    def __init__(self, name, file_path, log_level=logging.DEBUG):
        self.name = name
        self.file_path = file_path
        self.log_level = log_level

    def get_logger(self):
        """
         Using a getLogger interface for now because in the future we might want
         to return our own instance of Logger and inject custom data onto the
         logging string behond the ability of the Python Logging's formatting abilities,
         These could be dynamically formatted server IP address, user ID, worker
         process IDs.  For now we will simply return the Python Logger.
        """
        Logger._logger = logging.getLogger(self.name)
        Logger._logger.setLevel(self.log_level)

        file_handler = logging.handlers.RotatingFileHandler(
            self.file_path,
            maxBytes=1000000,
            backupCount=5,
            delay=True)

        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
        file_handler.setFormatter(formatter)

        Logger._logger.addHandler(file_handler)

        return Logger._logger
