# -*- coding=utf-8 -*-

import os
import logging

# import logging
#
# log_file_path = os.path.join(os.path.abspath('.'), 'warning.log')
# logging.basicConfig(level=logging.WARNING,
#                     format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     filename=log_file_path,
#                     filemode='a')
config_file_path = os.path.join(os.path.abspath('.'), 'config.ini')
# logging.getLogger()

#logger = logging_helper.MovieLog()

logger = logging.getLogger('luigi-interface')