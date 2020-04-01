#!/usr/bin/python
import logging
from datetime import datetime

currentDate = datetime.now().strftime('%Y-%m-%d')

logging.basicConfig(
    level = logging.INFO, 
    format = '%(asctime)s %(levelname)s: %(message)s', 
    datefmt = '%Y-%m-%d %H:%M:%S',
    filename = '/Foxconn/Projects/CNCTool/data-processing/' + currentDate + '.log'
)

print('====test====')
logging.info('====test====')
