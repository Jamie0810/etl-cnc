import os, logging, logging.config, shutil, sys, traceback, ujson, warnings, uuid
import numpy as np
import pandas as pd
import configparser
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Turn off the warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)
pd.set_option('mode.chained_assignment', None)

config = configparser.ConfigParser()
config.read('config.ini')

# Directory
plcDataDir = config['directory']['plc-data']
tempDir = config['directory']['data-temp']
csvDir = config['directory']['csv']

# MySQL 
host = config['MySQL']['host']
user = config['MySQL']['user']
passwd = config['MySQL']['passwd']
database = config['MySQL']['database']
max_allowed_packet = config['MySQL']['max_allowed_packet']
insertSize = int(config['MySQL']['insertSize'])

# machineInfo
line = config['machineInfo']['line']
floor = config['machineInfo']['floor']
location = config['machineInfo']['location']

# maxLine
maxLine = int(config['maxLine']['maxLine'])

# configure_logger set level
logger_level = config['configure_logger']['level']

# create folders with current date
currentDate = datetime.now().strftime('%Y-%m-%d')
currentDateDir = config['directory']['data-temp'] + currentDate

logging.basicConfig(
    level = logger_level, 
    format = '%(asctime)s %(levelname)s: %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    filename = config['directory']['log'] + currentDate + '.log'
)