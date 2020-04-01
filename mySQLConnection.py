from lib import *

config = configparser.ConfigParser()
config.read('config.ini')

conn = mysql.connector.connect(
        host = host,
        user = user,
        passwd = passwd,
        database = database
    )
cur = conn.cursor()