from lib import *
from Filemve import *
from createMySQLTables import *
from readAndSplitFiles import *

if __name__=='__main__':
    try:
        createMySQLTables()
        tempDirList = []
        for fileName in os.listdir(tempDir):
            if (fileName.endswith('.log')):
                tempDirList.append(fileName)
        if len(tempDirList) > 0:
            logging.info('Files in data-temp: ' + str(tempDirList))
            splitFileIntoDatasets(tempDir, tempDirList)
            readPLCDir()
        else:
            logging.info('There are no files in data-temp')
            readPLCDir()
    except:
        logging.error('Failed to create columnMeta')
        logging.error(traceback.format_exc())
        logging.error('The program terminated abnormally!!')
        print('The program terminated abnormally!!')
        sys.exit()