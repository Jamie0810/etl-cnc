from lib import *
from dataProcessing import *
from mySQLConnection import *
from datetime import datetime, timedelta

def readPLCDir():
    plcDataDirList = []
    for fileName in os.listdir(plcDataDir):
        if (fileName.endswith('.log')):
            plcDataDirList.append(fileName)
    plcDataDirList.sort()
    if len(plcDataDirList) == 0:
        logging.info('There are no files in plc-data')
    else:
        logging.info('Files in plc-data: ' + str(plcDataDirList))
        for file in plcDataDirList:
            shutil.move(plcDataDir + file, tempDir)
            splitFileIntoDatasets(tempDir, [file])

def splitFileIntoDatasets(tempDir, tempDirList):
    logging.info('@@@@@@@@@@ Start to do Data Processing: ' + str(tempDirList) + ' @@@@@@@@@@')
    print('@@@@@@@@@@ Start to do Data Processing from file: ' + str(tempDirList) + ' @@@@@@@@@@')

    logging.info('Reading the file line by line...')
    count = -1
    for count, line in enumerate(open(tempDir + tempDirList[0], 'rU')):
        pass
    count += 1
    file_df = pd.read_table(
        tempDir + tempDirList[0], 
        chunksize = maxLine, 
        header = None
    )
    dataSetCount = count / maxLine
    dataSetNum = 0
    for dataSet in file_df:
        dataSetNum += 1
        dataSet_list = list(dataSet[0])
        dataSetLen = len(dataSet_list)
        doProcessing = checkIfDataSetExist(dataSet_list, dataSetNum)
        if doProcessing == 'yes':
            dataProcessing(dataSet_list, dataSetNum, dataSetCount)
        else:
            logging.error('Duplicate Entry for this dataset')
            print('Duplicate Entry for this dataset')

def checkIfDataSetExist(dataSet_list, dataSetNum):
    logging.info('Check if dataset '+ str(dataSetNum) + ' exists')
    long_df = pd.DataFrame([line.split(' ')[:14] for line in dataSet_list])
    long_df = long_df.loc[:, [6, 9, 11, 12, 13]]
    data_key = str(long_df[6][0]).replace('?', '')
    data_value = str(long_df[9][0]).replace('?', '')
    

    long_df['date_time'] = long_df[11] + long_df[12] + long_df[13]
    long_df['date_time'] = list(map(lambda s: s.replace('?', ''), long_df['date_time']))

    for row in range(0, 3):
        if len(long_df['date_time'][row]) != 17:
            long_df = long_df.drop([row])
    long_df = long_df.reset_index(drop = True)

    date_time = long_df['date_time'][0]
    date_time = str(datetime(
        year = int(date_time[0:4]),
        month = int(date_time[4:6]), 
        day = int(date_time[6:8]),
        hour = int(date_time[8:10]),
        minute = int(date_time[10:12]),
        second = int(date_time[12:14]),
    )) + '.'  + date_time[14:17]

    machine_num = 'D10'
    condition = "machine_num='" + machine_num + "\
        'and line='" + line + "\
        'and floor='" + floor + "\
        'and location='" + location + "\
        'and date_time='" + date_time + "'"
    cur.execute('SELECT * FROM raw_data WHERE ' + condition)
    records = cur.fetchall()
    if (records):
        for row in records:
            if data_key in row[1] or data_value in row[1]:
                doProcessing = 'no'
            else:
                doProcessing = 'yes'
    else:
        doProcessing = 'yes'
    return doProcessing