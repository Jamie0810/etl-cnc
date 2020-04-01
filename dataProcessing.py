from lib import *
from Filemve import *
from mySQLConnection import *
from dataCleaning import dataCleaning
from createColumnMeta import createColumnMeta
from createBasicAnalysisData import createBasicAnalysisData
from createRawData import createRawData
from insertRecordsIntoDB import insertRecordsIntoDB
from createMySQLTables import createMySQLTables

flag_change_part = []
def dataProcessing(lineList, dataSetNum, dataSetCount):
    logging.info('Converting lineList to long_df...')
    lineSplit = [line.split(' ')[:14] for line in lineList]
    long_df = pd.DataFrame(lineSplit)
    long_df[6] = list(map(lambda s: s.replace('?', ''), long_df[6]))
    long_df[9] = list(map(lambda s: s.replace('?', ''), long_df[9]))
    long_df[11] = list(map(lambda s: s.replace('?', ''), long_df[11]))
    long_df[13] = list(map(lambda s: s.replace('?', ''), long_df[13]))
    long_df = long_df.loc[:, [6, 9, 11, 12, 13]]

    logging.info('Getting machine_num...')
    long_df = long_df.groupby([6]).tail(1)
    long_df = long_df.reset_index(drop=True)
    machine_num_index = list(np.where(long_df[6] == 'machine_num')[0])
    machine_num = long_df.iloc[machine_num_index][9]
    machine_num.index = range(len(machine_num))
    machine_num = machine_num[0]
    logging.info('Machine_num = ' + machine_num)

    logging.info('Getting all columns...')
    all_columns = list(set(long_df[6].tolist()))
    all_columns.remove('')
    all_columns.remove('send')
    all_columns.sort()

    logging.info('Reshaping long_df from long to wide format and assign groups with timestamp to df...')
    col_count = len(all_columns)
    pre_time = ''
    record = []
    records_list = []

    for l in lineList:
        str_list = l.split(' ')
        if str_list[6] != '' and str_list[6][0] == '?' and str_list[11][0] == '?':
            request = str_list[6][1:]
            if request in all_columns:
                date = str_list[11][1:5] + str_list[11][5:7] + str_list[11][7:]
                time = str_list[12][0:2] + str_list[12][2:4] + str_list[12][4:]
                millisec = str_list[13][:-1]
                cur_time = date + time + '.' + millisec
                response = str_list[9].replace('?', '')
                if cur_time != pre_time:
                    if record != []:
                        records_list.append(record)
                    record = [cur_time] + ['' for i in range(col_count)]
                    pre_time = cur_time
                record[all_columns.index(request) + 1] = response
    records_list.append(record)

    df = pd.DataFrame(records_list, columns=['date_time'] + all_columns)
    df = df[['date_time'] + all_columns]
    df[all_columns] = df[all_columns].apply(pd.to_numeric, errors='ignore')
    df = df.reset_index(drop=True)
    df = df.replace('', np.nan)
    logging.info('There are a total of ' + str(col_count) + ' columns and ' + str(df.shape[0]) + ' records in df')
    logging.info('Columns: ' + str(all_columns))

    logging.info('----------Creating rawData...----------')
    rawData = df.copy()
    rawData = createRawData(rawData, machine_num)
    lastRecord = getLastRawData(machine_num)
    if lastRecord != None:
        dateTime = lastRecord[0]
        raw_data = ujson.loads(lastRecord[1])
        if rawData['date_time'][0] == dateTime:
            isDuplicateEntry = True
            rawData['raw_data'][0] = ujson.loads(rawData['raw_data'][0])
            rawData['raw_data'][0].update(raw_data)
            rawData['raw_data'][0] = ujson.dumps(rawData['raw_data'][0])
        else:
            isDuplicateEntry = False
    else:
        isDuplicateEntry = False

    logging.info('----------Creating columnMeta...----------')
    columnMeta = long_df.copy()
    columnMeta = createColumnMeta(columnMeta, machine_num)

    logging.info('----------Creating basicAnalysisData...----------')
    basicAnalysisData = createBasicAnalysisData(records_list, all_columns, machine_num)

    logging.info('Adding flag_change_part & uuid to basicAnalysisData...')
    workcount_list = basicAnalysisData['workcount'].tolist()
    basicAnalysisData['flag_change_part'] = [False] + [workcount_list[i] != workcount_list[(i - 1)] for i in
                                                       range(1, len(workcount_list))]
    flag_change_part_index = list(np.where(basicAnalysisData['flag_change_part'] == True)[0])
    basicAnalysisData['uuid'] = None
    for i in flag_change_part_index:
        basicAnalysisData['uuid'][i] = str(uuid.uuid1())
    lastUUID = getLastUUID(machine_num)
    if (lastUUID):
        basicAnalysisData['uuid'][0] = lastUUID
    basicAnalysisData['uuid'] = basicAnalysisData['uuid'].ffill()

    logging.info('Adding flag_working to basicAnalysisData...')
    basicAnalysisData['flag_working'] = dataCleaning(basicAnalysisData)

    logging.info('Converting basicAnalysisData to JSON format...')
    basicAnalysisData_toJson = basicAnalysisData.copy()
    basicAnalysisData_toJson.drop(['date_time', 'flag_change_part', 'flag_working', 'uuid'], axis=1, inplace=True)
    basicAnalysisData_toJson = basicAnalysisData_toJson.apply(lambda x: [x.dropna()], axis=1).to_json()
    basicAnalysisData_list = []
    for i in ujson.loads(basicAnalysisData_toJson).values():
        basicAnalysisData_list.append(ujson.dumps(i[0]))
    basicAnalysisData['raw_data'] = pd.DataFrame(basicAnalysisData_list)
    basicAnalysisData = basicAnalysisData.loc[:, ['date_time', 'raw_data', 'flag_working', 'flag_change_part', 'uuid']]
    basicAnalysisData['machine_num'] = machine_num
    basicAnalysisData['line'] = line
    basicAnalysisData['floor'] = floor
    basicAnalysisData['location'] = location

    if isDuplicateEntry == True:
        basicAnalysisData['raw_data'][0] = ujson.loads(basicAnalysisData['raw_data'][0])
        basicAnalysisData['raw_data'][0].update(raw_data)
        basicAnalysisData['raw_data'][0] = ujson.dumps(basicAnalysisData['raw_data'][0])

    insertRecordsIntoDB('raw_data', rawData, isDuplicateEntry)
    insertRecordsIntoDB('basic_analysis_data', basicAnalysisData, isDuplicateEntry)
    allRecordsInsertResult = insertRecordsIntoDB('column_meta', columnMeta, isDuplicateEntry)

    for item in basicAnalysisData['flag_change_part']:
        if (item == True):
            flag_change_part.append(item)

    if (dataSetNum == dataSetCount and allRecordsInsertResult == 'All records have been inserted successfully'):
        logging.info('----------' + allRecordsInsertResult + '----------')
        print(allRecordsInsertResult)
        moveFilesToSuccDir(tempDir, currentDate)
        if (len(flag_change_part)) != 0:
            activateInference()

def getLastUUID(machine_num):
    condition = "machine_num='" + machine_num + "'and line='" + line + "'and floor='" + floor + "'and location='" + location + "'"
    cur.execute('SELECT * FROM basic_analysis_data WHERE ' + condition + ' ORDER BY date_time DESC LIMIT 0, 1')
    records = cur.fetchall()
    if (records):
        for row in records:
            uuid = row[4]
    else:
        uuid = None
    return uuid

def getLastRawData(machine_num):
    condition = "machine_num='" + machine_num + "'and line='" + line + "'and floor='" + floor + "'and location='" + location + "'"
    cur.execute('SELECT * FROM raw_data WHERE ' + condition + ' ORDER BY date_time DESC LIMIT 0, 1')
    records = cur.fetchall()
    lastRecord = []
    if (records):
        for row in records:
            dateTime = str(row[0]).replace('-', '').replace(' ', '').replace(':', '')[:18]
            rawData = row[1]
        lastRecord = dateTime, rawData
    else:
        lastRecord = None
    return lastRecord

# activate inference.py only once if tool part changes
def activateInference():
    logging.info('----------Activate alarm_inference.py----------')
    print('----------Activate alarm_inference.py----------')
    # os.system('python3.7 alarm_inference.py')
    logging.info('----------Activate health_inference.py----------')
    print('----------Activate health_inference.py----------')
    # os.system('python3.7 health_inference.py')
