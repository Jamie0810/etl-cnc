from lib import *

def createRawData(rawData, machine_num):
    try:
        logging.info('Converting rawData to JSON format...')
        rawData_toJson = rawData.loc[:, rawData.columns != 'date_time']
        rawData_toJson = rawData_toJson.apply(lambda x: [x.dropna()], axis=1).to_json()
        rawData_list = []
        for i in ujson.loads(rawData_toJson).values():
            rawData_list.append(ujson.dumps(i[0]))
        rawData['raw_data'] = pd.DataFrame(rawData_list)
        rawData = rawData.where((pd.notnull(rawData)), None)
        rawData['machine_num'] = machine_num
        rawData['line'] = line
        rawData['floor'] = floor
        rawData['location'] = location
        rawData = rawData.loc[:,['date_time', 'raw_data', 'machine_num', 'line', 'floor', 'location']]
        logging.info('rawData has been created')
        return rawData
        
    except:
        logging.error('Failed to create rawData')
        logging.error(traceback.format_exc())
        logging.error('The program terminated abnormally!!')
        print('The program terminated abnormally!!')
        sys.exit()