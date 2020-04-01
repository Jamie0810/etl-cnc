from lib import *

def createColumnMeta(columnMeta, machine_num):
    try:
        columnMeta['column_name'] = columnMeta[6]
        columnMeta['value'] = columnMeta[9]
        columnMeta['last_date'] = columnMeta[11] + columnMeta[12] + '.' + columnMeta[13]
        # identify dirty data by date_time & get them removed
        for i, item in enumerate(columnMeta['last_date']):
            if len(item) != 18:
                columnMeta = columnMeta.drop([i])
        columnMeta = columnMeta.reset_index(drop = True)
        columnMeta = columnMeta.loc[:,['column_name', 'value', 'last_date']]
        columnMeta = columnMeta[ ~ columnMeta['column_name'].isin(['send'])]
        columnMeta = columnMeta[ ~ columnMeta['column_name'].isin([''])]
        columnMeta.index = range(len(columnMeta))
        columnMeta['machine_num'] = machine_num
        columnMeta['line'] = line
        columnMeta['floor'] = floor
        columnMeta['location'] = location
        logging.info('columnMeta has been created')
        return columnMeta
    
    except:
        logging.error('Failed to create columnMeta')
        logging.error(traceback.format_exc())
        logging.error('The program terminated abnormally!!')
        print('The program terminated abnormally!!')
        sys.exit()