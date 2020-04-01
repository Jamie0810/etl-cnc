from lib import *
from mySQLConnection import *

def createBasicAnalysisData(records_list, basicAnalysisData_columns, machine_num):
    try:
        columnMeta_df = queryColumnMeta(machine_num)
        if (columnMeta_df.empty == False):
            columnMeta_df_list = columnMeta_df.columns.values.tolist()
            logging.info('Checking if there are different columns between basicAnalysisData & column_meta...')
            diff = list(set(basicAnalysisData_columns).symmetric_difference(set(columnMeta_df_list)))
            basicLacksDiff = []
            metaLacksDiff = []
            if len(diff) > 0:
                for i in range(0, len(diff)):
                    if diff[i] not in basicAnalysisData_columns:
                        basicLacksDiff.append(diff[i])
                    else:
                        metaLacksDiff.append(diff[i])
                if (metaLacksDiff):
                    logging.info('Column_meta lacks ' + str(len(metaLacksDiff)) + ' columns: ' + str(metaLacksDiff))
                    for i in range(0, len(metaLacksDiff)):
                        columnMeta_df[metaLacksDiff[i]] = ''
                    columnMeta_df = columnMeta_df.sort_index(axis = 1)
                    columnMeta_df_list = columnMeta_df.columns.values.tolist()
                    basicAnalysisData = fillMissingValues(machine_num, records_list, basicAnalysisData_columns, columnMeta_df, columnMeta_df_list)
                if (basicLacksDiff):
                    logging.info('basic_analysis_data lacks ' + str(len(basicLacksDiff)) + ' columns: ' + str(basicLacksDiff))
                    columnMeta_df_temp = columnMeta_df.copy()
                    columnMeta_df_temp.drop(basicLacksDiff, axis=1, inplace=True)
                    columnMeta_df_temp = columnMeta_df_temp.sort_index(axis = 1)
                    columnMeta_df_list = columnMeta_df_temp.columns.values.tolist()
                    basicAnalysisData = fillMissingValues(machine_num, records_list, basicAnalysisData_columns, columnMeta_df_temp, columnMeta_df_list)
                    for col in basicLacksDiff:
                        basicAnalysisData[col] = columnMeta_df[col]
                    basicAnalysisData = basicAnalysisData.where((pd.notnull(basicAnalysisData)), None)
            else:
                basicAnalysisData = fillMissingValues(machine_num, records_list, basicAnalysisData_columns, columnMeta_df, columnMeta_df_list)
        else:
            basicAnalysisData = fillMissingValues(machine_num, records_list, basicAnalysisData_columns, columnMeta_df, None)
        logging.info('basicAnalysisData has been created')
        return basicAnalysisData

    except: 
        logging.error('Failed to fill the missing values in basicAnalysisData')
        logging.error(traceback.format_exc())
        logging.error('The program terminated abnormally!!')
        print('The program terminated abnormally!!')
        sys.exit()

def fillMissingValues(machine_num, records_list, basicAnalysisData_columns, columnMeta_df, columnMeta_df_list):
    logging.info('Filling the missing values in basicAnalysisData...')
    if (columnMeta_df.empty == False):
        for col, item in enumerate(records_list[0]):
            if item == '':
                records_list[0][col] = columnMeta_df.at[0, columnMeta_df_list[col - 1]]
    basicAnalysisData = pd.DataFrame(records_list, columns = ['date_time'] + basicAnalysisData_columns)
    basicAnalysisData = basicAnalysisData[['date_time'] + basicAnalysisData_columns]
    basicAnalysisData[basicAnalysisData_columns] = basicAnalysisData[basicAnalysisData_columns].apply(pd.to_numeric, errors = 'ignore')
    basicAnalysisData = basicAnalysisData.reset_index(drop = True) 
    basicAnalysisData = basicAnalysisData.replace('', np.nan)
    basicAnalysisData = basicAnalysisData.ffill()
    basicAnalysisData = basicAnalysisData.where((pd.notnull(basicAnalysisData)), None)
    return basicAnalysisData

def queryColumnMeta(machine_num):
    try:
        condition = "machine_num='" + machine_num + "'and line='" + line + "'and floor='" + floor + "'and location='" + location + "'"  
        cur.execute('SELECT * from column_meta WHERE ' + condition)
        records = cur.fetchall()
        columns = []
        values = []
        for row in records:
            columns.append(row[1])
            values.append(row[2])
        columnMeta_df = pd.DataFrame([values], columns = columns)
        columnMeta_df = columnMeta_df.sort_index(axis = 1)
        logging.info('There are a total of ' + str(len(columns)) + ' columns in column_meta: ' + str(columns))
        return columnMeta_df

    except Error as e:
        logging.error('Error querying data from column_meta' + e)