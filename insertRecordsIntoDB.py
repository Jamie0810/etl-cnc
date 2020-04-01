from lib import *
from mySQLConnection import *
from Filemve import *

def executemany_size(sql, data):
    dataSet = [data[i: i + insertSize] for i in range(0, len(data), insertSize)]
    for i in range(len(dataSet)):
        cur.executemany(sql,dataSet[i])

def insertRecordsIntoDB(tableName, records, isDuplicateEntry):
    allRecordsInsertResult = ''
    logging.info('Inserting '+ str(len(records.index)) + ' records into ' + tableName + '...')
    
    try:
        cols = ",".join([str(i) for i in records.columns.tolist()])
        data = []
        if (tableName == 'raw_data'):
            if (isDuplicateEntry == True):
                for i, row in records.iterrows():
                    if i == 0:
                        sql = ("UPDATE " + tableName + " SET \
                            date_time=" + "%s," + "raw_data=" + "%s," + "\
                            machine_num=" + "%s," + "line=" + "%s," + "\
                            floor=" + "%s," + "location=" + "%s" + " \
                            where date_time=" + "'" + row[0] + "'")
                        cur.execute(sql, tuple(row))
                        conn.commit()
                for i, row in records.iterrows():
                    sql = ("INSERT INTO " + tableName + "(" + cols + ")VALUES\
                        (" + "%s," + "%s," + "%s," + "%s," + "%s," + "%s)")
                    data.append(tuple(row))
                data.pop(0)
                executemany_size(sql, data)
            else:
                sql = ("INSERT INTO " + tableName + "(" + cols + ")VALUES\
                    (" + "%s," + "%s," + "%s," + "%s," + "%s," + "%s)")
                for i, row in records.iterrows():
                    data.append(tuple(row))
                executemany_size(sql, data)
            conn.commit()

        if (tableName == 'basic_analysis_data'):
            if (isDuplicateEntry == True):
                for i, row in records.iterrows():
                    if i == 0:
                        sql = ("UPDATE " + tableName + " SET \
                            date_time=" + "%s," + "raw_data=" + "%s," + "\
                            flag_working=" + "%s," + "flag_change_part=" + "%s," + "\
                            uuid=" + "%s," + "machine_num=" + "%s," + "\
                            line=" + "%s," + "floor=" + "%s," + "\
                            location=" + "%s" + " where date_time=" + "'" + row[0] + "'")
                        cur.execute(sql, tuple(row))
                        conn.commit()
                for i, row in records.iterrows():
                    sql = ("INSERT INTO " + tableName + "(" + cols + ")VALUES\
                        (" + "%s," + "%s," + "%s," + "%s," + "%s," + "%s," + "%s," + "%s," + "%s)")
                    data.append(tuple(row))
                data.pop(0)
                executemany_size(sql, data)
            else:
                sql = ("INSERT INTO " + tableName + "(" + cols + ")VALUES\
                    (" + "%s," + "%s," + "%s," + "%s," + "%s," + "%s," + "%s," + "%s," + "%s)")
                for i, row in records.iterrows():
                    data.append(tuple(row))
                executemany_size(sql, data)
            conn.commit()

        if (tableName == 'column_meta'):
            cur.execute('SELECT * FROM column_meta')
            column_meta = cur.fetchall()
            if (column_meta):
                for i, row in records.iterrows():
                    sql = ("UPDATE " + tableName + " SET \
                        value=" + "%s," + "last_date=" + "%s," + "\
                        machine_num=" + "%s," + "line=" + "%s," + "\
                        floor=" + "%s," + "location=" + "%s" + " \
                        where column_name=" + "'" + row[0] + "'")
                    tupleRow = tuple(row)
                    tupleRow = tupleRow[:0] + tupleRow[1:]
                    cur.execute(sql, tupleRow)
            else:
                sql = ("INSERT INTO " + tableName + "(" + cols + ")\
                    VALUES(" + "%s," + "%s," + "%s," + "%s," + "%s," + "%s," + "%s)")
                for i, row in records.iterrows():
                    data.append(tuple(row))
                executemany_size(sql, data)
            conn.commit()

        logging.info('Records have been inserted into '+ tableName + ' successfully')
        if (tableName == 'column_meta'):
             allRecordsInsertResult = 'All records have been inserted successfully'
        return allRecordsInsertResult

    except mysql.connector.Error as e:
        logging.error('Something went wrong with MySQL')
        logging.error(e)
        conn.rollback()
        moveFilesToFailDir()
        logging.error('The program terminated abnormally!!')
        print('The program terminated abnormally!!')
        sys.exit()

    except:
        logging.error('Something went wrong while inserting records...')
        logging.error(traceback.format_exc())
        logging.error('The program terminated abnormally!!')
        print('The program terminated abnormally!!')
        sys.exit()