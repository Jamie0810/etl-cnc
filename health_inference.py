from lib import *
import numpy as np
import pandas as pd
import matplotlib.pylab as plt
import mysql.connector
import json
from time import time 
from sklearn.metrics.pairwise import rbf_kernel

def connectDB():
    conn = mysql.connector.connect(
        host = host,
        user = user,
        passwd = passwd,
        database = database
    )
    cursor = conn.cursor()    
    # show tables
    cursor.execute('SHOW TABLES')
    for x in cursor:
        print(x)
    return conn, cursor

def createTable(cursor, tablename, colname, col_dtype):
    cols = ['`'+str(i)+'`' for i in colname]
    col_string = ','.join([x+' '+y for x, y in zip(cols, col_dtype)])
    execute_string = "CREATE TABLE IF NOT EXISTS "+ tablename +" ("+col_string+")"
    cursor.execute(execute_string)
    # show table schema
    cursor.execute('SHOW COLUMNS FROM '+tablename)
    for x in cursor:
        print(x)

def exec_sql(sql):
    try:
        cursor.execute(sql)
        return cursor
    except:
       print ("Exec Error!!", sql)

def json_to_array(raw, cols1):
    #print('json_to_array()')
    #print('raw:', raw)
    raw = json.loads(raw)
    data = [-1]*len(cols1)
    for i, col in enumerate(cols1):
        if col in raw.keys(): data[i] = raw[col]
    return data

def transform_to_df(results):
    cols0 = ['time', 'flag_working', 'flag_change_part', 'uuid', 'machine_num', 'floor', 'location']
    cols1 = ['x_pos', 'y_pos', 'z_pos', 'OPstate', 'F_actual', 'cycletime', 'feedratio', \
    'shop_name','workcount', 'RPM_actual', 'cuttingTime', 'poweronTime', 'spindle_load', \
    'spindle_temp', 'executionFlag', 'operatingTime', 'currenttoolnum', "tool_preset_life_01", \
    "tool_preset_life_02", "tool_preset_life_03", "tool_preset_life_04", "tool_preset_life_05", \
    "tool_preset_life_11", "tool_preset_life_12", "tool_preset_life_13", "tool_preset_life_14", \
    "tool_preset_life_15", "tool_current_life_01", "tool_current_life_02", "tool_current_life_03", \
    "tool_current_life_04", "tool_current_life_05", "tool_current_life_11", "tool_current_life_12", \
    "tool_current_life_13", "tool_current_life_14", "tool_current_life_15"]
    #print('results:', results)
    all_data = []
    for row in results:
        data = [row[0], row[2], row[3], row[4], row[5], row[6], row[7]] + json_to_array(row[1], cols1)
        all_data.append(data)
    df = pd.DataFrame(all_data, columns=cols0+cols1)

    df['tool_current_life'] = np.where(df.currenttoolnum == 1, df.tool_current_life_01,
                              np.where(df.currenttoolnum == 2, df.tool_current_life_02,
                              np.where(df.currenttoolnum == 3, df.tool_current_life_03,
                              np.where(df.currenttoolnum == 4, df.tool_current_life_04,
                              np.where(df.currenttoolnum == 5, df.tool_current_life_05, 0)))))
    df['tool_preset_life'] = np.where(df.currenttoolnum == 1, df.tool_preset_life_01,
                             np.where(df.currenttoolnum == 2, df.tool_preset_life_02,
                             np.where(df.currenttoolnum == 3, df.tool_preset_life_03,
                             np.where(df.currenttoolnum == 4, df.tool_preset_life_04,
                             np.where(df.currenttoolnum == 5, df.tool_preset_life_05, 0)))))
    return df


def loadData(location, floor, machine_num, toolnum):
    ### step 1: 先找出要到basic_analysis_data中撈資料的初始時間點：part_start_time
    condition0 = " where location='" + location + "' and floor='" + floor + "' and machine_num='" + machine_num + "' and toolnum='" + str(toolnum) + "' "
    sql = "select part_start_time from health_inference"+  condition0 + "order by part_start_time desc limit 1"
    cursor = exec_sql(sql)
    results = cursor.fetchall()
    if len(results) == 0:
        print('health_inference table of currenttoolnum is empty!')
        condition1 = " where location='" + location + "' and floor='" + floor + "' and machine_num='" + machine_num  + "' "
        sql = "select date_time from basic_analysis_data" + condition1 + "and flag_change_part='1' order by date_time desc limit 30"
        #sql = "select date_time from basic_analysis_data" + condition1 + "limit 3000"
        cursor = exec_sql(sql)
        results = cursor.fetchall()
        if len(results) < 2:#在basic_analysis_data中，只有少於2片的資料，先略過不計算
            return pd.DataFrame(),  np.nan
        part_start_time = results[-1][0]#以第一片的時間做part_start_time
    else:
        part_start_time = results[0]
    print('len of results:', len(results))
    print('part_start_time:', part_start_time)

    ### step 2: 再去basic_analysis_data撈出所有>part_start_time的資料，裡面會有>=3筆換刀的資料在內
    print('fetch data')
    condition1 = " where location='" + location + "' and floor='" + floor + "' and machine_num='" + machine_num  + "' "
    sql = "select * from basic_analysis_data" + condition1 + "and date_time > '" + str(part_start_time) + "' "
    cursor = exec_sql(sql) #在basic_analysis_data中，取出所有大於part_start_time的資料
    results = cursor.fetchall()
    raw_df = transform_to_df(results)
    return raw_df, part_start_time

def getTrainData(train, mean_load_len=30):
    k=5
    avg_mask = np.ones((k,))/k
    spindle_load_train = []
    tool_remain_life_train = []
    for machine in train.machine_num.unique():
        df_machine = train[(train.machine_num==machine)]
        for tool_group in df_machine.toolGroup.unique():
            df_machine_tool = df_machine[(df_machine.toolGroup==tool_group)]
            for i in range(len(df_machine_tool)-mean_load_len):
                if ((len(df_machine_tool)-i) < 200) & (i % 5 == 0):
                    spd_mean = df_machine_tool.iloc[i:i+mean_load_len]['spindle_load_mean'].values
                    spd_mean_avg = np.convolve(spd_mean, avg_mask, mode='valid')
                    spindle_load_train.append(spd_mean_avg)
                    tool_remain_life_train.append(df_machine_tool.iloc[i+mean_load_len]['tool_remain_life'])
                elif i % (mean_load_len/2) == 0:
                    spd_mean = df_machine_tool.iloc[i:i+mean_load_len]['spindle_load_mean'].values
                    spd_mean_avg = np.convolve(spd_mean, avg_mask, mode='valid')
                    spindle_load_train.append(spd_mean_avg)
                    tool_remain_life_train.append(df_machine_tool.iloc[i+mean_load_len]['tool_remain_life'])
    #print(len(spindle_load_train)) #534
    return spindle_load_train, tool_remain_life_train

def mmdTrain(spindle_load_train, tool_remain_life_train, spindle_load_test):
    start = time()
    min_list = []
    min_ten_list = []
    mmd_ten_list = []
    for i in range(len(spindle_load_test)): #1356
        x1 = spindle_load_test[i]
        mmd_result = []
        for j in range(len(spindle_load_train)):
            if i < 50:
                if tool_remain_life_train[j] > (50-i-15):
                    flag=True
                else:
                    flag=False
            else:
                flag=True
            if (tool_remain_life_train[j] < (1000 - i + 15)) & flag:
                print(i, ':', j)
                x2 = spindle_load_train[j]
                if np.ndim(x1)==1: x1 = np.reshape(x1, (-1,1))
                if np.ndim(x2)==1: x2 = np.reshape(x2, (-1,1))
                mmd_result.append(MMD_computing(x1, x2))
            else:
                mmd_result.append(np.inf)
        mmd_df = pd.DataFrame({'remain_life':tool_remain_life_train, 
                          'mmd':mmd_result})
        
        #---- threshold
        mmd_df['mmd'] = abs(mmd_df['mmd'])
        mmd_df['threshold'] = mmd_df['mmd'] < 0.0006
        mmd_df = mmd_df.loc[mmd_df['threshold']]
        if len(mmd_df)!=0:
            mmd_df.sort_values(by='mmd', inplace=True)
            min_list.append(mmd_df.remain_life.iloc[0])
            min_ten_list.append(mmd_df.remain_life.values)
            mmd_ten_list.append(mmd_df.mmd.iloc[:10].values)        
        else:
            min_list.append(0)
            min_ten_list.append(np.array([0,0]))
            mmd_ten_list.append(np.array([0,0]))                    
        '''
        #---- 10
        mmd_df['mmd'] = abs(mmd_df['mmd'])
        mmd_df.sort_values(by='mmd', inplace=True)
        min_list.append(mmd_df.remain_life.iloc[0])
        min_ten_list.append(mmd_df.remain_life.iloc[:10].values)
        mmd_ten_list.append(mmd_df.mmd.iloc[:10].values)
        '''
    print('time:', time() - start)
    return min_list, min_ten_list, mmd_ten_list

def sigma_computing(x1,x2):
    n, nfeatures = x1.shape
    m, mfeatures = x2.shape
    k1 = np.sum((x1*x1), 1)
    q = np.tile(k1, (m, 1)).transpose()
    del k1
    k2 = np.sum((x2*x2), 1)
    r = np.tile(k2, (n, 1))
    del k2
    h= q + r
    del q,r
    
    h = h-2*np.dot(x1,x2.transpose())
    h = np.array(h, dtype=float)
    mdist = np.median([i for i in h.flat if i])
    sigma = np.sqrt(mdist/2.0)
    if not sigma: sigma = 1
    return sigma

def MMD_computing(x1,x2):
    K11 = rbf_kernel(x1,x1,sigma_computing(x1,x1))
    K22 = rbf_kernel(x2,x2,sigma_computing(x2,x2))
    K12 = rbf_kernel(x1,x2,sigma_computing(x1,x2))
    
    m = K11.shape[0]
    n = K22.shape[0]
    t11 = (1./(m*(m-1)))*np.sum(K11 - np.diag(np.diagonal(K11)))
    t22 = (1./(n*(n-1)))* np.sum(K22 - np.diag(np.diagonal(K22)))
    t12 = (2./(m*n)) * np.sum(K12)
    MMD = t11 + t22 -t12
    return MMD

def inferenceData(train, raw_df, part_start_time, location, floor, machine_num, toolnum):
    raw_df = raw_df[raw_df.currenttoolnum==toolnum]
    raw_df = raw_df[raw_df.flag_working==1]
    if len(raw_df)==0:
        print('non working data!')
        return
    raw_df['part_no'] = raw_df['workcount'] + raw_df['tool_current_life'] * 0.0001 #### get unique part * 0.0001
    raw_df['changeTool'] =\
        (raw_df['tool_current_life'].diff() <0) & \
        (raw_df['tool_current_life']<60) & \
        ((raw_df['tool_current_life'].diff(periods=-1)+50>0) | np.isnan(raw_df['tool_current_life'].diff(periods=-1)))
    raw_df['toolGroup'] = raw_df['changeTool'].cumsum()
    train1 = train[train['currenttoolnum']==toolnum]
    spindle_load_train, tool_remain_life_train = getTrainData(train1)    
    row_list = []
    for part in raw_df.part_no.unique():
        # get last_mean_load from db
        #conn, cursor = connectDB()
        condition0 = " where location='" + location + "' and floor='" + floor + "' and machine_num='" + machine_num + "' and toolnum='" + str(toolnum) + "' "
        sql = "select tool_current_life from health_inference"+  condition0 + "and part_start_time > '" + str(part_start_time) +"' order by part_start_time desc limit 30"
        cursor = exec_sql(sql)
        last_mean_load = cursor.fetchall()
        if len(last_mean_load) < 30:    
            last_mean_load = [-1]*14
        
        # get last_tool_current_life from db
        #conn, cursor = connectDB()
        condition0 = " where location='" + location + "' and floor='" + floor + "' and machine_num='" + machine_num + "' and toolnum='" + str(toolnum) + "' "
        sql = "select tool_current_life from health_inference"+  condition0 + "order by part_start_time desc limit 1"
        cursor = exec_sql(sql)
        last_tool_current_life = cursor.fetchall()
        if len(last_tool_current_life) == 0:        
            last_tool_current_life = -1

        part_df = raw_df[raw_df.part_no==part]
        part_start_time = part_df['time'].iloc[0]
        part_end_time = part_df['time'].iloc[-1]
        part_start_time = pd.to_datetime(str(part_start_time))
        part_start_time = part_start_time.strftime('%Y-%m-%d %H:%M:%S')
        part_end_time = pd.to_datetime(str(part_end_time))
        part_end_time = part_end_time.strftime('%Y-%m-%d %H:%M:%S')
        mean_load = part_df['spindle_load'].mean()
        row1 = part_df.iloc[0]
    
        new_tool = (row1['tool_current_life'] - last_tool_current_life < 0) & \
        (row1['tool_current_life']<60)
        new_tool = int(np.where(new_tool, 1, 0))
        
        # inference
        if last_mean_load != [-1]*14:
            spindle_load_test = last_mean_load + [mean_load]
            k=5
            smooth_mask = np.ones((k,))/k
            spindle_load_test = [np.convolve(spindle_load_test, smooth_mask, mode='valid')]
            min_list, min_ten_list, mmd_ten_list = mmdTrain(spindle_load_train, tool_remain_life_train, spindle_load_test)
            predict_remain_life = np.mean(min_ten_list)
        else:
            predict_remain_life = -1
        if (predict_remain_life > 30) | (predict_remain_life == -1):
            alarm_condition = 0
        else:
            alarm_condition = 1
        row = [part_start_time, part_end_time, '', row1['location'], row1['floor'], row1['machine_num'],\
               row1['currenttoolnum'], row1['tool_preset_life'], row1['tool_current_life'], row1['workcount'], mean_load, new_tool, \
               predict_remain_life, alarm_condition, 1]
        row = [str(x) for x in row]
        row_list.append(tuple(row))
    
        # insert 1 row into db
        colname = ['part_start_time', 'part_end_time', 'uuid', 'location', 'floor', 'machine_num', \
                       'toolnum', 'tool_preset_life', 'tool_current_life', 'workcount', 'mean_load', 'new_tool', \
                       'predict_remain_life', 'alarm_condition', 'model_version']
        #colname = colname[:]
        cols = ",".join([str(i) for i in colname])
        val = row_list
        #val = [tuple(row[:]) for row in row_list]
        sql = "INSERT INTO health_inference ("+cols+") VALUES ("+ "%s,"*(len(val[0])-1) +"%s)"        
        cursor.executemany(sql, val)
        conn.commit()
        print(cursor.rowcount, "记录插入成功。")
        
###############################################################################################
conn, cursor = connectDB()
#cursor.execute('DROP TABLE health_inference')
colname = ['part_start_time', 'part_end_time', 'uuid', 'location', 'floor', 'machine_num', \
               'toolnum', 'tool_preset_life', 'tool_current_life', 'workcount', 'mean_load', 'new_tool', \
               'predict_remain_life', 'alarm_condition', 'model_version']
col_dtype = ['DATETIME(3)', 'DATETIME(3)', 'VARCHAR(25)', 'VARCHAR(15)', 'VARCHAR(15)', 'VARCHAR(5)', \
             'SMALLINT(6)', 'SMALLINT(6)', 'SMALLINT(6)', 'SMALLINT(6)', 'FLOAT', 'TINYINT(1)', \
             'FLOAT', 'TINYINT(1)', 'SMALLINT(6)']
createTable(cursor, 'health_inference', colname = colname, col_dtype=col_dtype)

train = pd.read_csv(csvDir + 'all_spindle_load_all.csv')
locations = ['GL']
floors = ['C04-1F']
#machine_nums = ['D09']
machine_nums = ['D08', 'D09', 'D10', 'D11', 'D12']
toolnums = [2, 3, 4, 5]
for location in locations:
    for floor in floors:
        for machine_num in machine_nums:
            for toolnum in toolnums:
                print('@@@ start ', location, floor, machine_num, toolnum,'@@@')
                raw_df, part_start_time = loadData(location, floor, machine_num, toolnum)
                if len(raw_df)!=0:
                    inferenceData(train, raw_df, part_start_time, location, floor, machine_num, toolnum)


'''
row_list = []
row = ('2019-10-27 07:19:31',
  '2019-10-27 07:22:37',
  '',
  'C04-1F',
  'line',
  'D09',
  2,
  900,
  296,
  16819,
  13.381443298969073,
  False,
  0.0,
  1,
  1)
row = row[:4]
row_list.append(tuple(row))
colname = ['part_start_time', 'part_end_time', 'uuid', 'location', 'floor', 'machine_num', \
               'toolnum', 'tool_preset_life', 'tool_current_life', 'workcount', 'mean_load', 'new_tool', \
               'predict_remain_life', 'alarm_condition', 'model_version']
colname = colname[:4]
cols = ",".join([str(i) for i in colname])
sql = "INSERT INTO health_inference ("+cols+") VALUES ("+ "%s,"*(len(row_list[0])-1) +"%s)"
val = row_list
cursor.executemany(sql, val)
conn.commit()
print(cursor.rowcount, "记录插入成功。")
'''



'''df_list = []
data1 = pd.read_csv('D:\\Sources\\Data\\Data-CAA_CNC\\data\\all_spindle_load2.csv')
data1.groupby(['machine_num', 'toolGroup'])['tool_current_life'].max()
data1['flagLife300'] = data1.groupby(['machine_num', 'toolGroup'])['tool_current_life'].transform(lambda x: max(x)>300)
data1 = data1[data1['flagLife300']]
train = data1[data1['machine_num']!='D09']
train = train[['shop_name', 'machine_num', 'currenttoolnum', 'toolGroup', 'spindle_load_mean', 'tool_remain_life']]
df_list.append(train)
df_final = pd.concat(df_list, axis=0)
df_final.to_csv('D:\\Sources\\Data\\Data-CAA_CNC\\data\\all_spindle_load_all.csv')
'''
#raw_df.to_csv('D:\\Sources\\Data\\Data-CAA_CNC\\data\\raw_df_test.csv')