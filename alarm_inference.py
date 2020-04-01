from lib import *
import numpy as np
import pandas as pd
import matplotlib.pylab as plt
import mysql.connector
import json
import time
import datetime
from datetime import timedelta

def json_to_array(raw, cols1):
    raw = json.loads(raw)
    data = [-1]*len(cols1)
    for i, col in enumerate(cols1):
        if col in raw.keys(): data[i] = raw[col]
    return data

def transform_to_df(results):
    print('transform_to_df()')
    cols0 = ['time', 'flag_change_part', 'flag_working', 'machine_num', 'floor', 'location']
    cols1 = ['x_pos', 'y_pos', 'z_pos', 'OPstate', 'F_actual', 'cycletime', 'feedratio', \
    'shop_name','workcount', 'RPM_actual', 'cuttingTime', 'poweronTime', 'spindle_load', \
    'spindle_temp', 'executionFlag', 'operatingTime', 'currenttoolnum', "tool_preset_life_01", \
    "tool_preset_life_02", "tool_preset_life_03", "tool_preset_life_04", "tool_preset_life_05", \
    "tool_preset_life_11", "tool_preset_life_12", "tool_preset_life_13", "tool_preset_life_14", \
    "tool_preset_life_15", "tool_current_life_01", "tool_current_life_02", "tool_current_life_03", \
    "tool_current_life_04", "tool_current_life_05", "tool_current_life_11", "tool_current_life_12", \
    "tool_current_life_13", "tool_current_life_14", "tool_current_life_15"]

    all_data = []
    for row in results:
        data = [row[0], row[2], row[3], row[4], row[5], row[6]] + json_to_array(row[1], cols1)
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

def init_connect():
    db = mysql.connector.connect(
      host=host,
      user=user,
      passwd=passwd,
      database=database
    )
    cursor = db.cursor()
    return db, cursor

def exec_sql(sql):
    try:
        cursor.execute(sql)
        return cursor
    except:
       print ("Exec Error!!", sql)

def add_change_tool(x, start_new_tool=0):
    tmpdf = x.groupby(by=['workcount', 'tool_current_life']).count()
    tmpdf = tmpdf[tmpdf['time']>3]#每一對workcount,tool_current_life中要含有至少3個資料點
    pairlist = tmpdf.index.to_list()
    g = start_new_tool
    wclife_map = {}
    for i in range(1,len(pairlist)-1):
        wc = pairlist[i][0]
        life = pairlist[i][1]
        life0 = pairlist[i-1][1]
        life1 = pairlist[i+1][1]

        if (life-life0<0) and (life<35) and (life1-life<50):
            g += 1
        wclife_map[str(wc)+'_'+str(life)] = g

    wclife_map[str(pairlist[0][0])+'_'+str(pairlist[0][1])] = -1
    wclife_map[str(pairlist[len(pairlist)-1][0])+'_'+str(pairlist[len(pairlist)-1][1])] = -1
    x['new_tool'] = -1
    x['wc_life'] = x['workcount'].map(str)+'_'+x['tool_current_life'].map(str)
    x['new_tool'] = x['wc_life'].map(wclife_map)
    x = x[x.new_tool>-1]
    return x

def get_part_df(df):#取出每片的資訊
    print('get_part_df()')
    tmp = []
    for new_tool in df.new_tool.unique():#選第幾段
        df2 = df[df.new_tool==new_tool]
        min_wc = df2.workcount.min()
        for wc in df2.workcount.unique():#選第幾片
            df3 = df2[df2.workcount==wc]
            df3 = df3[df3.z_pos<0]#因為改成是取top 3工時長的z平面，有時會取到z>=0的
            if len(df3)==0:continue
            load = df3.spindle_load.values
            #load = outlier_remove(load)
            mean_load = np.mean(load)
            std_load = np.std(load)
            part_start_time = df3.time.values[0]
            part_end_time = df3.time.values[-1]

            tool_preset_life = df3.tool_preset_life.values[0]
            tool_current_life = df3.tool_current_life.values[0]
            if mean_load<1.5 or mean_load>15 or std_load==0: continue #特殊情形就忽略不考慮


            tmp.append([new_tool, part_start_time, part_end_time, wc-min_wc, wc, mean_load, tool_preset_life, tool_current_life])#注意：此處的life=wc-min_wc!!!
    df = pd.DataFrame(tmp, columns=['new_tool', 'part_start_time', 'part_end_time', 'life', 'workcount', 'mean_load', 'tool_preset_life', 'tool_current_life'])

    return df

def calc_health(loads, lifes, presets):
    healthes = []
    for load, life, preset in zip(loads, lifes, presets):
        b = 0
        if life >= preset: b = 1
        else: b = (preset - life) / preset

        if b < 0.33: b=0.33
        health = -np.log(load)*b/2
        healthes.append(health)
    return healthes

def simulate_health(df):
    df['adj_load'] = 0
    df['ewm_load'] = 0
    df['health_value'] = 0
    baseline = np.min(df.mean_load.values[:7])
    df['adj_load'] = df.mean_load.values-baseline+0.000001
    df['ewm_load'] = df[['adj_load']].ewm(alpha=0.1).mean()
    print('df.mean_load', df.mean_load.values)
    print('df.adj_load', df.adj_load.values)
    print('df.ewm_load', df.ewm_load.values)
    print('df.preset_life', df.tool_preset_life.values)
    print('df.current_life', df.tool_current_life.values)

    df['health_value'] = calc_health(df.ewm_load.values, df.tool_current_life.values, df.tool_preset_life.values)
    print('df.health_value', df.health_value.values)

    return df


def analyse_data(location, floor, machine_num, toolnum):

    ### step 1: 先找出要到basic_analysis_data中撈資料的初始時間點：part_start_time
    result = []


    #step 1.1: 到part_prediction中去找最近的一筆的part_start_time

    print('setp 1. find part_start_time')
    condition0 = " where location='" + location + "' and floor='" + floor + "' and machine_num='" + machine_num + "' and toolnum='" + str(toolnum) + "' "
    condition1 = " where location='" + location + "' and floor='" + floor + "' and machine_num='" + machine_num  + "' "

    sql = "select part_start_time, new_tool from part_prediction"+  condition0 + "order by part_start_time DESC limit 1"
    cursor = exec_sql(sql)
    results = cursor.fetchall()
    previous_new_tool = -1

    if len(results) == 1:#如果可以找到part_start_time
        part_start_time = results[0][0]
        previous_new_tool = results[0][1]
        sql = "select date_time from basic_analysis_data" + condition1 + "and date_time > '" + str(part_start_time) + "' and flag_change_part='1' order by date_time"###改成不限制3
        cursor = exec_sql(sql)
        results = cursor.fetchall()
        if len(results)<3:#在basic_analysis_data中，只有少於三片的資料，先略過不計算
            return
    else:
    #step 1.2:若是找不到，就去basic_analysis_data中找所有有換刀紀錄的時間點
        sql = "select date_time from basic_analysis_data" + condition1 + "and flag_change_part='1' order by date_time"
        cursor = exec_sql(sql)
        results = cursor.fetchall()
        if len(results)<3:#在basic_analysis_data中，只有少於三片的資料，先略過不計算
            return
        part_start_time = results[-20][0]#以第一片的時間做part_start_time
    print('part_start_time:', part_start_time)


    ### step 2: 再去basic_analysis_data撈出所有>part_start_time的資料，裡面會有>=3筆換刀的資料在內

    print('step 2. retive all data > part_start_time')
    sql = "select * from basic_analysis_data" + condition1 + "and date_time > '" + str(part_start_time) + "' "
    cursor = exec_sql(sql)#在basic_analysis_data中，取出所有大於part_start_time的資料
    results = cursor.fetchall()
    raw_df = transform_to_df(results)
    raw_df = raw_df[raw_df.currenttoolnum==toolnum]

    ### step 3: 增加換刀紀錄new_tool，並取出第二片到倒數第二片的資料依序做分析

    print('setp 3. Add new_tool column')
    if previous_new_tool > -1:#表示已經有之前預測的結果
        dfs = add_change_tool(raw_df, previous_new_tool)#增加換刀紀錄new_tool
    else:
        dfs = add_change_tool(raw_df, 0)#增加換刀紀錄new_tool

    dfs = get_part_df(dfs)#切出一片一片的資訊
    print('dfs', dfs)


    ### step 4. retrive 2nd to -2nd work to analyze

    print('step 4. retrive 2nd to -2nd work to analyze')

    lifes = sorted(dfs.life.unique())[1:-1]#取出要分析的第二片到倒數第二片的life，注意：此數的life=wc-min_wc
    print('lifes:', lifes)

    for life in lifes:
        df = dfs[dfs.life==life]#取出要分析的片
        if len(df)==0: continue
        part_start_time = df.part_start_time.values[0]
        part_end_time = df.part_end_time.values[0]
        part_start_time = pd.to_datetime(str(part_start_time))
        part_start_time = part_start_time.strftime('%Y-%m-%d %H:%M:%S')
        part_end_time = pd.to_datetime(str(part_end_time))
        part_end_time = part_end_time.strftime('%Y-%m-%d %H:%M:%S')
        workcount = df.workcount.values[0]
        tool_preset_life = df.tool_preset_life.values[0]
        tool_current_life = df.tool_current_life.values[0]
        mean_load = df.mean_load.values[0]
        new_tool = df.new_tool.values[0]

        if previous_new_tool == -1:#表示part_prediction裡面是空的
            health_value = -1
            health_condition = -1

        else: #表示part_prediction裡面不是空的,
            if new_tool != previous_new_tool:#表示有換刀
                health_value = -1
                health_condition = -1

            else:#若沒有換刀,表示有load的baseline可參考
                sql = "select * from part_prediction" + condition0 + "and new_tool='" + str(new_tool) + "' "
                cursor = exec_sql(sql)
                results = cursor.fetchall()
                cols = ['part_start_time', 'part_end_time', 'location', 'floor', 'machine_num', 'toolnum', 'tool_preset_life', 'tool_current_life', \
                'workcount', 'mean_load', 'new_tool', 'health_value', 'health_condition']
                raw_df1 = pd.DataFrame(results,columns=cols)
                raw_df1 = raw_df1.append({'part_start_time' : part_start_time , 'part_end_time' : part_end_time, \
                'location': location, 'floot': floor, 'machine_num': machine_num, 'toolnum': toolnum,\
                'tool_preset_life': tool_preset_life, 'tool_current_life': tool_current_life, 'workcount': workcount,\
                'mean_load': mean_load, 'new_tool':new_tool, 'health_value':-1, 'health_condition':-1
                } , ignore_index=True)
                result_df = simulate_health(raw_df1)
                health_value = result_df.health_value.values[-1]
                if health_value>0: health_condition = 1
                else: health_condition = 0


        previous_new_tool = new_tool
        sql = "INSERT INTO part_prediction VALUES ("

        val = "'"+str(part_start_time)+"','"+str(part_end_time)+"','"+str(location)+"','"+str(floor)+"','"+str(machine_num)+"','"+str(toolnum)+"','"+\
        str(tool_preset_life)+"','"+str(tool_current_life)+"','"+str(workcount)+"','"+str(mean_load)+"','"+str(new_tool)+"','"+str(health_value)+"','"+str(health_condition)
        sql += val + "')"

        cursor = exec_sql(sql)
        db.commit()
        model_version = 1
        sql = "INSERT INTO tool_alarm VALUES ("

        val = "'"+str(part_start_time)+"','"+str(part_end_time)+"','"+str(toolnum)+"','"+str(health_value)+"','"+str(model_version)+"','"+\
        str(machine_num)+"','"+str(floor)+"','"+str(location)+"','"+str(tool_preset_life)+"','"+str(tool_current_life)
        sql += val + "')"

        cursor = exec_sql(sql)
        db.commit()

        result.append([location, floor ,'', machine_num, toolnum, part_start_time, part_end_time, tool_preset_life, tool_current_life, health_value])
    result = pd.DataFrame(result, columns=['location', 'floor', 'line', 'machine_num', 'toolnum', 'part_start_time', 'part_end_time', 'tool_preset_life', 'tool_current_life', 'health_value'])
    return result

db, cursor = init_connect()

sql = "CREATE TABLE IF NOT EXISTS `part_prediction` (\
   `part_start_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
   `part_end_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
   `location` VARCHAR(255) NOT NULL,\
   `floor` VARCHAR(255) NOT NULL,\
   `machine_num` VARCHAR(255) NOT NULL,\
   `toolnum` TINYINT NOT NULL,\
   `tool_preset_life` SMALLINT NOT NULL,\
   `tool_current_life` SMALLINT NOT NULL,\
   `workcount` SMALLINT NOT NULL,\
   `mean_load` FLOAT NOT NULL,\
   `new_tool` SMALLINT NOT NULL,\
   `health_value` FLOAT NOT NULL,\
   `health_condition` TINYINT NOT NULL\
   ) CHARSET=utf8;"
exec_sql(sql)
db.commit()

sql = "CREATE TABLE IF NOT EXISTS `tool_alarm` (\
   `part_start_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
   `part_end_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
   `toolnum` TINYINT NOT NULL,\
   `value` FLOAT NOT NULL,\
   `model_version` TINYINT NOT NULL,\
   `machine_num` VARCHAR(255) NOT NULL,\
   `floor` VARCHAR(255) NOT NULL,\
   `location` VARCHAR(255) NOT NULL,\
   `tool_preset_life` SMALLINT NOT NULL,\
   `current_tool_life` SMALLINT NOT NULL\
   ) CHARSET=utf8;"
exec_sql(sql)
db.commit()

locations = ['GL']
floors = ['C04-1F']
machine_nums = ['D09', 'D10', 'D11', 'D12']
toolnums = [2,3,4,5]

###請Ｊamie呼叫時附帶location/floor/machine_nums等資訊

output = []
for location in locations:
    for floor in floors:
        for machine_num in machine_nums:
            for toolnum in toolnums:
                print('---Analysing ', location, floor, machine_num, toolnum,'---')
                output.append(analyse_data(location, floor, machine_num, toolnum))

db.close()

output = pd.concat(output, axis=0).reset_index(drop=True)
if len(output)>0:
    filename = 'Health-'+output.iloc[0]['part_start_time']+'.csv'
    output.to_csv(filename)

'''
def outlier_remove(load):
    # --> 機殼的數據清理, 若起頭與結尾的主軸負載過高(>4std)則將被刪除, 再進行後續探討
            spindle_mean = np.mean(load)
            spindle_std = np.std(load)
            spindle_outlier_limit = spindle_mean + 4*spindle_std

            spindle_outlier_indexs = np.where(load > spindle_outlier_limit)[0]

            spindle_outlier_indexs = [idx for idx in spindle_outlier_indexs if idx < (0.02*len(load)) or idx > (0.98*len(load))]
            ok_index = [i for i in range(len(load)) if i not in spindle_outlier_indexs]
            load = load[ok_index]

            # --> 機殼的數據清理, 若在整段中的主軸負載過高(>5std)則將被刪除, 再進行後續探討
            spindle_mean = np.mean(load)
            spindle_std = np.std(load)
            spindle_outlier_limit = spindle_mean + 5*spindle_std
            spindle_outlier_indexs = np.where(load > spindle_outlier_limit)[0]

            ok_index = [i for i in range(len(load)) if i not in spindle_outlier_indexs]
            load = load[ok_index]

            # --> 機殼的數據清理, 若在整段中的主軸負載過高(>20)則將被刪除, 再進行後續探討
            ok_index = np.where(load<=20)[0]
            load = load[ok_index]

            return load
'''
