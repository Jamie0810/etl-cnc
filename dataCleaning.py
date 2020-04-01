from lib import *

def dataCleaning(df):
    logging.info('*****Start to do Data Cleaning*****')
    try:
        col_list = [
            'currenttoolnum',
            'workcount',
            'OPstate',
            'z_pos',
            'spindle_load',
            'RPM_actual',
            'F_actual',
            'tool_current_life_01',
            'tool_current_life_02',
            'tool_current_life_03',
            'tool_current_life_04',
            'tool_current_life_05',
            'tool_preset_life_01',
            'tool_preset_life_02',
            'tool_preset_life_03',
            'tool_preset_life_04',
            'tool_preset_life_05',
        ]
        pd.options.mode.chained_assignment = None
        df = df[col_list] # keep col_list columns 
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
        df.drop(['tool_current_life_01','tool_current_life_02','tool_current_life_03','tool_current_life_04','tool_current_life_05',
                'tool_preset_life_01','tool_preset_life_02','tool_preset_life_03','tool_preset_life_04','tool_preset_life_05'], axis=1, inplace=True)
        df['part_no'] = df['workcount'] + df['tool_current_life'] * 0.001 #### get unique part

        preserve_index = []
        for tool_type in [2, 3, 4, 5]:
            #---- 取得特定刀型數據
            data = df[df.currenttoolnum == tool_type]
            if len(data) == 0:
                continue
            #---- 取得機殼數據
            data_part_list = []
            part_list = list(data['part_no'].unique()) 
            for part in part_list: 
                data_part_list.append(data.loc[data['part_no']==part])
            
            data_part_new_list = []
            preprocess_time_list = []
            preprocess_count_list = []
            for df_part in data_part_list:
                # --> 機殼的數據清理 
                df_part = df_part.dropna(axis=0, how='any')
                df_part = df_part[df_part['z_pos'] != 0]
                df_part = df_part[df_part['RPM_actual'] != 0]
                df_part = df_part[df_part['F_actual'] != 0]
                df_part = df_part[df_part['OPstate'] == 2]
            
                z_pos_series = df_part['z_pos'].value_counts()
                
                #機殼不是真的有加工的情況1
                if z_pos_series.shape[0] == 0:
                    continue
                
                #機殼不是真的有加工的情況2
                if np.sum(z_pos_series/df_part.shape[0] > 0.1) == 0:
                    continue
                
                # --> 機殼的數據清理, 只保留加工時長最久的Z平面, 再進行後續探討
                z_pos_freq_max = z_pos_series.index[0]                   
                df_part = df_part.loc[df_part['z_pos']==z_pos_freq_max]  
                
                rpm_series = df_part['RPM_actual'].value_counts()
                rpm_series = rpm_series/np.sum(rpm_series) > 0.2
                
                #機殼不是真的有加工的情況3
                if np.sum(rpm_series) == 0: 
                    continue
                
                # --> 機殼的數據清理, 只保留出現頻繁的rpm轉速的數據, 再進行後續探討
                rpm_series = rpm_series.index[np.where(rpm_series==True)].values
                df_part = df_part.loc[np.isin(df_part['RPM_actual'], rpm_series)]
                
                # --> 機殼的數據清理, 若起頭與結尾的主軸負載過高(>4std)則將被刪除, 再進行後續探討
                spindle_mean = np.mean(df_part['spindle_load'].values)
                spindle_std = np.std(df_part['spindle_load'].values)
                spindle_outlier_limit = spindle_mean + 4*spindle_std
                spindle_outlier_indexs = np.where(df_part['spindle_load'].values > spindle_outlier_limit)[0]
                spindle_outlier_indexs = [idx for idx in spindle_outlier_indexs if idx < (0.02*df_part['spindle_load'].size) or idx > (0.98*df_part['spindle_load'].size)]
                ok_index = list(range(df_part.shape[0]))
                for i in spindle_outlier_indexs:
                    ok_index.remove(i)
                df_part = df_part.iloc[ok_index]
                
                # --> 機殼的數據清理, 若在整段中的主軸負載過高(>10std)則將被刪除, 再進行後續探討
                spindle_mean = np.mean(df_part['spindle_load'].values)
                spindle_std = np.std(df_part['spindle_load'].values)
                spindle_outlier_limit = spindle_mean + 10*spindle_std
                df_part = df_part.loc[df_part['spindle_load']<spindle_outlier_limit]
                
                #機殼不是真的有加工的情況4
                if df_part.shape[0] == 0:
                    continue
                
                preprocess_time_list.append(df_part.index[-1] - df_part.index[0])
                preprocess_count_list.append(len(df_part))
                data_part_new_list.append(df_part)
        
        
            '''
            # --> 機殼的數據清理, 若加工時間過長則刪除機殼, 再進行後續探討
            preprocess_Q3, preprocess_Q1 = np.quantile(preprocess_time_list, [0.75, 0.25])
            preprocess_IQ = preprocess_Q3 - preprocess_Q1
            preprocess_not_outlier_time = (preprocess_time_list < preprocess_Q3 + 1.5*preprocess_IQ) & (preprocess_time_list > preprocess_Q1 - 1.5*preprocess_IQ)
            
            # --> 機殼的數據清理, 若加工紀錄資料過少則刪除機殼, 再進行後續探討
            preprocess_Q3, preprocess_Q1 = np.quantile(preprocess_count_list, [0.75, 0.25])
            preprocess_IQ = preprocess_Q3 - preprocess_Q1
            preprocess_not_outlier_count = (preprocess_count_list < preprocess_Q3 + 1.5*preprocess_IQ) & (preprocess_count_list > preprocess_Q1 - 1.5*preprocess_IQ)
            data_part_new_list = [d for a, b, d in zip(preprocess_not_outlier_time, preprocess_not_outlier_count, data_part_new_list) if (a and b)]
            '''
            
            for data_part in data_part_new_list:
                preserve_index.extend(data_part.index)
        df['flag_working'] = df.index.isin(preserve_index)
        df.reset_index(inplace=True)
        df = df.loc[:,['flag_working']]
        logging.info('*****Data Cleaning Finished*****')
        return df

    except:
        logging.error(traceback.format_exc())