import tejapi
import pandas
import numpy
tejapi.ApiConfig.api_key = "your_API_key"
class query_base(object):
    def __init__(self):
        self.tejapi = tejapi

    def set_apikey(self,api_key='yourkey'):
        self.tejapi.ApiConfig.api_key = api_key
        
    def set_listed_coid(self,df):
        listed_coids = self.basic_info.loc[self.basic_info['list_day1']<=self.current_zdate,'coid'].values.tolist()
        self.current_coids = df.loc[(df['zdate']==self.current_zdate),['zdate','coid']]
        self.listed_coids = df.loc[(df['zdate']==self.current_zdate)&(df['coid'].isin(listed_coids)),'coid'].values.tolist()    
    def get_activedate_data(self,window,column_names,peer_future=False,base_date=None,base_mdate=None,clue_length=None,keep='first'):
        current_data = None
        column_names = ['zdate','mdate','coid']+column_names
        df = self.all_date_data
        if self.all_date_data is None and self.prc_basedate is None:
            df = self.findata_all
        elif self.findata_all is None:
            df = self.prc_basedate
        if 'q' not in window:
            if clue_length is None:
                clue_length = 0
            if base_date is None:
                base_date = self.current_zdate
            elif base_date is not None:
                base_date =numpy.array([base_date]).astype('datetime64')[0]
            if 'd'  in window:
                this_window_type = 'D'
                window = int(window.replace('d',''))
            elif 'w'  in window:
                this_window_type = 'W'
                window = int(window.replace('w',''))
            elif 'm'  in window:
                this_window_type = 'M'
                window = int(window.replace('m',''))
            jump_length = window if peer_future is False else -1*window
            next_base_date = self.cal_zdate(base_date=base_date,jump_length=jump_length,jump_kind=this_window_type)
            if pandas.to_datetime(base_date).strftime('%Y-%m-%d')==next_base_date:
                current_data = df.loc[(df['zdate']<=base_date)&(df['zdate']>=next_base_date),column_names]
                current_data['temp_d'] = (base_date - current_data['zdate']) / numpy.timedelta64(1, 'D')
                current_data = current_data.sort_values(by=['coid','zdate']).reset_index(drop=True).drop(columns=['temp_d'])
            elif peer_future is False:
                current_data = df.loc[(df['zdate']<=base_date)&(df['zdate']>next_base_date),column_names]
                current_data['temp_d'] = (base_date - current_data['zdate']) / numpy.timedelta64(1, 'D')
                current_data = current_data.sort_values(by=['coid','zdate']).drop(columns=['temp_d'])
            else:
                current_data = df.loc[(df['zdate']>base_date)&(df['zdate']<=next_base_date),column_names]
                current_data['temp_d'] = (base_date - current_data['zdate']) / numpy.timedelta64(1, 'D')
                current_data = current_data.sort_values(by=['coid','temp_d']).drop(columns=['temp_d'])
        else:
            if clue_length is None:
                clue_length = 2
            window = int(window.replace('q',''))
            if base_mdate is not None:
                base_mdate =numpy.array([base_mdate]).astype('datetime64')[0]
            else:
                base_mdate_temp = pandas.to_datetime(self.current_mdate).strftime('%Y-%m-%d')
                base_mdate = numpy.array([base_mdate_temp]).astype('datetime64')[0]

            this_window_type = 'Q'
            #決定用來篩選的日期遮罩
            this_datefilter = pandas.DataFrame(self.all_mdate_list,columns=['mdate']).sort_values(by=['mdate'],ascending=False)
            if peer_future is False:
                this_datefilter = this_datefilter[this_datefilter['mdate']<=base_mdate]
                this_datefilter = this_datefilter.values[0:window+clue_length].flatten()
                up_date = this_datefilter[0]
                down_date = this_datefilter[len(this_datefilter)-1]
            else:
                this_datefilter = this_datefilter[this_datefilter['mdate']>=base_mdate]
                this_datefilter = this_datefilter.values[len(this_datefilter)-1-window:len(this_datefilter)].flatten()
                up_date = this_datefilter[0]
                down_date = this_datefilter[len(this_datefilter)-1]

            #用日期遮罩篩出檔案，因為是mdate為主資料，篩掉重複值
            current_data = df.loc[(df['mdate']<=up_date)&(df['mdate']>=down_date),column_names]
            
            current_data['temp_d'] = (base_mdate - current_data['mdate']) / numpy.timedelta64(1, 'D')
            if peer_future is False:
                current_data = current_data[(current_data['mdate']<=base_mdate)]
                current_data = current_data.sort_values(by=['coid','mdate']).drop_duplicates(subset=['coid','mdate'],keep='first').drop(columns=['temp_d'])
            else:
                current_data = current_data.sort_values(by=['coid','temp_d']).drop_duplicates(subset=['coid','mdate'],keep='first').drop(columns=['temp_d'])

        current_data = current_data.loc[current_data['coid'].isin(self.listed_coids),:].reset_index(drop=True)
        current_data[column_names] = current_data[column_names].replace([numpy.inf, -numpy.inf], numpy.nan)
        return current_data ,this_window_type, window