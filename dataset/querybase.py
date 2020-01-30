import tejapi
import pandas
import numpy

tejapi.ApiConfig.api_key = "your_API_key"



class query_base(object):
    def __init__(self):
        self.tejapi = tejapi

    def set_apikey(self,api_key='yourkey'):
        self.tejapi.ApiConfig.api_key = api_key
    def get_info(self):
        info = self.tejapi.ApiConfig.info()
        return info
    def set_data_attr(self):
        self.data_attr = {'datastart_date':str(self.datastart_date),'dataend_date':str(self.dataend_date)}
    def get_query_interval(self):
        dataend_date = numpy.datetime64(self.data_attr.get('dataend_date')) 
        datastart_date = numpy.datetime64(self.data_attr.get('datastart_date')) 
        job_list = []

        if self.dataend_date > dataend_date:
            #代表目前資料的迄日早於新的迄日，要補上次迄日到本次迄日間資料
            job_list = job_list +[{'mdate_up':self.dataend_date,'mdate_down':self.dataend_date}]
        if self.datastart_date < datastart_date:
            #代表目前資料的起日於新的迄日，要補本次起日到上次起日間資料
            job_list = job_list +[{'mdate_up':datastart_date,'mdate_down':self.dataend_date}]
        return job_list
    def query_data_with_date_coid(self,
            market='TWN',table_name='APRCD',query_coid=['2330'],
            mdate_up='2019-12-31',mdate_down='2018-12-31',
            query_columns=['coid','mdate'],rename_columns=None):
            
        dataset_name = market+'/'+table_name
        
        self.tempdata = None
        mdate_name = self.mdate_name_dict.get(table_name)
        if mdate_name is None:
            mdate_name='mdate'
        query_columns = list(set(query_columns + ['coid',mdate_name]))
        command_line = "self.tempdata=self.tejapi.get(dataset_name,coid=query_coid,"
        command_line+= mdate_name+"={'gte':mdate_down,'lte':mdate_up},"
        command_line+= "opts={'sort':'"+mdate_name+".desc','columns':query_columns}, paginate=True)"
        command_line+= ".rename(index=str, columns={'"+mdate_name+"':'zdate'})"
        context = {"self":self, "__name__": "__main__",
                   "dataset_name":dataset_name,"query_coid":query_coid,
                   "mdate_down":mdate_down,"mdate_up":mdate_up,
                   "query_columns":query_columns}
        exec(command_line, context)

        #data['zdate'] = pandas.to_datetime(data['zdate'].values,utc=True)
        self.tempdata['zdate'] = self.tempdata['zdate'].astype(str).astype('datetime64')
        if rename_columns is not None:
            print(rename_columns)
            self.tempdata = self.tempdata.rename(index=str, columns=rename_columns)

        return self.tempdata
    def get_table_cname(self,market='TWN',table_name='APRCD',language='cname'):
        dataset_name = market+'/'+table_name
        if self.table_info.get(dataset_name) is None:
            table_info = self.tejapi.table_info(dataset_name)
            table_info['columns_cname'] = [ 
                cols['cname'] for cols in table_info['columns'].values()]
            table_info['columns_name'] = [
                cols['name'] for cols in table_info['columns'].values()]
            self.table_info[dataset_name] = table_info
        return self.table_info[dataset_name]['name']

    def compare_column_name(self,market,table_name,query_columns,kind='cname'):
        table_cname = self.get_table_cname(market=market,table_name=table_name)
        dataset_name = market+'/'+table_name
        all_columns = self.table_info[dataset_name]['columns']
        ans_code = []
        ans_name = []
        for column in all_columns:
            if kind in ['cname','name']:
                name = all_columns.get(column)[kind]
            else:
                name = column
            if name in query_columns:
                ans_code += [column]
                ans_name += [name]
        left_name = numpy.setdiff1d(query_columns, ans_name).tolist()
        if len(left_name)>0:
            print('lack columns:')
            print(left_name)
        return ans_code,ans_name
    def get_column_name(self,market='TWN',table_name='APRCD',language='cname'):
        dataset_name = market+'/'+table_name
        table_cname = self.get_table_cname(market=market,table_name=table_name)
        table_info = self.table_info.get(dataset_name)
        return {'columns_cname':table_info['columns_cname'],
                'columns_name':table_info['columns_name']}
    def set_listed_coid(self,df):
        listed_coids = self.basic_info.loc[
                                       self.basic_info['list_day1']<=self.current_zdate,'coid'
                                       ].values.tolist()
        self.current_coids = df.loc[(df['zdate']==self.current_zdate),['zdate','coid']]
        self.listed_coids = df.loc[
            (df['zdate']==self.current_zdate)&(df['coid'].isin(listed_coids)),'coid'
            ].values.tolist()    
    def cal_zdate_by_window(self,window,base_date,peer_future=False,tradeday=True):
        if 'q' not in window:
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
            next_base_date = self.cal_zdate(
                                  base_date=base_date,jump_length=jump_length,
                                  jump_kind=this_window_type,tradeday=tradeday)
        next_base_date = numpy.array([next_base_date]).astype('datetime64')[0]

        return this_window_type,next_base_date
    def get_activedate_data(self,
            window,column_names,peer_future=False,
            base_date=None,base_mdate=None
            ,clue_length=None,keep='first'):
            
        current_data = None
        column_names = ['zdate','mdate','coid']+column_names
        df = self.all_date_data
        if self.all_date_data is None:
            df = self.prc_basedate
            
        if 'q' not in window:
            if clue_length is None:
                clue_length = 0
            if base_date is None:
                base_date = self.current_zdate
            elif base_date is not None:
                base_date =numpy.array([base_date]).astype('datetime64')[0]

            this_window_type,next_base_date = self.cal_zdate_by_window(
                                                   window=window,base_date=base_date,
                                                   peer_future=peer_future)

            if base_date==next_base_date:
                current_data = df.loc[(df['zdate']==next_base_date),column_names]
            elif peer_future is False:
                current_data = df.loc[(df['zdate']<=base_date)&(df['zdate']>next_base_date),column_names]
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
            this_datefilter = pandas.DataFrame(
                                     self.all_mdate_list,columns=['mdate']
                                     ).sort_values(by=['mdate'],ascending=False)
            if peer_future is False:
                this_datefilter = this_datefilter[this_datefilter['mdate']<=base_mdate]
                this_datefilter = this_datefilter.values[0:window+clue_length].flatten()
                up_date = this_datefilter[0]
                down_date = this_datefilter[len(this_datefilter)-1]
            else:
                this_datefilter = this_datefilter[this_datefilter['mdate']>=base_mdate]
                this_datefilter = this_datefilter.values[
                                                  len(this_datefilter)-1-window:len(this_datefilter)
                                                  ].flatten()
                up_date = this_datefilter[0]
                down_date = this_datefilter[len(this_datefilter)-1]

            #用日期遮罩篩出檔案，因為是mdate為主資料，篩掉重複值
            current_data = df.loc[(df['mdate']<=up_date)&(df['mdate']>=down_date),column_names]
            
            current_data['temp_d'] = (base_mdate - current_data['mdate']) / numpy.timedelta64(1, 'D')
            if peer_future is False:
                current_data = current_data[(current_data['mdate']<=base_mdate)]
                current_data = current_data.sort_values(by=['coid','mdate']
                                            ).drop_duplicates(subset=['coid','mdate'],keep='first'
                                            ).drop(columns=['temp_d'])
            else:
                current_data = current_data.sort_values(by=['coid','temp_d']
                                           ).drop_duplicates(subset=['coid','mdate'],keep='first'
                                           ).drop(columns=['temp_d'])
        
        current_data = current_data.loc[
                                    current_data['coid'].isin(self.listed_coids),
                                    current_data.columns].reset_index(drop=True)
        current_data[column_names] = current_data[column_names].replace(
                                                                [numpy.inf, -numpy.inf], numpy.nan)
        return current_data ,this_window_type, window