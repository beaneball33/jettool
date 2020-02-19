from . import dbapi
import tejapi
import pandas
import numpy

tejapi.ApiConfig.api_key = "your_API_key"

class query_base(dbapi.db_attr):
    def __init__(self):
        self.tejapi = tejapi
        self.tablelist = tablelist
    def set_apikey(self,api_key='yourkey'):
        # 使用者設定api key之後的各種工作
        self.tejapi.ApiConfig.api_key = api_key
        self.info = self.get_info()
        self.set_tablelist(list(self.info.get('user').get('tables').keys()))
        self.get_market()
        self.get_category()
        self.get_tables()
        # 標準化日資料(有zdate，不需轉置)的查詢工具)，給定欄位名稱就可以查詢
        self.set_query_ordinal()        
        
    def get_info(self):
        #取得使用者api key資訊
        info = self.tejapi.ApiConfig.info()
        print_info = [
                      '使用者名稱：'+str(info.get('user').get('name'))+'('+str(info.get('user').get('shortName'))+')',
                      '使用權限日期：'+str(info.get('user').get('subscritionStartDate'))+'/'+str(info.get('user').get('subscritionEndDate')),
                      '日連線次數狀態：'+str(info.get('todayReqCount'))+'/'+str(info.get('reqDayLimit')),
                      '日查詢資料量狀態：'+str(info.get('todayRows'))+'/'+str(info.get('rowsDayLimit')),
                      '月查 詢資料量狀態：'+str(info.get('monthRows'))+'/'+str(info.get('rowsMonthLimit')),
                      ]
        
        print(print_info)
        return info        
        
    def set_market(self,market):
        #設定使用者要查詢的市場
        self.market = market

    def set_data_attr(self):
        # 設定查詢工具對於資料的各種參數設定，用來製造暫存檔用
        self.data_attr = {'datastart_date':str(self.datastart_date),
                          'dataend_date':str(self.dataend_date)}
    def get_query_interval(self):
        # 用來產生不重複查詢區間
        dataend_date = numpy.datetime64(self.data_attr.get('dataend_date')) 
        datastart_date = numpy.datetime64(self.data_attr.get('datastart_date'))
        job_list = []

        if self.dataend_date > dataend_date:
            #代表目前資料的迄日早於新的迄日，要補上次迄日到本次迄日間資料
            job_list.append({'mdate_up':self.dataend_date,
                                   'mdate_down':dataend_date})

        if self.datastart_date < datastart_date:
            #代表目前資料的起日於新的迄日，要補本次起日到上次起日間資料
            job_list.append({'mdate_up':datastart_date,
                             'mdate_down':self.datastart_date})
        return job_list
    def get_dataset_name(self,market,table_name):
        # 產生資料表全名
        return '{}/{}'.format(market,table_name)
        
    def query_data_with_date_coid(self,
            market='TWN',table_name='APRCD',query_coid=['2330'],
            mdate_up='2019-12-31',mdate_down='2018-12-31',mdate_name='mdate',
            query_columns=['coid','mdate'],rename_columns=None):
        # 根據給定資料表名稱與條件，動態產生查詢式
        
        dataset_name = self.get_dataset_name(market,table_name)
        
        self.tempdata = None
        
        query_columns = list(set(query_columns + ['coid',mdate_name]))

        """
        command_line = "self.tempdata=self.tejapi.get(dataset_name,coid=query_coid,"
        command_line+= mdate_name+"={'gte':mdate_down,'lte':mdate_up},"
        command_line+= "opts={'sort':'"+mdate_name+".desc','columns':query_columns}, paginate=True)"
        command_line+= ".rename(index=str, columns={'"+mdate_name+"':'zdate'})"
        """
        command_line = ["self.tempdata=self.tejapi.get('",
                        dataset_name+"',coid=query_coid,",
                        mdate_name+"={'gte':mdate_down,'lte':mdate_up},",
                        "opts={'sort':'"+mdate_name+".desc',",
                        "'columns':query_columns}, paginate=True)",
                        ".rename(index=str, columns={'"+mdate_name+"':'zdate'})"]
        
        
        context = {"query_coid":query_coid,
                   "mdate_down":mdate_down,"mdate_up":mdate_up,
                   "query_columns":query_columns}
                   
        
        self.exec_tool(context,command_line)

        #data['zdate'] = pandas.to_datetime(data['zdate'].values,utc=True)
        self.tempdata['zdate'] = self.tempdata['zdate'].astype(str).astype('datetime64')
        if rename_columns is not None:

            self.tempdata = self.tempdata.rename(index=str, columns=rename_columns)

        return self.tempdata
    def exec_tool(self,context,command_line):
        context['self'] = self
        context['__name__'] = '__main__'
        command_line_str = ''.join(command_line)

        exec(command_line_str, context)
    def get_table_cname(self,market='TWN',table_name='APRCD',language='cname'):
        # 取得table名稱並同時透過api查詢該table的資訊
        dataset_name = self.get_dataset_name(market,table_name)
        
        if self.table_info.get(dataset_name) is None:
            try:
                table_info = self.tejapi.table_info(dataset_name)
            except (RuntimeError, TypeError, NameError):
                # 代表不是有資料而是對照表，略過
                return None
            table_info['columns_cname'] = [ 
                cols['cname'] for cols in table_info['columns'].values()]
            table_info['columns_name'] = [
                cols['name'] for cols in table_info['columns'].values()]
            self.table_info[dataset_name] = table_info
        return self.table_info[dataset_name]['name']

    def compare_column_name(self,market,table_name,query_columns,kind='cname'):
        # 比較指定表單中是否存在某個欄位名稱
    
        dataset_name = self.get_dataset_name(market,table_name)
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
        # 取得欄位的中文名稱，以便用來把欄位實體名稱改為中文
        dataset_name = self.get_dataset_name(market,table_name)
        
        table_cname = self.get_table_cname(market=market,table_name=table_name)
        table_info = self.table_info.get(dataset_name)

        return {'columns_cname':table_info['columns_cname'],
                'columns_name':table_info['columns_name']}
    def set_listed_coid(self,df):
        # 透過basic_info中的上市股票名單取得coid
        
        listed_coids = self.basic_info.loc[
                                       self.basic_info['list_day1']<=self.current_zdate,
                                       'coid'].values.tolist()
        self.current_coids = df.loc[(df['zdate']==self.current_zdate),['zdate','coid']]
        self.listed_coids = df.loc[
            (df['zdate']==self.current_zdate)&(df['coid'].isin(listed_coids)),'coid'
            ].values.tolist()    
    def cal_zdate_by_window(self,window,base_date,peer_future=False,tradeday=True):
        # 計算指定移動窗口以前的zdate
    
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

        return this_window_type,next_base_date,window
    def get_zdate(self,base_date):

        base_date = numpy.datetime64(base_date) 
        last_zdate = numpy.datetime64(base_date) 
        for zdate in self.all_zdate_list:
            if zdate < base_date:
                print(zdate)
                break
            last_zdate = zdate
            
        return [zdate,last_zdate]
    def get_activedate_data(self,
            window,column_names,peer_future=False,
            base_date=None,base_mdate=None
            ,clue_length=None,keep='first'):
        # 根據window取得某些欄位資料
        
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

            this_window_type,next_base_date,window = self.cal_zdate_by_window(
                                                   window=window,base_date=base_date,
                                                   peer_future=peer_future)

            if base_date==next_base_date:
                current_data = df.loc[(df['zdate']==next_base_date),column_names]
            elif peer_future is False:
                current_data = df.loc[(df['zdate']<=base_date)&(df['zdate']>next_base_date),column_names]
            else:
                current_data = df.loc[(df['zdate']>base_date)&(df['zdate']<=next_base_date),column_names]
            current_data['temp_d'] = (base_date - current_data['zdate']) / numpy.timedelta64(1, 'D')
            current_data = current_data.sort_values(by=['coid','temp_d'],ascending=False).drop(columns=['temp_d'])

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
                current_data = current_data.sort_values(by=['coid','mdate'],ascending=False                                            
                                            ).drop_duplicates(subset=['coid','mdate'],keep='first'
                                            ).reset_index(drop=True
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
    def query_tradedata(self,mkts=['TSE','OTC'],prc_name=[]):
    

        print('查詢日資料 最大資料日期:'+str(self.dataend_date))
        #產生標準交易日期資料
        self.partquery_prc_basedate = self.create_prc_base()
        self.fullquery_prc_basedate = self.partquery_prc_basedate.reindex(columns=['coid','zdate']).copy()
        self.append_list = []
        

        self.part_query_interval = self.get_query_interval()
        self.full_query_interval = [{'mdate_up':self.dataend_date,'mdate_down':self.datastart_date}]
        
        #for table_name in self.all_prc_dataset:
        for table_name in prc_name:
            if table_name in self.all_prc_dataset is None:
                print("沒有存取權限："+table_name)
                continue

            mdate_name = 'mdate' 
            if self.mdate_name_dict.get(table_name) is not None:
                mdate_name = self.mdate_name_dict.get(table_name).get('mdate')
            job_list = self.part_query_interval
            temp_data = None
            full_query = False

            

            available_cname = prc_name.get(table_name)

            if self.prc_basedate is not None:
                for col_name in available_cname:
                    #未在原資料集內的名稱，整個重查
                    if col_name  not in self.prc_basedate.columns.tolist():
                        full_query =  True           
                        job_list = self.full_query_interval
                        self.append_list = self.append_list + [col_name]
            else:
                full_query = True
            if available_cname is not None:
                #在此table尋找可用的欄位
                available_code,available_cname = self.compare_column_name(market=self.market,
                                                                          table_name=table_name,
                                                                          query_columns=available_cname)

                rename_set = { available_code[i]:available_cname[i] 
                                   for i in range(0,len(available_code))}      
                table_cname = self.get_table_cname(market=self.market,table_name=table_name)       
                print(table_cname+' 重新查詢:'+str(full_query))    
                for data_interval in job_list:
                    mdate_up = data_interval['mdate_up']     
                    mdate_down=data_interval['mdate_down']                    

                    
                    if full_query is False:
                        self.append_list = self.append_list + available_cname                    
                    queried_data = self.query_data_with_date_coid(market=self.market,
                                                                  table_name=table_name,
                                                                  query_coid=self.input_coids,
                                                                  mdate_up=mdate_up,
                                                                  mdate_down=mdate_down,
                                                                  mdate_name=mdate_name,
                                                                  query_columns=available_code,
                                                                  rename_columns=rename_set)
                    
                                
                    if len(queried_data)>0:

                        if temp_data is None:
                            temp_data = queried_data
                        else:
                            temp_data = temp_data.append(queried_data,sort=False)

            #各日期查詢完畢 開始組裝            
            if temp_data is not None:
                if full_query is True:
                    self.fullquery_prc_basedate = self.fullquery_prc_basedate.merge(temp_data,on=['zdate','coid'],how='left')
                else:
                    self.partquery_prc_basedate = self.partquery_prc_basedate.merge(temp_data,on=['zdate','coid'],how='left')
 
        if self.prc_basedate is None:
            self.prc_basedate = self.fullquery_prc_basedate
        else:
            #先進行刪除，再append，再進行merge
            append_columns = ['zdate','coid'] + self.append_list
            self.prc_basedate = self.prc_basedate.reindex(columns=append_columns)
            #要分段append，避免重複
            for data_interval in self.part_query_interval:
                temp_data = self.partquery_prc_basedate.loc[(self.partquery_prc_basedate['zdate']<data_interval['mdate_up'])&
                                            (self.partquery_prc_basedate['zdate']>=data_interval['mdate_down']),
                                            append_columns]
                self.prc_basedate = self.prc_basedate.append(temp_data,sort=False)
            self.prc_basedate = self.prc_basedate.drop_duplicates(subset=['coid','zdate'],keep='last')
            self.prc_basedate = self.prc_basedate.sort_values(by=['coid','zdate'],
                                                              ascending=True).reset_index(drop=True)

            self.prc_basedate = self.fullquery_prc_basedate.merge(self.prc_basedate,
                                                        on=['zdate','coid'],how='left')
            self.prc_basedate = self.prc_basedate.sort_values(by=['coid','zdate'], ascending=True).reset_index(drop=True)
    def create_prc_base(self,query_coids=None,benchmark=False):
        # 透過績效指標的交易日用來產生有考慮上市日的coid+zdate集合，藉此校正資料
        
        prc_basedate = None
        if query_coids is None:
            query_coids = self.input_coids
        for query_coid in query_coids:
            list_day1 = self.basic_info.loc[self.basic_info['coid']==query_coid,'list_day1'].values[0]

            this_prc_basedate = self.benchmark_roi[(self.benchmark_roi['zdate']>=list_day1)].copy()
            if int(self.basic_info.loc[self.basic_info['coid']==query_coid,'list_day2'].isnull().values[0])==0:
                list_day2 = self.basic_info.loc[self.basic_info['coid']==query_coid,'list_day2'].values[0]       
                this_prc_basedate = self.benchmark_roi[(self.benchmark_roi['zdate']<=list_day2)].copy()                
            #要補上代碼，否則仍是空值
            this_prc_basedate['coid'] = query_coid
            if prc_basedate is None:
                prc_basedate = this_prc_basedate
            else:
                prc_basedate = prc_basedate.append(this_prc_basedate,sort=False)
        if benchmark is False:
            prc_basedate = prc_basedate.reindex(columns=['coid','zdate'])
        return prc_basedate.sort_values(by=['coid','zdate'], ascending=True).reset_index(drop=True)
        
    def get_available_name(self,column_names,category=5):
        available_cname = {}
        # 用來查出可以用的欄位
        #1總經
        
        #2信用風險分析
        
        #3公司營運面資料
        
        #4公司交易面資料
        if category == 4 :
            for table_name in self.all_prc_dataset:
                table_cname = self.get_table_cname(market=self.market,table_name=table_name)       
                if table_cname is None:
                    continue
                
                available_items = self.get_column_name(market=self.market,table_name=table_name)
                
                columns_cname = available_items.get('columns_cname')
                columns_cname = numpy.intersect1d(column_names,columns_cname).tolist()
                if len(columns_cname)>0:  
                    column_names = numpy.setdiff1d(column_names, columns_cname).tolist()
                    available_cname[table_name] = columns_cname
        #5公司財務面資料
        elif category == 5 :
            columns_cname = self.accountData.loc[
                self.accountData['cname'].isin(column_names),['code','ename','cname']
                ].drop_duplicates(subset=['code'],keep='last')
            columns_cname = columns_cname['cname'].values        
            column_names = numpy.setdiff1d(column_names, columns_cname).tolist()
            available_cname['fin'] = columns_cname
        #6基金資料庫
        
        #7衍生性金融商品資料庫
        
        #8債券資料庫
        
        #9試用
    

        return available_cname,column_names