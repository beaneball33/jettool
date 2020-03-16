"""
TODO LIST:

1.總經與外匯、利率等單key值轉置控制
2.記錄各個查詢結果的基本參數：頻率、zdate清單、原始表單與欄位原始實體名稱
3.query_tradedata()應該要控制不同資料頻率，要有不同的額外查詢區間，以避免0
"""
from . import dbapi
import tejapi
import pandas
import numpy
import re
from .. import params

class query_base(object):
    def __init__(self):
        self.tejapi = tejapi
        self.set_params(params.__dict__,allow_null=True)
        
    def set_params(self,new_params:dict,allow_null=False):
        for param in new_params:
            if '__' not in param and not callable(new_params.get(param)):   
                if self.__dict__.get(param) is not None or allow_null is True:
                    self.__dict__[param] = new_params.get(param)
                    
    def set_apikey(self,api_key:str = 'yourkey'):
        # 使用者設定api key之後的各種工作
        self.tejapi.ApiConfig.api_key = api_key
        self.api_key = api_key
        dbapi.api_key = self.api_key
        self.info = dbapi.get_info()
        tables = list(self.info.get('user').get('tables').keys())
        self.api_tables = dbapi.set_tablelist(tables)
        self.market_list = dbapi.get_market()
        self.category_list = dbapi.get_category()
        self.table_list = dbapi.get_tables()
        # 標準化日資料(有zdate，不需轉置)的查詢工具)，以便給定欄位名稱就可以查詢
        self.set_query_ordinal()        
        
     
        
    def set_market(self,market:str):
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
        
    def set_query_ordinal(self):
        #按照category_list中的順序，將可查詢的表拼湊
        tempordinal = []

        # 加入交易屬性類，以頻率決定

        for subcategory in self.category_list.get(4).get('subs'):
            if len(subcategory.get('tableMap'))>0:
                for table_attr in subcategory.get('tableMap'):
                    table_name = table_attr.get('tableId')
                    table_id = table_attr.get('tableId').split('/')[1]
                    if table_attr.get('dbCode') == self.market:
                        if table_id in self.api_tables.get(table_attr.get('dbCode')):
                            table_data = self.table_list.get(table_attr.get('dbCode'))
                            data_freq = table_data.get(table_id).get('frequency')
                            if data_freq  in self.all_prc_dataset_freq:

                                tempordinal.append(table_name)
                            else:
                                print(table_id+':'+data_freq)

        # 加入基本屬性類，需要知道mdate真實名稱才會被列入，故需要修改描述表
        for subcategory in self.category_list.get(3).get('subs'):
            if len(subcategory.get('tableMap'))>0:
                for table_attr in subcategory.get('tableMap'):
                    table_name = table_attr.get('tableId')
                    table_id = table_attr.get('tableId').split('/')[1]
                    if table_attr.get('dbCode') == self.market:
                        if table_id in self.api_tables.get(table_attr.get('dbCode')):
                            if table_id in self.mdate_name_dict.keys():
                                mdate_dict = self.mdate_name_dict.get(table_id)
                                frequency = mdate_dict.get('frequency')
                                if frequency  in self.all_prc_dataset_freq:

                                    tempordinal.append(table_name)
    
        # 加入信用風險分析
        for subcategory in self.category_list.get(2).get('subs'):
            if len(subcategory.get('tableMap'))>0:
                for table_attr in subcategory.get('tableMap'):
                    table_name = table_attr.get('tableId')
                    table_id = table_attr.get('tableId').split('/')[1]
                    if table_attr.get('dbCode') == self.market:
                        if table_id in self.api_tables.get(table_attr.get('dbCode')):
                            table_data = self.table_list.get(table_attr.get('dbCode'))
                            data_freq = table_data.get(table_id).get('frequency')
                            if data_freq  in self.all_prc_dataset_freq:

                                tempordinal.append(table_name)
        self.all_prc_dataset = tempordinal
        tempmarco = []
        
        #管理總經類GLOBAL
        for subcategory in self.category_list.get(1).get('subs'):
            # 逐一檢視設定表內各個TABLE
            if len(subcategory.get('tableMap'))>0:
                for table_attr in subcategory.get('tableMap'):
                    table_name = table_attr.get('tableId')
                    table_id = table_name.split('/')[1]
                    #檢查必須有代碼對照表
                    if table_id in self.api_tables.get(table_attr.get('dbCode')):
                        coid_map_table = self.macro_mapping_coids.get(table_name)
                        if coid_map_table is not None: 
                            #總經必須查詢代碼表
                            if coid_map_table.get('coid_list') is None:
                                coid_table = coid_map_table.get('coid_table')
                                coid_cname_column = coid_map_table.get('cname')
                                code_data = tejapi.get(coid_table,
                                                      opts={'columns':['coid',coid_cname_column]},
                                                      paginate=True).values.tolist()
                                coid_list = {rows[1]:rows[0] for rows in code_data}
                                coid_map_table['coid_list'] = coid_list
                            # 產生欄位名稱後綴列表
                            val_cname_dict = self.get_val_cname(table_name)
                            # 取得名稱與代碼對照表
                            coid_list = coid_map_table.get('coid_list')          
                            # 產生欄位全名對實體欄位名+代碼對照表
                            coid_map_table['fullname_map'] = self.create_mapping_cname(val_cname_dict,coid_list)
                            tempmarco.append(table_name)
                            
                                                        
        self.all_marco_dataset = tempmarco
                
    def get_dataset_name(self,table_name:str) -> str:
        # 產生資料表全名

        dataset_name = table_name
        
        if self.table_info.get(dataset_name) is None:
            try:
                self.table_info[dataset_name] = self.tejapi.table_info(dataset_name)
            except (RuntimeError, TypeError, NameError):
                # 代表不是有資料而是對照表，略過
                self.table_info[dataset_name] = None        
                print(dataset_name+':table info error')
                
        return dataset_name
        
    def query_tool(self,
            table_name:str = 'APRCD',query_coid:list = ['2330'],
            mdate_up:str = '2019-12-31',mdate_down:str = '2018-12-31',
            coid_name:str = 'coid',mdate_name:str = 'mdate',
            query_columns=['coid','mdate'],rename_columns=None) -> pandas.DataFrame:
        # 根據給定資料表名稱與條件，動態產生查詢式
        
        dataset_name = self.get_dataset_name(table_name)
        
        self.tempdata = None
        
        query_columns = list(set(query_columns + [coid_name,mdate_name]))

        """
        command_line = "self.tempdata=self.tejapi.get(dataset_name,coid=query_coid,"
        command_line+= mdate_name+"={'gte':mdate_down,'lte':mdate_up},"
        command_line+= "opts={'sort':'"+mdate_name+".desc','columns':query_columns}, paginate=True)"
        command_line+= ".rename(index=str, columns={'"+mdate_name+"':'zdate'})"
        """
        command_line = ["self.tempdata=self.tejapi.get('",
                        dataset_name+"',"+coid_name+"=query_coid,",
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
    def exec_tool(self,context:dict,command_line:list):
        context['self'] = self
        context['__name__'] = '__main__'
        command_line_str = ''.join(command_line)

        exec(command_line_str, context)
    def get_table_cname(self,table_name:str = 'TWN/APRCD',language:str = 'cname') -> str:
    
        # 取得table名稱並同時透過api查詢該table的資訊
        dataset_name = self.get_dataset_name(table_name)
        
        table_info = self.table_info.get(dataset_name)
        table_info['columns_cname'] = [ 
            cols['cname'] for cols in table_info['columns'].values()]
        table_info['columns_name'] = [
            cols['name'] for cols in table_info['columns'].values()]
        self.table_info[dataset_name] = table_info
        return self.table_info[dataset_name]['name']
    def get_table_key(self,table_name:str = 'TWN/APRCD') -> list:
    
        dataset_name = self.get_dataset_name(table_name)
        return self.table_info[dataset_name]['primaryKey']
        
    def compare_column_name(self,table_name:str = 'TWN/APRCD',
                                query_columns:list = ['收盤價(元)'],
                                kind:str = 'cname'):
        # 比較指定資料表單中是否存在某個欄位名稱
    
        dataset_name = self.get_dataset_name(table_name)
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
    def compare_code_name(self,table_name:str,query_columns:list):
        # 比較指定代碼表單中是否存在某個代碼名稱
    
        coid_list = None
        for table_id in self.macro_mapping_coids:
            dataset = self.macro_mapping_coids.get(table_id)
            if table_name == table_id:
                coid_list = dataset.get('coid_list')
                break

        ans_code = []
        ans_name = []
        for cname in query_columns:
            if coid_list.get(cname) is not None:
                ans_code += [coid_list.get(cname)]
                ans_name += [cname]
        left_name = numpy.setdiff1d(query_columns, ans_name).tolist()
        if len(left_name)>0:
            print('lack columns:')
            print(left_name)
        return ans_code,ans_name        
    def get_column_name(self,table_name:str = 'TWN/APRCD',
                        language:str = 'cname') -> dict:
        # 取得欄位的中文名稱，以便用來把欄位實體名稱改為中文
        dataset_name = self.get_dataset_name(table_name)
        
        table_cname = self.get_table_cname(table_name=table_name)
        table_info = self.table_info.get(dataset_name)

        return {'columns_cname':table_info['columns_cname'],
                'columns_name':table_info['columns_name']}
                
    def combine_column_record(self,column_record:list):
        column_record_dict = {}
        fin_dict = None
        fin_table_id = self.account_table.get(self.market).get('data')
        for row in column_record:
            table_id = row.get('id')
            if fin_table_id == table_id:
                fin_dict = row
            else:
                if column_record_dict.get(table_id) is None:
                    column_record_dict[table_id] = row.get('columns_cname')
                else:
                    column_record_dict[table_id] = list(set(column_record_dict[table_id] + 
                                                    row.get('columns_cname')))
        column_record = [{'id':table_id,'columns_cname':column_record_dict.get(table_id)}
                        for table_id in column_record_dict.keys()]
        return column_record,fin_dict
        
    def get_column_record(self,column_names:list):
        column_record = []
        column_list = []
        for column in column_names:
            
            if type(column) is str:
                column_list.append(column)
            elif type(column) is dict:
                column_id = column.get('id').split('/')           
                column_market = column_id[0]
                column_codes = column_id[1]

                if column_market in self.market or column_market in 'GLOBAL':
                   
                    column_record.append(column)
                else:
                    print(column.get('id')+': inconsistent db code')
        column_list = list(set(column_list)) 
        return column_list,column_record
        
    def set_listed_coid(self,df:pandas.DataFrame):
        # 透過basic_info中的上市股票名單取得coid
        
        listed_coids = self.basic_info.loc[
                                       self.basic_info['list_day1']<=self.current_zdate,
                                       'coid'].values.tolist()
        self.current_coids = df.loc[(df['zdate']==self.current_zdate),['zdate','coid']]
        self.listed_coids = df.loc[
            (df['zdate']==self.current_zdate)&(df['coid'].isin(listed_coids)),'coid'
            ].values.tolist()

    def get_window(self, window:str = '4Q') -> list:
        """
        check input window is begin with number and end with D(day), W(week), M(month), Q(quanter) or Y(year)
        return a list with [int, str]: [4, 'Q']
        """
        window = window.upper()
        match = re.match(r"^([0-9.]+)([DWMQY]{1}$)", window)
        if bool(match):
            return [int(match.group(1)), match.group(2)]

    def cal_zdate_by_window(self,window,base_date:str,
                                 peer_future:bool = False,tradeday:bool = True):
        # 計算指定移動窗口以前的zdate
    
        match_window = self.get_window(window=window)
        window = match_window[0]
        this_window_type = match_window[1]

        if this_window_type != 'Q':
            jump_length = window if peer_future is False else -1*window
            next_base_date = self.cal_zdate(
                                  base_date=base_date,jump_length=jump_length,
                                  jump_kind=this_window_type,tradeday=tradeday)
        next_base_date = numpy.array([next_base_date]).astype('datetime64')[0]

        return this_window_type,next_base_date,window
    def get_zdate(self,base_date:str) ->list:

        base_date = numpy.datetime64(base_date) 
        last_zdate = numpy.datetime64(base_date) 
        for zdate in self.all_zdate_list:
            if zdate < base_date:
                print(zdate)
                break
            last_zdate = zdate
            
        return [zdate,last_zdate]
    def get_activedate_data(self,
            window:str = '3m',column_names:list = ['zdate','mdate','coid'],
            peer_future:bool = False,base_date:str = None,base_mdate:str = None,
            clue_length:int = None,keep:str = 'first'):
        # 根據window取得某些欄位資料
        
        current_data = None
        column_names = list(set(['zdate','mdate','coid']+column_names))
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

    def check_full_query(self,table_name:str,
                         target_dataframe:pandas.DataFrame,
                         available_cname:list) -> bool:
        full_query = False  
        table_cname = self.get_table_cname(table_name=table_name)     

        # 判斷查詢的日期區間工作要執行哪一個，以及要不要重查
        if target_dataframe is not None:
            for col_name in available_cname:
                #未在原資料集內的名稱，整個重查
                if col_name  not in target_dataframe.columns.tolist():
                    full_query =  True     
                    job_list = self.full_query_interval
                else:
                    self.append_list = self.append_list + [col_name]
        else:
            full_query = True
            
        #控制整個完成後要append還merge
        if full_query is False:
            self.append_list = self.append_list + available_cname   
            
        print(table_cname+' 重新查詢:'+str(full_query)) 
        return full_query
    def make_query_job(self,table_name:str,query_columns:list,query_coid:list,
                       rename_set:dict,full_query:bool,) -> pandas.DataFrame:
        
        temp_data = None
        # 控制要查詢的長度
        job_list = self.part_query_interval            
        if full_query is True:
            job_list = self.full_query_interval    
        # 用來控制mdate名稱，需修改，並且要多判斷coid名稱
        mdate_name = 'mdate' 
        if self.mdate_name_dict.get(table_name) is not None:
            mdate_name = self.mdate_name_dict.get(table_name).get('mdate')
            
        # 根據job_list進行整合查詢
        for data_interval in job_list:
            mdate_up = data_interval['mdate_up']     
            mdate_down=data_interval['mdate_down']                    
     
            queried_data = self.query_tool(table_name=table_name,
                                           query_coid=query_coid,
                                           mdate_up=mdate_up,
                                           mdate_down=mdate_down,
                                           mdate_name=mdate_name,
                                           query_columns=query_columns,
                                           rename_columns=rename_set)

            if len(queried_data)>0:
                if temp_data is None:
                    temp_data = queried_data
                else:
                    temp_data = temp_data.append(queried_data,sort=False)
        return temp_data
        
    # 此方法用來控制查詢機制，以便查詢交易資料，並控制查完的整併
    #
    def query_tradedata(self,query_list:list = []):
        partquery_prc_basedate = self.create_prc_base()
        fullquery_prc_basedate = partquery_prc_basedate.reindex(columns=['coid','zdate']).copy()
        print('查詢日交易資料 最大資料日期:'+str(self.dataend_date))

        #for table_name in self.all_prc_dataset:
        for table_attr in query_list:
            table_name = table_attr.get('id')
            full_query = False
            print(table_name)
            available_cname = table_attr.get('columns_cname')
            if table_name in self.all_prc_dataset is False:
                print("沒有存取權限："+table_name)
                continue
   
            #在此table尋找可用的欄位
            available_code,available_cname = self.compare_column_name(table_name=table_name,
            
                                                    query_columns=available_cname)
            temp_data = None
            if len(available_code)>0:
                #判斷是否要重新查詢
                full_query = self.check_full_query(table_name=table_name,
                                                   target_dataframe=self.prc_basedate,
                                                   available_cname=available_cname)   
                #設定查詢完畢更名的邏輯                
                rename_set = { available_code[i]:available_cname[i] 
                           for i in range(0,len(available_code))} 
                           
                # 因為是有股票代碼的，查詢代碼為股票代碼清單           
                
                temp_data = self.make_query_job(table_name=table_name,
                                                query_columns=available_code,
                                                query_coid=self.input_coids,
                                                rename_set=rename_set,
                                                full_query=full_query)

            #各日期查詢完畢 開始組裝            
            if temp_data is not None:
                if full_query is True:
                    fullquery_prc_basedate = fullquery_prc_basedate.merge(temp_data,on=['zdate','coid'],how='left')
                else:
                    partquery_prc_basedate = partquery_prc_basedate.merge(temp_data,on=['zdate','coid'],how='left')
        
        if self.prc_basedate is None:
            self.prc_basedate = fullquery_prc_basedate
        else:
            #先進行刪除，再append，再進行merge
            append_columns = ['zdate','coid'] + self.append_list
            self.prc_basedate = self.prc_basedate.reindex(columns=append_columns)

            #要分段append，避免重複
            for data_interval in self.part_query_interval:
                temp_data = partquery_prc_basedate.loc[(partquery_prc_basedate['zdate']<data_interval['mdate_up'])&
                                            (partquery_prc_basedate['zdate']>=data_interval['mdate_down']),
                                            append_columns]
                self.prc_basedate = self.prc_basedate.append(temp_data,sort=False)
            self.prc_basedate = self.prc_basedate.drop_duplicates(subset=['coid','zdate'],keep='last')
            self.prc_basedate = self.prc_basedate.sort_values(by=['coid','zdate'],
                                                              ascending=True).reset_index(drop=True)

            self.prc_basedate = fullquery_prc_basedate.merge(self.prc_basedate,
                                                        on=['zdate','coid'],how='left')
            self.prc_basedate = self.prc_basedate.sort_values(by=['coid','zdate'], ascending=True).reset_index(drop=True)


    # 此方法用來控制查詢機制，以便查詢交易資料，並控制查完的整併
    #
    def query_macrometa(self,query_list:list = []):
        partquery_prc_basedate = self.create_prc_base()
        fullquery_prc_basedate = partquery_prc_basedate.reindex(columns=['coid','zdate']).copy()
        print('查詢日交易資料 最大資料日期:'+str(self.dataend_date))

        #for table_name in self.all_marco_dataset
        for table_attr in query_list:
            table_name = table_attr.get('id')
            full_query = False
            print(table_name)
            available_cnames = table_attr.get('columns_cname')
            if table_name in self.all_marco_dataset is False:
                print("沒有存取權限："+table_name)
                continue
                
            
            #在此table尋找可用的欄位
            for available_cname in available_cnames:
                this_mapping_table = self.macro_mapping_coids.get(table_name).get('fullname_map')
                available_coid = this_mapping_table.get(available_cname).get('coid')
                available_col = [this_mapping_table.get(available_cname).get('val')]
            
                temp_data = None
                if len(available_code)>0:
                    #判斷是否要重新查詢
                    available_cname = [available_cname]
                    full_query = self.check_full_query(table_name=table_name,
                                                       target_dataframe=self.prc_basedate,
                                                       available_cname=available_cname)   
                    #設定查詢完畢更名的邏輯                
                    rename_set = {'val':available_cname} 
                               
                    # 因為是有股票代碼的，查詢代碼為股票代碼清單           
                    
                    temp_data = self.make_query_job(table_name=table_name,
                                                    query_columns=available_col,
                                                    query_coid=available_coid,
                                                    rename_set=rename_set,
                                                    full_query=full_query)

                #各日期查詢完畢 開始組裝            
                if temp_data is not None:
                    if full_query is True:
                        fullquery_prc_basedate = fullquery_prc_basedate.merge(temp_data,on=['zdate','coid'],how='left')
                    else:
                        partquery_prc_basedate = partquery_prc_basedate.merge(temp_data,on=['zdate','coid'],how='left')
            
        if self.prc_basedate is None:
            self.prc_basedate = fullquery_prc_basedate
        else:
            #先進行刪除，再append，再進行merge
            append_columns = ['zdate','coid'] + self.append_list
            self.prc_basedate = self.prc_basedate.reindex(columns=append_columns)

            #要分段append，避免重複
            for data_interval in self.part_query_interval:
                temp_data = partquery_prc_basedate.loc[(partquery_prc_basedate['zdate']<data_interval['mdate_up'])&
                                            (partquery_prc_basedate['zdate']>=data_interval['mdate_down']),
                                            append_columns]
                self.prc_basedate = self.prc_basedate.append(temp_data,sort=False)
            self.prc_basedate = self.prc_basedate.drop_duplicates(subset=['coid','zdate'],keep='last')
            self.prc_basedate = self.prc_basedate.sort_values(by=['coid','zdate'],
                                                              ascending=True).reset_index(drop=True)

            self.prc_basedate = fullquery_prc_basedate.merge(self.prc_basedate,
                                                        on=['zdate','coid'],how='left')
            self.prc_basedate = self.prc_basedate.sort_values(by=['coid','zdate'], ascending=True).reset_index(drop=True)
            
    def create_prc_base(self,query_coids:list=None,benchmark:bool=False) -> pandas.DataFrame:
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

    def get_val_cname(self,table_name:str):
                #從代碼對照表中取出所有代碼名稱，
        dataset_name = self.get_dataset_name(table_name)
        if dataset_name is not None:
            val_cname = {}
            # 取出表單的key
            pk_columns = self.table_info.get(dataset_name).get('primaryKey')
            # 取出表單的完整屬性
            all_column = self.table_info.get(dataset_name).get('columns')
            for column in all_column:
                if column not in pk_columns:
                    this_column = all_column.get(column)
                    all_coid_cname = self.macro_mapping_coids.get('coid_list')
                    val_cname[this_column.get('cname')] = this_column.get('name')
        return val_cname
        
    def create_mapping_cname(self,val_cname:dict,coid_map:dict):
        ans = { coid_name+'_'+val_name:{'val':val_cname.get(val_name),'coid':coid_map.get(coid_name)}
                for val_name in val_cname for coid_name in coid_map }
    
        return ans
    def get_available_name(self,column_names:list,category:int = 5):
        available_cname = []
        # 用來查出可以用的欄位
        #1總經
        # 總經要控制coid名稱+欄位來當欄位名稱 比如總經GLOBAL/ANMAR的'外銷訂單總額(年)'指定數值'val'欄位(中文名稱'數值')時
        # 則產生的全名為'外銷訂單總額(年)_數值'，而flag欄位pfr則產生'外銷訂單總額(年)_數值類型'
        if category == 1 :
            for table_id in self.all_marco_dataset:
                #取出對照資料
                coid_map_table = self.macro_mapping_coids.get(table_id)
                # 產生欄位名稱後綴列表
                val_cname_dict = self.get_val_cname(table_id)
                # 取得名稱與代碼對照表
                coid_list = coid_map_table.get('coid_list')          
                # 產生欄位全名對實體欄位名+代碼對照表
                columns_cname = coid_map_table.get('fullname_map')
                
                columns_cname = list(columns_cname.keys())
                #查詢欄位與名稱的交集
                columns_cname = numpy.intersect1d(column_names,columns_cname).tolist()
                if len(columns_cname)>0:  
                    column_names = numpy.setdiff1d(column_names, columns_cname).tolist()
                    available_cname.append({'id':table_id,'columns_cname':columns_cname})
        #2信用風險分析        
        #3公司營運面資料
        #4公司交易面資料
        elif category == 2 or category == 3 or category == 4 :
            for table_id in self.all_prc_dataset:
                table_cname = self.get_table_cname(table_name=table_id)       
                if table_cname is None:
                    continue
                
                available_items = self.get_column_name(table_name=table_id)
                
                columns_cname = available_items.get('columns_cname')
                columns_cname = numpy.intersect1d(column_names,columns_cname).tolist()
                if len(columns_cname)>0:  
                    column_names = numpy.setdiff1d(column_names, columns_cname).tolist()
                    available_cname.append({'id':table_id,'columns_cname':columns_cname})
                    print(table_id+' '+table_cname)
        #5公司財務面資料
        elif category == 5 :
            columns_cname = self.accountData.loc[
                self.accountData['cname'].isin(column_names),['code','ename','cname']
                ].drop_duplicates(subset=['code'],keep='last')
            columns_cname = columns_cname['cname'].values.tolist()     
            column_names = numpy.setdiff1d(column_names, columns_cname).tolist()
            available_cname = [{'id':'fin','columns_cname':columns_cname}]
        #6基金資料庫
        
        #7衍生性金融商品資料庫
        
        #8債券資料庫
        
        #9試用
    
        return available_cname,column_names

