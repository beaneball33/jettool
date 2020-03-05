"""
TODO LIST:

1.query_basicdata需要改為抽象化查詢，改成到各個屬性table找裡面有標記"基本資料"的，但其實目前只有一個TWN的表

2.query_benchmark需要改為抽象化查詢，改成到各個table找描述裡面有標記'內含績效指標&績效指標代碼'
"""
from . import params
from . import dataset
from .dataset import finreport
from .dataset import dbapi
from .pipeline import backtest
import os
import numpy
import inspect
import json
import pandas
class financial_tool(finreport.financial_report,
                     backtest.backtest_base):
    """
    此為最外層的tool，規範所有讓使用者直接使用的查詢工具
    必須做到以下防呆處理：
    1.不需指定股票代碼，自動根據市場別決定股票名單
    2.不需指定財務科目代碼，自動根據中文名稱決定查詢科目名稱
    3.日資料的交易日會以該市場的大盤指數為準，校正交易日、補零
    4.find開頭代表查找某種東西，query開頭代表須要進行api取資料，get代表不進行query在已經取好的資料集中進行資料整合取得
    """
    def __init__(self,api_key=None):
        for name in params.__dict__:
            if '__' not in name and not callable(params.__dict__.get(name)):   
                self.__dict__[name] = params.__dict__.get(name)
        if api_key is not None:
            self.set_apikey(api_key)
        #self.load_data()

        self.dbapi=dbapi
        self.dbapi.api_key = api_key

    def query_data(self,window='1m',column_names='收盤價(元)',*,base_date=None,mkts=['TSE','OTC']):

        #自動化處理觀測日期base_date=current_zdate
        if base_date is None:
            base_date = self.dataend_date
        else:
            #若使用者代的base_date超過範圍，必須校正
            self.dataend_date = numpy.datetime64(base_date) 
        (this_window_type,
         self.datastart_date,
         window_int) = self.cal_zdate_by_window(window=window,
                                                base_date=self.dataend_date,
                                                tradeday=False)
        print('資料起始日：'+str(self.datastart_date))
        self.check_initial_data(mkts=mkts)        
        
        #處理查詢欄位名稱
        column_names_list = list(set(column_names)) 
        #開始檢查各個查詢名稱所屬的資料表

        #取出確實存在於會計科目的名稱
        available_fin_name,column_names = self.get_available_name(column_names_list,category=5)
        if len(available_fin_name)>0:
            self.query_report_data(mkts=mkts,available_cname=available_fin_name)
        #取出差異名稱


        
        #逐一檢查可查詢日資料清單
        available_prc_name,column_names = self.get_available_name(column_names,category=4)
        self.available_prc_name = available_prc_name
        self.query_dailydata(prc_name=available_prc_name)
        
        #查詢完畢，更新設定值
        self.set_data_attr()
        
        if (self.prc_basedate is not None and 
            self.findata_all is not None):
            
            self.set_back_test(back_interval=[1,-1])
            self.manage_report()
        df = self.get_activedate_data(window=window,
                                      column_names=column_names_list,
                                      base_date=base_date)[0]
        return df
    def get_data(self,column_names='收盤價(元)',*,window='1d',base_date=None):
        #處理查詢欄位名稱
        column_names_list = list(set(column_names))     
        if base_date is None:
            base_date = self.dataend_date        
        zdate_interval = self.get_zdate(base_date)
        print(zdate_interval)
        if (self.prc_basedate is not None and 
            self.findata_all is not None):
            
            self.set_back_test(back_interval=zdate_interval)
            self.manage_report()
        df = self.get_activedate_data(window=window,
                                      column_names=column_names_list,
                                      base_date=base_date)[0]
        return df
    def check_initial_data(self,*,mkts=['TSE','OTC']):
        #用來初始化查詢用的基本資料
        #取得會計科目表
        if self.accountData is None:
            self.inital_report()          
        #取得上市公司清單
        if self.input_coids is None:
            self.query_basicdata(mkts=mkts,base_startdate=self.datastart_date)    
        #取得標準交易日期資料
        self.query_benchmark(base_startdate=self.datastart_date,
                           base_date=self.dataend_date)


    def query_report_data(self,available_cname='收盤價(元)',*,mkts=['TSE','OTC'],active_view=False):
        # 可以抽象化查詢財報資料，自動整何公告日與財報季別
        
        print('查詢財報資料')
        
        acc_name = available_cname[0].get('columns_cname')
        self.check_initial_data()
        if len(acc_name) ==0:
            acc_name = ['常續性稅後淨利']
        query_code = self.get_acc_code(acc_name=acc_name,
                                       active_view=active_view)
        
        self.do_query(query_code=query_code,
                      query_length = self.query_length,
                      active_view=active_view)
        
        print('最大財報資料日期:'+str(self.current_mdate))
    def find_account_name(self,cname='收盤價(元)',active_view=False):
        #自動整合查詢財報科目，可以查名稱，若沒有則查分類
    
        if self.accountData is None:
            self.inital_report()    
        if type(cname).__name__ == 'str':
            cname_list = [cname]
        else:
            cname_list = cname
        cname_outcome = self.get_by_cgrp(active_view=active_view,
                                         cgrp=cname_list)
        if len(cname_outcome)<1:
            for cname in cname_list:
                cname_outcome_temp = self.get_by_word(active_view=active_view,
                                                      keyword=cname)
                if len(cname_outcome)<1 :
                    cname_outcome = cname_outcome_temp 
                else:
                    cname_outcome = numpy.append(cname_outcome,
                                                 cname_outcome_temp)
        return cname_outcome
    def query_dailydata(self,*,mkts=['TSE','OTC'],prc_name={}):
        
        if len(prc_name)>0:

            self.query_tradedata(prc_name=prc_name)
        
    def query_macrodata(self,*,prc_name={}):
        print('macro')
    def query_basicdata(self,*,mkts=['TSE'],base_startdate='2015-12-31'):
        # 基本屬性資料，需要改為抽像化查詢
        

        
        table_maping = dbapi.get_table_mapping(market='TWN',
                                               category_list=self.category_list,
                                               id='AIND').get('tableMap')
        base_table = None
        for table in table_maping:
            if self.market == table.get('dbCode'):
                base_table = table.get('tableId')

        query_columns = [
            'coid','mkt','elist_day1','list_day2','list_day1',
            'tejind2_c','tejind3_c','tejind4_c','tejind5_c']
            
        # define rename column, must remove after bugfixed
        rename_columns = {'tejind2_c': 'TEJ產業名','tejind3_c': 'TEJ子產業名',
            'tejind4_c':'TSE新產業名','tejind5_c':'主計處產業名'}
            
        # query all up-to-date listed stock 
        command_line = ["self.basic_info=self.tejapi.get('"+base_table+"',",
                        "opts={'columns':query_columns}, ",
                        "paginate=True)",
                        ".rename(index=str, columns=rename_columns)"]
        context = {"query_columns":query_columns,"rename_columns":rename_columns}
        
        self.exec_tool(context,command_line)
        # query all up-to-date delisted stock 
        self.basic_info_delist = self.tejapi.get('TWN/AIND',mkt='',
            list_day2={'gte':base_startdate},
            opts={'columns':query_columns},
            paginate=True).rename(index=str, columns=rename_columns)
        # always makesure date format is datetime64 without [ns]
        self.basic_info = self.basic_info.append(self.basic_info_delist,sort=False)
        self.basic_info['list_day2'] = pandas.to_datetime(self.basic_info['list_day2'].values,utc=True)
        self.basic_info['list_day2'] = self.basic_info['list_day2'].astype(str).astype('datetime64')
        self.basic_info['list_day1'] = self.basic_info['list_day1'].astype(str).astype('datetime64')
        self.basic_info['elist_day1'] = self.basic_info['elist_day1'].astype(str).astype('datetime64')
        # ANPRCSTD has no 'F' listed stock 
        self.listdata = self.tejapi.get('TWN/ANPRCSTD',
            coid=self.basic_info['coid'].values.tolist(),
            stype='STOCK',opts={'columns':['coid']},paginate=True)
        if self.input_coids is None:
            self.input_coids = self.listdata['coid'].values.tolist()
        else:
            self.input_coids = self.listdata.loc[self.listdata['coid'].isin(self.input_coids),'coid'].values.tolist()
			

    def query_benchmark(self,*,benchmark_id=None,base_startdate='2015-12-31',base_date='2019-12-31'):
        # 績效指標的函式 需要改為抽象化查詢
        
        if benchmark_id is None:
            benchmark_id=self.benchmark_id
        
        rename_column = {'mdate':'zdate','close_d':'績效指標指數','roib':'績效指標報酬率'}
        


        prc_table_list = self.category_list[4]
        # 從分類表中查詢可用的資料表
        
        self.benchmark_roi = self.tejapi.get('TWN/AAPRCDA',coid=benchmark_id,
            mdate={'gte':base_startdate,'lte':base_date},
            opts={"sort":"mdate.desc",'columns':['mdate','close_d']},
            paginate=True).rename(index=str, columns=rename_column)
            
        self.benchmark_roi['zdate'] = self.benchmark_roi['zdate'].astype(str).astype('datetime64')
        self.benchmark_roi['sdate'] = self.benchmark_roi['zdate'].astype(str).str[0:7].astype('datetime64')
        self.all_zdate_list = self.benchmark_roi['zdate'].astype(str).unique().astype('datetime64')
        self.back_date_list = self.all_zdate_list.copy()


    def save_data(self):
        # 此函式用來把目前query結果儲存在module路徑
        dir_name = os.path.dirname(inspect.getfile(dataset))
        
        if self.prc_basedate is not None:
            file_name = os.path.join(dir_name, "prc_basedate.pkl")
            self.prc_basedate.to_pickle(file_name)
            
        if self.findata_all is not None:
            file_name = os.path.join(dir_name, "findata_all.pkl")
            self.findata_all.to_pickle(file_name)
            
        file_name = os.path.join(dir_name, "data_attr.json")
        with open(file_name, 'w') as f:
            json.dump(self.data_attr, f)   
            
    def load_data(self,*,file_path=None):
        # 此函式用來把module路徑的暫存檔取出
        if file_path is None:
            dir_name = os.path.dirname(inspect.getfile(dataset))
        else:
            dir_name = os.path.dirname(file_path)

        file_name = os.path.join(dir_name, "prc_basedate.pkl")
        if os.path.isfile(file_name):
            self.prc_basedate = pandas.read_pickle(file_name)
        
        file_name = os.path.join(dir_name, "findata_all.pkl")
        if os.path.isfile(file_name):
            self.findata_all = pandas.read_pickle(file_name)

        file_name = os.path.join(dir_name, "data_attr.json")
        with open(file_name) as f:
            self.data_attr = json.load(f)  