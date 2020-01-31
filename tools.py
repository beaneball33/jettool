"""
在最外層的tool中，規範所有讓使用者直接使用的查詢工具
必須做到以下防呆處理：
1.不需指定股票代碼，自動根據市場別決定
2.不需指定財務科目代碼，自動根據中文名稱決定
3.日資料必須以該市場的大盤指數為準，校正交易日、補零
  依序使用get_basicdata()、get_benchmark()、create_prc_base()
  即可產生包含coid、zdate的標準交易日模板prc_basedate，以此為主來合併日資料
"""
from . import params
from . import dataset
from .dataset import finreport
from .dataset import listedstock
from .pipeline import backtest
import os
import numpy
import inspect
import json
import pandas
class financial_tool(finreport.financial_report,listedstock.listed_stock,backtest.backtest_base):
    def __init__(self):
        for name in params.__dict__:
            if '__' not in name and not callable(params.__dict__.get(name)):   
                self.__dict__[name] = params.__dict__.get(name)
        #self.load_data()
    def get_data(self,window,column_names,base_date=None,mkts=['TSE','OTC']):
         
        #自動化處理觀測日期base_date=current_zdate
        if base_date is None:
            base_date = self.dataend_date
        else:
            #若使用者代的base_date超過範圍，必須校正
            self.dataend_date = numpy.datetime64(base_date) 
        this_window_type,self.datastart_date,window_int = self.cal_zdate_by_window(window=window,
                                                                        base_date=self.dataend_date,
                                                                        tradeday=False)

        self.check_initial_data(mkts=mkts)        
        
        #處理查詢欄位名稱
        column_names = list(set(column_names)) 
        #開始檢查各個查詢名稱所屬的資料表

        #取出確實存在於會計科目的名稱
        available_fin_name = self.get_available_name(column_names)
        if len(available_fin_name)>0:
            self.get_report_data(mkts=mkts,acc_name=available_fin_name)
        #取出差異名稱

        left_name = numpy.setdiff1d(column_names, available_fin_name)
        
        #逐一檢查可查詢日資料清單
        self.get_price_data(prc_name=left_name)
        
        #查詢完畢，更新設定值
        self.set_data_attr()
        
        if self.all_date_data is None and self.prc_basedate is not None and self.findata_all is not None:
            self.set_back_test(back_interval=[1,-1])
            self.manage_report()
        df = self.get_activedate_data(window=window,
            column_names=column_names,
            base_date=base_date)[0]
        return df
    def check_initial_data(self,mkts=['TSE','OTC']):
        #用來初始化查詢用的基本資料
        #取得會計科目表
        if self.accountData is None:
            self.inital_report()          
        #取得上市公司清單
        if self.input_coids is None:
            self.get_basicdata(mkts=mkts,base_startdate=self.datastart_date)           
        #取得標準交易日期資料
        self.get_benchmark(base_startdate=self.datastart_date,base_date=self.dataend_date)

    def get_price_data(self,mkts=['TSE','OTC'],prc_name=[]):
        print('查詢日資料 最大資料日期:'+str(self.dataend_date))
        #產生標準交易日期資料
        prc_basedate = self.create_prc_base()
        merge_prc_basedate = None
        append_list = []
        

        part_query_interval = self.get_query_interval()
        full_query_interval = [{'mdate_up':self.dataend_date,'mdate_down':self.datastart_date}]
        
        for table_name in self.all_prc_dataset:
            job_list = part_query_interval
            temp_data = None
            merge_all = False
            if self.prc_basedate is not None:
                #未在原資料集內的名稱，整個重查
                if table_name in self.prc_basedate.columns.tolist():
                    merge_all = True            
                    job_list = full_query_interval
                    


            for data_interval in job_list:
                available_items = self.get_column_name(market=self.market,table_name=table_name)
                available_cname = available_items.get('columns_cname')
                available_cname = numpy.intersect1d(prc_name,available_cname).tolist()
                prc_name = numpy.setdiff1d(prc_name, available_cname).tolist()
            
                if len(available_cname)>0:

                    available_code,available_cname = self.compare_column_name(market=self.market,
                                                                              table_name=table_name,
                                                                              query_columns=available_cname)
                    mdate_up = data_interval['mdate_up']     
                    mdate_down=data_interval['mdate_down']                    
                    rename_set = { available_code[i]:available_cname[i] 
                                   for i in range(0,len(available_code))}
                    table_cname = self.get_table_cname(market=self.market,table_name=table_name)
                    print(''.join(table_cname))            
                    if merge_all is False:
                        append_list = append_list + available_cname                    
                    queried_data = self.query_data_with_date_coid(market=self.market,
                                                                  table_name=table_name,
                                                                  query_coid=self.input_coids,
                                                                  mdate_up=mdate_up,
                                                                  mdate_down=mdate_down,
                                                                  query_columns=available_code,
                                                                  rename_columns=rename_set)
                                
                    if len(queried_data)>0:
                        if temp_data is None:
                            temp_data = queried_data
                        else:
                            temp_data = temp_data.append(queried_data,sort=False)
            #各日期查詢完畢 開始組裝            
            if temp_data is not None:
                if merge_all is True:
                    if merge_prc_basedate is None:
                        merge_prc_basedate = temp_data
                    else:
                        merge_prc_basedate = merge_prc_basedate.merge(temp_data,on=['zdate','coid'])
                else:
                    prc_basedate = prc_basedate.merge(temp_data,on=['zdate','coid'],how='left')
                    
        if self.prc_basedate is None:
            self.prc_basedate = prc_basedate
        else:
            #先進行append，再進行merge
            #要分段append，避免重複
            append_columns = ['zdate','coid'] + append_list
            for data_interval in part_query_interval:
                temp_data = prc_basedate.loc[(prc_basedate['zdate']<data_interval['mdate_up'])&
                                            (prc_basedate['zdate']>=data_interval['mdate_down']),
                                            append_columns]
                self.prc_basedate = self.prc_basedate.append(temp_data,sort=False)
            self.prc_basedate = self.prc_basedate.drop_duplicates(subset=['coid','zdate'],keep='last')
            self.prc_basedate = self.prc_basedate.sort_values(by=['coid','zdate'],
                                                              ascending=True).reset_index(drop=True)
            if merge_prc_basedate is not None:
                self.prc_basedate = self.prc_basedate.merge(merge_prc_basedate,
                                                            on=['zdate','coid'],how='left')
            
    def get_report_data(self,mkts=['TSE','OTC'],acc_name=[],active_view=False):
        print('查詢財報資料')
        
        self.check_initial_data()
            
        query_code = self.get_acc_code(acc_name=acc_name,
                                       active_view=active_view)
        
        self.do_query(query_code=query_code,
                      query_length = self.query_length,
                      active_view=active_view)
        
        print('最大財報資料日期:'+str(self.current_mdate))
    def get_account_name(self,cname,active_view=False):
        if self.accountData is None:
            self.inital_report()    
        if type(cname).__name__ == 'str':
            cname_list = [cname]
        else:
            cname_list = cname
        cname_outcome = self.get_by_cgrp(active_view=active_view,cgrp=cname_list)
        if len(cname_outcome)<1:
            for cname in cname_list:
                cname_outcome_temp = self.get_by_word(active_view=active_view,keyword=cname)
                if len(cname_outcome)<1 :
                    cname_outcome = cname_outcome_temp 
                else:
                    cname_outcome = numpy.append(cname_outcome,cname_outcome_temp)
        return cname_outcome

    def save_data(self):
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
            
    def load_data(self,file_path=None):
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