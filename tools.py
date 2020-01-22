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
import jettool.dataset
from .dataset import finreport
from .dataset import listedstock
from .pipeline import backtest
import os
import numpy

class financial_tool(finreport.financial_report,listedstock.listed_stock,backtest.backtest_base):
    def __init__(self):
        for name in params.__dict__:
            if '__' not in name and not callable(params.__dict__.get(name)):   
                self.__dict__[name] = params.__dict__.get(name)

    def get_data(self,window,column_names,base_date=None,mkts=['TSE']):
         
        #自動化處理觀測日期base_date=current_zdate
        if base_date is None:
            base_date = self.current_zdate
        else:
            #若使用者代的base_date超過範圍，必須校正
            self.current_zdate = base_date
        this_window_type,self.datastart_date = self.cal_zdate_by_window(window=window,
                                                                        base_date=self.current_zdate,
                                                                        tradeday=False)
        self.check_initial_data(mkts=mkts)        
        #產生標準交易日期資料
        self.prc_basedate = self.create_prc_base()
        
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
        self.get_benchmark(base_startdate=self.datastart_date,base_date=self.current_zdate)
        
    def get_price_data(self,mkts=['TSE','OTC'],prc_name=[]):
        print('查詢日資料 最大資料日期:'+str(self.current_zdate))
        for table_name in self.all_prc_dataset:
            
            available_items = self.get_column_name(market=self.market,table_name=table_name)
            available_cname = available_items.get('columns_cname')
            available_cname = numpy.intersect1d(prc_name,available_cname).tolist()
            prc_name = numpy.setdiff1d(prc_name, available_cname).tolist()

            if len(available_cname)>0:
                available_code,available_cname = self.compare_column_name(market=self.market,
                                                                          table_name=table_name,
                                                                          query_columns=available_cname)
                                                                          
                rename_set = { available_code[i]:available_cname[i] for i in range(0,len(available_code))}
                table_cname = self.get_table_cname(market=self.market,table_name=table_name)
                print(''.join(table_cname))            
                
                queried_data = self.query_data_with_date_coid(market=self.market,
                                                              table_name=table_name,
                                                              query_coid=self.input_coids,
                                                              mdate_up=self.current_zdate,
                                                              mdate_down=self.datastart_date,
                                                              query_columns=available_code,
                                                              rename_columns=rename_set)
                if len(queried_data)>0:
                    self.prc_basedate = self.prc_basedate.merge(queried_data,on=['zdate','coid'],how='left')
        
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