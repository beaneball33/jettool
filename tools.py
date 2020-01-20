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
        this_window_type,self.datastart_date = self.cal_zdate_by_window(window=window,base_date=self.current_zdate,tradeday=False)
        #取得會計科目表
        if self.accountData is None:
            self.inital_report()          
        #取得上市公司清單
        if self.input_coids is None:
            self.get_basicdata(mkts=mkts,base_startdate=self.datastart_date)           
        #取得標準交易日期資料
        self.get_benchmark(base_startdate=self.datastart_date,base_date=self.current_zdate)
        self.prc_basedate = self.create_prc_base()
        
        #處理查詢欄位名稱
        column_names = list(set(column_names)) 
        #開始檢查各個查詢名稱所屬的資料表
        print([self.current_zdate,self.datastart_date])
        #取出確實存在於會計科目的名稱
        available_fin_name = self.get_available_name(column_names)
        if len(available_fin_name)>0:
            self.get_report_data(mkts=mkts,acc_name=available_fin_name)
        #取出差異名稱
        print([self.current_zdate,self.datastart_date])
        left_name = numpy.setdiff1d(column_names, available_fin_name)
        #逐一檢查日資料清單
        all_pre_dataset = ['TWN/APRCD','TWN/AFF_RAW']

        for dataset_name in all_pre_dataset:
            available_items = self.get_column_name(table_name=dataset_name)
            available_cname = available_items.get('columns_cname')
            available_cname = numpy.intersect1d(left_name,available_cname).tolist()
            left_name = numpy.setdiff1d(left_name, available_cname).tolist()

            if len(available_cname)>0:
                available_code,available_cname = self.compare_column_name(table_name=dataset_name,query_columns=available_cname)
                rename_set = { available_code[i]:available_cname[i] for i in range(0,len(available_code))}
                table_cname = self.get_table_cname(table_name=dataset_name)
                print(''.join(table_cname))            
                
                queried_data = self.query_data_with_date_coid(
                    table_name=dataset_name,
                    query_coid=self.input_coids,
                    mdate_up=self.current_zdate,
                    mdate_down=self.datastart_date,
                    query_columns=available_code,
                    rename_columns=rename_set)
                if len(queried_data)>0:
                    self.prc_basedate = self.prc_basedate.merge(queried_data,on=['zdate','coid'],how='left')
        
        
        if self.all_date_data is None and self.prc_basedate is not None and self.findata_all is not None:
            self.set_back_test(back_interval=[1,-1])
            self.manage_report()
        df = self.get_activedate_data(window=window,column_names=column_names,base_date=base_date)[0]
        return df
    def get_price_data(self,mkts=['TSE','OTC']):
        print('查詢股價資料')
        self.get_basicdata(mkts=mkts,base_startdate=self.datastart_date)
        self.get_benchmark(base_startdate=self.datastart_date,base_date=self.base_date)
        self.get_dailydata(base_startdate=self.datastart_date,base_date=self.base_date)
        self.current_zdate = self.prc_basedate['zdate'].max()
        self.all_zdate_list = numpy.sort(self.prc_basedate['zdate'].unique())
        self.set_listed_coid(self.prc_basedate)
        print('最大資料日期:'+self.current_zdate.strftime('%Y-%m-%d'))
    def get_report_data(self,mkts=['TSE','OTC'],acc_name=[],active_view=False):
        print('查詢財報資料')
        if self.accountData is None:
            self.inital_report()    
        if self.benchmark_roi is None:
            self.get_benchmark(base_startdate=self.datastart_date,base_date=self.base_date)
        if self.input_coids is None:
            self.get_basicdata(mkts=mkts,base_startdate=self.datastart_date)
            
        query_code = self.get_acc_code(acc_name=acc_name,active_view=active_view)
        
        self.do_query(query_code=query_code,
            query_length = self.query_length,
            active_view=active_view)

        self.current_mdate = self.findata_all['mdate'].max()
        self.all_mdate_list = numpy.sort(self.findata_all['mdate'].unique())
        
        print('最大財報資料日期:'+self.current_mdate.strftime('%Y-%m-%d'))
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