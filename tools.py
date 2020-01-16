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
    def get_data(self,window,column_names,base_date=None):
        if base_date is None:
            base_date = self.current_zdate
        if self.all_date_data is None and self.prc_basedate is not None and self.findata_all is not None:
            self.set_back_test(back_interval=[1,-1])
            self.manage_report()
        df = self.get_activedate_data(window=window,column_names=column_names,base_date=base_date)[0]
        return df
    def get_price_data(self,mkts=['TSE','OTC']):
        self.get_basicdata(mkts=mkts,base_startdate=self.datastart_date)
        self.get_benchmark(base_startdate=self.datastart_date,base_date=self.base_date)
        self.get_dailydata(base_startdate=self.datastart_date,base_date=self.base_date)
        self.current_zdate = self.prc_basedate['zdate'].max()
        self.all_zdate_list = numpy.sort(self.prc_basedate['zdate'].unique())
        print('最大資料日期:'+self.current_zdate.strftime('%Y-%m-%d'))
    def get_report_data(self,mkts=['TSE','OTC'],acc_name=[],active_view=False):
        if self.accountData is None:
            self.inital_report()    
        if self.input_coids is None:
            self.get_basicdata(mkts=mkts,base_startdate=self.datastart_date)
        query_code = self.get_acc_code(acc_name=acc_name,active_view=active_view)
        self.do_query(query_code=query_code,
            query_length = self.query_length,
            active_view=active_view)
        self.current_zdate = self.findata_all['zdate'].max()
        self.current_mdate = self.findata_all['mdate'].max()
        self.all_mdate_list = numpy.sort(self.findata_all['mdate'].unique())
        self.set_listed_coid(self.findata_all)
        print('最大資料日期:'+self.current_mdate.strftime('%Y-%m-%d'))
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