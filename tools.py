from . import params
from . import dataset
import jettool.dataset
from .dataset import finreport
from .dataset import listedstock
import os

class financial_tool(finreport.financial_report,listedstock.listed_stock):
    def __init__(self):
        for name in params.__dict__:
            if '__' not in name and not callable(params.__dict__.get(name)):   
                self.__dict__[name] = params.__dict__.get(name)
    def get_combinedata(self,mkts=['TSE','OTC']):
        self.get_basicdata(mkts=mkts,base_startdate=self.datastart_date)
        self.get_benchmark(base_startdate=self.datastart_date,base_date=self.base_date)
        self.get_dailydata(base_startdate=self.datastart_date,base_date=self.base_date)