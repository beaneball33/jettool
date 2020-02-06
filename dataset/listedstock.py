from . import querybase
import pandas
# to-do:query basic data mapping dict to change between country
class listed_stock(querybase.query_base):
    """
    此類中暫時存放hardcode寫死的query方法，在修改為對照表查詢之功能後移除改放至querybase
    """
    def __init__(self):
        self.basic_info = None
        self.prc_basedate = None
        
        self.market_attr = {}
        
    def get_basicdata(self,mkts=['TSE'],base_startdate='2015-12-31'):
        # define query column
        query_column = [
            'coid','mkt','elist_day1','list_day2','list_day1',
            'tejind2_c','tejind3_c','tejind4_c','tejind5_c']
            
        # define rename column, must remove after bugfixed
        rename_column = {'tejind2_c': 'TEJ產業名','tejind3_c': 'TEJ子產業名',
            'tejind4_c':'TSE新產業名','tejind5_c':'主計處產業名'}
            
        # query all up-to-date listed stock 
        self.basic_info = self.tejapi.get('TWN/AIND',mkt=mkts,
            opts={'columns':query_column},
            paginate=True).rename(index=str, columns=rename_column)
            
        # query all up-to-date delisted stock 
        self.basic_info_delist = self.tejapi.get('TWN/AIND',mkt='',
            list_day2={'gte':base_startdate},
            opts={'columns':query_column},
            paginate=True).rename(index=str, columns=rename_column)
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
			

    def get_benchmark(self,base_startdate='2015-12-31',base_date='2019-12-31'):
        rename_column = {'mdate':'zdate','close_d':'績效指標指數','roib':'績效指標報酬率'}
        self.benchmark_roi = self.tejapi.get('TWN/APRCD',coid=self.benchmark_id,
            mdate={'gte':base_startdate,'lte':base_date},
            opts={"sort":"mdate.desc",'columns':['mdate','close_d','roib']},
            paginate=True).rename(index=str, columns=rename_column)
        self.benchmark_roi['zdate'] = self.benchmark_roi['zdate'].astype(str).astype('datetime64')
        self.benchmark_roi['sdate'] = self.benchmark_roi['zdate'].astype(str).str[0:7].astype('datetime64')
        self.all_zdate_list = self.benchmark_roi['zdate'].astype(str).unique().astype('datetime64')
        self.back_date_list = self.all_zdate_list.copy()


        
