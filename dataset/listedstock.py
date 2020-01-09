from . import querybase


class listed_stock(querybase.query_base):
    def __init__(self):
        self.basic_info = None
        self.prc_basedate = None

    def get_basicdata(self,mkts=['TSE'],base_startdate='2015-12-31'):
        #define query column
        query_column = [
            'coid','mkt','elist_day1','list_day2','list_day1',
            'tejind2_c','tejind3_c','tejind4_c','tejind5_c']
        #define rename column, must remove after bugfixed
        rename_column = {'tejind2_c': 'TEJ產業名','tejind3_c': 'TEJ子產業名',
            'tejind4_c':'TSE新產業名','tejind5_c':'主計處產業名'}
        #query all up-to-date listed stock 
        self.basic_info = self.tejapi.get('TWN/AIND',mkt=mkts,
            opts={'columns':query_column},
            paginate=True).rename(index=str, columns=rename_column)
        #query all up-to-date delisted stock 
        self.basic_info_delist = self.tejapi.get('TWN/AIND',mkt='',
            list_day2={'gte':base_startdate},
            opts={'columns':query_column},
            paginate=True).rename(index=str, columns=rename_column)
        #always makesure date format is datetime64 without [ns]
        self.basic_info = self.basic_info.append(self.basic_info_delist,sort=False)
        self.basic_info['list_day1'] = self.basic_info['list_day1'].astype(str).astype('datetime64')
        self.basic_info['elist_day1'] = self.basic_info['elist_day1'].astype(str).astype('datetime64')
        #ANPRCSTD has not 'F' listed stock 
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
        self.all_zdate_list = self.benchmark_roi['zdate'].unique()
        self.back_date_list = self.benchmark_roi['zdate'].unique()


    def get_dailydata(self,query_coids=None,base_startdate='2015-12-31',base_date='2019-12-31'):
        query_column = ['mdate','coid','close_d','open_d','high_d','low_d','roib','mv','tej_cdiv']
        rename_column = {'mdate':'zdate','close_d':'股價','open_d':'開盤價',
            'high_d':'最高價','low_d':'最低價','mv':'市值','roib':'報酬率','tej_cdiv':'現金股利率'}
        self.prc_basedate = None
        if query_coids is None:
            query_coids = self.input_coids
        if self.benchmark_roi is not None:
            print('查詢個股股價')
            for query_coid in query_coids:
                list_day1 = self.basic_info.loc[self.basic_info['coid']==query_coid,'list_day1'].values[0]
                this_roi_data = self.get_price_with_order(query_coid,base_startdate,base_date,query_column,rename_column)
                list_day2 = this_roi_data['zdate'].max()
                this_prc_basedate = self.benchmark_roi[(self.benchmark_roi['zdate']<=list_day2)].copy()
                this_prc_basedate = this_prc_basedate.merge(this_roi_data,on=['zdate'],how='left')
                #要補上代碼，否則仍是空值
                this_prc_basedate['coid'] = query_coid
                #報酬率空值處理
                this_prc_basedate['報酬率'] = this_prc_basedate['報酬率'].fillna(0)
                if self.prc_basedate is None:
                    self.prc_basedate = this_prc_basedate
                else:
                    self.prc_basedate = self.prc_basedate.append(this_prc_basedate,sort=False)
            self.prc_basedate['zdate'] =  self.prc_basedate['zdate'].astype(str).astype('datetime64')
            self.prc_basedate['報酬率'] = self.prc_basedate['報酬率'] .fillna(0)
        else:
            self.prc_basedate = self.get_price_with_order(query_coids,base_startdate,base_date,query_column,rename_column)

    def get_price_with_order(self,query_coid,base_startdate,base_date,query_column,rename_column):
        this_roi_data = self.tejapi.get('TWN/APRCD',coid=query_coid,
            mdate={'gte':base_startdate,'lte':base_date},
            opts={"sort":"mdate.desc",'columns':query_column}, paginate=True).rename(index=str,
            columns=rename_column)
        if 'zdate' in this_roi_data.columns:
            this_roi_data['zdate'] = this_roi_data['zdate'].astype(str).astype('datetime64')
        return this_roi_data