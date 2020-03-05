from . import querybase
import pandas
import numpy
from .. import params

class financial_report(querybase.query_base):
    """
    本方法用來管理財報資料專屬查詢工具
    """
    def __init__(self):
        self.accountData = None
        self.activeAccountData  = None	
            
    def inital_report(self):
        self.accountData = self.tejapi.get('TWN/AIACC')
        self.activeAccountData = self.tejapi.get('TWN/AINVFACC_INFO_C')
        self.accountData['cname'] = self.accountData['cname'].str.replace('(','（').replace(')','）')
        self.accountData = self.accountData.sort_values(by=['cname'])
        self.accountData = self.accountData.drop_duplicates(subset=['code'],keep='last')
    def get_acc_code(self,acc_name=[],active_view=False):
        ans = []
        acc_name_set = set(acc_name)
        acc_name = list(acc_name_set)
        if len(acc_name)>0:
            for cname in acc_name:
                if active_view is False:
                    self.accountData['target'] = cname
                    this_code = self.accountData.loc[self.accountData['cname']==self.accountData['target'],'code']
                else:
                    self.activeAccountData['target'] = cname
                    this_code = self.activeAccountData.loc[self.activeAccountData['cdesc']==self.activeAccountData['target'],'acct_code']                    
                if len(this_code)>0:
                    ans += [this_code.values[0]]
                else:
                    if '兌' in cname and '匯率' in cname:
                        cname = cname.replace('匯率','').replace('兌','@')
                        ans += [cname]
                    else:
                        print(cname+':not find')
        return ans
    def get_announce(self,query_coid=None,target_name=''):
        if len(query_coid)>0:
            #嘗試查詢報表封面
            table_name = self.market+'/'+target_name
            fin_data = self.tejapi.get(table_name,
                coid=query_coid,a0003={'gte':self.datastart_date,'lte':self.current_zdate},
                opts={"sort":"mdate.desc",'columns':['coid','mdate','a0003']},paginate=True).rename(
                index=str, columns={"a0003": "zdate"})
        return   fin_data
    def get_report(self,query_code,query_coid=None,rename_cols=True):
        self.acc_code = query_code
        target_name = self.account_table.get(self.market)
        if query_coid is None:
            query_coid = self.input_coids
        all_fin_data = None
        if len(query_coid)>0:
            #嘗試查詢報表封面
            self.fin_cover = self.get_announce(query_coid,target_name['cover'])
            fin_data = self.fin_cover.copy()            
            actual_ciod = fin_data['coid'].unique().tolist()
            if len(fin_data)>0:
                #如果有查到，代表是這個表
                #查詢報表
                query_column = ['coid','mdate'] + query_code
                table_name = self.market+'/'+target_name['data']
                fin_report  = self.tejapi.get(table_name,coid=actual_ciod,
                    mdate={'gte':self.datastart_date,'lte':self.current_zdate},
                    opts={"sort":"mdate.desc",'columns':query_column,"pivot":True},paginate=True)

                fin_data = fin_data.merge(fin_report,on=['coid','mdate'],how='left').fillna(0)
                fin_data['mdate'] =  fin_data['mdate'].astype(str).astype('datetime64')
                fin_data['zdate'] =  fin_data['zdate'].astype(str).astype('datetime64')
                #處理調整合理的公告日，檢查用
                fin_data['ndate'] = fin_data['mdate'] + pandas.DateOffset(months=6)

        if rename_cols is True:
            for this_code in query_code:
                accountType = self.accountData.loc[self.accountData['code']==this_code,'cgrp'].values[0]
                accountCname = self.accountData.loc[self.accountData['code']==this_code,'cname'].values[0]
                fin_data[this_code] = fin_data[this_code].fillna(0)
                fin_data = fin_data.rename(index=str, columns={this_code: accountCname})
                self.indicator_attr[this_code] = {'name':accountCname,'frequency':4}
             #全部查完 開始合併
        print('成功查詢會計家數:'+str(len(fin_data['coid'].unique())))
        return  fin_data        

    def get_active_report(self,query_code,query_coid=None):
        self.acc_code = query_code

        if query_coid is None:
            query_coid = self.input_coids.copy()
        query_column = ['coid','mdate','fin_od']+query_code

        findata_cover = self.tejapi.get('TWN/AINVFINQA',coid=query_coid,
            a_dd={'gte':self.datastart_date,'lte':self.current_zdate},
            opts={'columns':['coid','mdate','fin_od','a_dd']}, paginate=True).rename(
            index=str, columns={"a_dd": "zdate"})
            
        actual_ciod = findata_cover['coid'].unique().tolist()
        
        findata_all = self.tejapi.get('TWN/AINVFINQ',coid=actual_ciod,
            mdate={'gte':findata_cover['mdate'].min(),'lte':findata_cover['mdate'].max()},
            opts={'columns':query_column,"pivot":True}, paginate=True)
            
        findata_all = findata_cover.merge(findata_all,on=['coid','mdate','fin_od'],how='left')
        print('成功查詢重編家數：'+str(len(findata_all['coid'].unique())))
        
        #金融業因為無版本別，直接以統一版處理

        query_transfet_list = []
        new_code_list = []
        for i in range(0,len(self.transfer_acccode_list)):
            if self.transfer_acccode_list[i]['new_acc_code'] in query_code:
                query_transfet_list = query_transfet_list+[self.transfer_acccode_list[i]['acc_code'] ]
                new_code_list = new_code_list+[self.transfer_acccode_list[i]['new_acc_code']]
                
        not_active_list = self.basic_info.loc[self.basic_info['coid'].isin(actual_ciod)==False,'coid'].values.tolist()
        
        if len(not_active_list)>0 and len(query_transfet_list)>0:
            findata_not_active = self.get_report(query_code=query_transfet_list,
                query_coid=not_active_list,rename_cols=False)

            findata_not_active = findata_not_active.fillna(0)
            for i in range(0,len(query_transfet_list)):
                findata_not_active = findata_all.rename(index=str, columns={query_transfet_list[i]: new_code_list[i]})
                findata_not_active['fin_od'] = 1
            findata_all = findata_all.append(findata_not_active,sort=False)
        #型別處理
        findata_all['semester'] = (findata_all['mdate'].dt.strftime('%m').astype(int)/3).astype(int)
        findata_all['zdate'] =  findata_all['zdate'].astype(str).astype('datetime64')
        findata_all['mdate'] =   findata_all['mdate'].dt.strftime('%Y-%m-%d').astype('datetime64')
        #排序方便合併
        findata_all = findata_all.sort_values(by=['coid','mdate','fin_od'])
        self.acc_code_name = []
        for this_code in query_code:
            accountType = self.activeAccountData.loc[self.activeAccountData['acct_code']==this_code,'acct_type'].values[0]
            accountCname = self.activeAccountData.loc[self.activeAccountData['acct_code']==this_code,'cdesc'].values[0]
            findata_all[this_code] = findata_all[this_code].fillna(0)
            findata_all = findata_all.rename(index=str, columns={this_code: accountCname})
            self.indicator_attr[this_code] = {'name':accountCname,'frequency':4}
            self.acc_code_name = self.acc_code_name + [accountCname]
        print('成功查詢會計家數:'+str(len(findata_all['coid'].unique())))
        return findata_all
        
    def do_query(self,query_code,query_length = 365,active_view=False):
        acc_code_set = set(query_code)
        query_code = list(acc_code_set)
        fx_list = []
        for codes in query_code:
            if '@' in codes:
                fx_list = fx_list + [codes]
                query_code.remove(codes)

        self.query_length = query_length


        #查詢績效指標報酬率    
        if len(self.input_coids)>0:
            #先查詢財報公告日，要已公告日為主要日期
            #查詢季報
            if active_view is False:
                self.active_view = False
                self.findata_all = self.get_report(query_code=query_code)                
            else:
                self.active_view = True
                self.findata_all = self.get_active_report(query_code=query_code)
            
            self.all_mdate_list = numpy.sort(self.findata_all['mdate'].astype(str).unique().astype('datetime64')) 
            self.current_mdate = self.all_mdate_list[len(self.all_mdate_list)-1] 
        self.fxrate_attr = {}
        if len(fx_list)>0:
            print('查詢匯率')
            self.rate_data = self.get_fxrate(fx_list,query_length)
    def get_by_word(self,keyword='損益',active_view=False):
        if active_view is False:
            ans = self.accountData.loc[self.accountData['cname'].str.contains(keyword),'cname'].reset_index(drop=True)
        else:
            ans = self.activeAccountData.loc[self.activeAccountData['cdesc'].str.contains(keyword),'cdesc'].reset_index(drop=True)
        return ans
    def get_by_cgrp(self,cgrp=['損益表'],active_view=False):

        if active_view is False:
            ans = self.accountData.loc[self.accountData['cgrp'].isin(cgrp),'cname'].reset_index(drop=True)
        else:
            cgrp_list = {'非經常性損益':'X','比率':'R','損益表':'I','現金流量表':'C','資產負債表':'B','比率計算':'Z'}
            cgrp_code = [cgrp_list.get(cgrp[k]) for k in range(0,len(cgrp))]
            ans = self.activeAccountData.loc[self.activeAccountData['acct_type'].isin(cgrp_code),'cdesc'].reset_index(drop=True)
        return ans
