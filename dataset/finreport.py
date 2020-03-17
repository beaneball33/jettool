"""
此為整合查詢會計報表的工具，使用do_query()為整合查詢函數
此工具與jet.engine合併使用時，由於jet.engine沒有params，而是直接存在self下的名稱空間
故必須將兩邊的params同步，先將self下的名稱空間__dict__以finreport.set_params()存入到finreport.params
執行完finreport任何會改變params的函式後，以jet.engin.set_params()
將finreport.params回存到self下的名稱空間__dict__
"""

import tejapi
import pandas
import numpy
from .. import params
api_key=''	

def set_params(new_params):
    for param in new_params:
        #只取用新dict中是普通參數的部分
        if '__' not in param and not callable(new_params.get(param)):   
            #只更新兩者共通參數
            if params.__dict__.get(param) is not None:
                params.__dict__[param] = new_params.get(param)
           
def inital_report(*,code_table:str = 'TWN/AIACC',
                    actvie_code_table:str = 'TWN/AINVFACC_INFO_C'):

    if params.accountData is 'na':
        tejapi.ApiConfig.api_key = api_key
        params.accountData = tejapi.get(code_table)
        params.activeAccountData = tejapi.get(actvie_code_table)
        params.accountData['cname'] = params.accountData['cname'].str.replace('(','（').replace(')','）')
        params.accountData = params.accountData.sort_values(by=['cname'])
        params.accountData = params.accountData.drop_duplicates(subset=['code'],keep='last')

def get_by_word(keyword:str = '損益',*,
                active_view:bool = False):

    inital_report()
    if active_view is False:
        ans = params.accountData.loc[params.accountData['cname'].str.contains(keyword),'cname'].reset_index(drop=True)
    else:
        ans = params.activeAccountData.loc[params.activeAccountData['cdesc'].str.contains(keyword),'cdesc'].reset_index(drop=True)
    return ans
def get_by_cgrp(cgrp:list = ['損益表'],*,active_view:bool = False):

    inital_report()
    if active_view is False:
        ans = params.accountData.loc[params.accountData['cgrp'].isin(cgrp),'cname'].reset_index(drop=True)
    else:
        cgrp_list = {'非經常性損益':'X','比率':'R','損益表':'I','現金流量表':'C','資產負債表':'B','比率計算':'Z'}
        cgrp_code = [cgrp_list.get(cgrp[k]) for k in range(0,len(cgrp))]
        ans = params.activeAccountData.loc[params.activeAccountData['acct_type'].isin(cgrp_code),'cdesc'].reset_index(drop=True)
    return ans
    
def get_acc_code(acc_name:list = [],*,active_view:bool = False):

    tejapi.ApiConfig.api_key = api_key

    inital_report()
    ans = []
    acc_name_set = set(acc_name)
    acc_name = list(acc_name_set)
    if len(acc_name)>0:
        for cname in acc_name:
            if active_view is False:
                params.accountData['target'] = cname
                this_code = params.accountData.loc[params.accountData['cname']==params.accountData['target'],'code']
            else:
                params.activeAccountData['target'] = cname
                this_code = params.activeAccountData.loc[params.activeAccountData['cdesc']==params.activeAccountData['target'],'acct_code']                    
            if len(this_code)>0:
                ans += [this_code.values[0]]
            else:
                if '兌' in cname and '匯率' in cname:
                    cname = cname.replace('匯率','').replace('兌','@')
                    ans += [cname]
                else:
                    print(cname+':not find')
        return ans
def get_announce(*,table_id:str = None,query_coid:list = [],sample_dates:list = []):

    tejapi.ApiConfig.api_key = api_key
    if table_id is None:
        table_id = params.announceTable
    if len(query_coid) == 0:
        query_coid = params.input_coids
    if len(query_coid)==0 or len(sample_dates)!=2:
        return None
    datastart_date = sample_dates[0]
    current_zdate = sample_dates[1]
    fin_cover = tejapi.get(table_id,
                           coid=query_coid,
                           a0003={'gte':datastart_date,'lte':current_zdate},
                           opts={'sort':'mdate.desc',
                                 'columns':['coid','mdate','a0003']
                                 },paginate=True).rename(
                           index=str, columns={"a0003": "zdate"})
    return   fin_cover
def get_report(*,query_code:list = [],query_coid:list = [],
                 sample_dates:list = [],rename_cols:bool= True):
    params.acc_code = query_code

    tejapi.ApiConfig.api_key = api_key

    if len(query_coid) == 0:
        query_coid = params.input_coids
    all_fin_data = None
    if (len(query_coid)==0 or 
        len(query_code)==0 or
        len(sample_dates)!=2):
        return None
    #嘗試查詢報表封面

    fin_cover = get_announce(query_coid=query_coid,
                             sample_dates=sample_dates)
    fin_data = fin_cover.copy()            
    actual_ciod = fin_data['coid'].unique().tolist()
    datastart_date = sample_dates[0]
    current_zdate = sample_dates[1]
    if len(fin_data)>0:
        #如果有查到，代表是這個表
        #查詢報表
        query_column = ['coid','mdate'] + params.acc_code

        fin_report  = tejapi.get(params.findataTable,coid=actual_ciod,
            mdate={'gte':datastart_date,'lte':current_zdate},
            opts={"sort":"mdate.desc",'columns':query_column,"pivot":True},paginate=True)

        fin_data = fin_data.merge(fin_report,on=['coid','mdate'],how='left').fillna(0)
        fin_data['mdate'] =  fin_data['mdate'].astype(str).astype('datetime64')
        fin_data['zdate'] =  fin_data['zdate'].astype(str).astype('datetime64')
        #處理調整合理的公告日，檢查用
        fin_data['ndate'] = fin_data['mdate'] + pandas.DateOffset(months=6)

    if rename_cols is True:
        for this_code in query_code:
            accountType = params.accountData.loc[params.accountData['code']==this_code,'cgrp'].values[0]
            accountCname = params.accountData.loc[params.accountData['code']==this_code,'cname'].values[0]
            fin_data[this_code] = fin_data[this_code].fillna(0)
            fin_data = fin_data.rename(index=str, columns={this_code: accountCname})
            params.indicator_attr[this_code] = {'name':accountCname,'frequency':4}
         #全部查完 開始合併
    print('成功查詢會計家數:'+str(len(fin_data['coid'].unique())))
    return  fin_data        

def get_active_report(*,query_code:list = [],query_coid:list = [],
                        sample_dates:list = []):
    params.acc_code = query_code

    tejapi.ApiConfig.api_key = api_key
    if len(query_coid) == 0:
        query_coid = params.input_coids.copy()
    query_column = ['coid','mdate','fin_od']+params.acc_code

    if len(query_coid)==0 or len(query_code)==0 or len(sample_dates)!=2:
        return None

    datastart_date = sample_dates[0]
    current_zdate = sample_dates[1]

    findata_cover = tejapi.get(params.activeAnnounceTable,
                               coid=query_coid,
                               a_dd={'gte':datastart_date,'lte':current_zdate},
                               opts={'columns':['coid','mdate','fin_od','a_dd']},paginate=True
                               ).rename(index=str, columns={"a_dd": "zdate"})
            
    params.active_list = findata_cover['coid'].unique().tolist()
        
    findata_all = tejapi.get(params.activeFindataTable,coid=params.active_list,
                  mdate={'gte':findata_cover['mdate'].min(),'lte':findata_cover['mdate'].max()},
                  opts={'columns':query_column,"pivot":True}, paginate=True)
            
    findata_all = findata_cover.merge(findata_all,on=['coid','mdate','fin_od'],how='left')
    print('成功查詢重編家數：'+str(len(params.active_list)))
        
    #金融業因為無版本別，直接以統一版處理
    query_transfet_list = []
    new_code_list = []
    for i in range(0,len(params.transfer_acccode_list)):
        if params.transfer_acccode_list[i]['new_acc_code'] in params.acc_code:
            query_transfet_list = query_transfet_list+[params.transfer_acccode_list[i]['acc_code'] ]
            new_code_list = new_code_list+[params.transfer_acccode_list[i]['new_acc_code']]
    
    #改為以query_coid與actual_ciod做補集
    not_active_list = numpy.setdiff1d(params.input_coids, params.active_list).tolist()
    print('需檢查非重編家數：'+str(len(not_active_list)))
    if len(params.not_active_list)>0 and len(query_transfet_list)>0:
        findata_not_active = get_report(query_code=query_transfet_list,
                                        query_coid=params.not_active_list,
                                        sample_dates=sample_dates,
                                        rename_cols=False)
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
    params.acc_code_name = []
    for this_code in params.acc_code:
        accountType = params.activeAccountData.loc[params.activeAccountData['acct_code']==this_code,'acct_type'].values[0]
        accountCname = params.activeAccountData.loc[params.activeAccountData['acct_code']==this_code,'cdesc'].values[0]
        findata_all[this_code] = findata_all[this_code].fillna(0)
        findata_all = findata_all.rename(index=str, columns={this_code: accountCname})
        params.indicator_attr[this_code] = {'name':accountCname,'frequency':4}
        params.acc_code_name = params.acc_code_name + [accountCname]
    print('成功查詢會計家數:'+str(len(findata_all['coid'].unique())))
    return findata_all
        
def do_query(*,query_code:list = [],query_coid:list = [],sample_dates:list = [],
               query_length:int = 365,active_view:bool = False):
    acc_code_set = set(query_code)
    query_code = list(acc_code_set)
    params.acc_code = query_code
        
    params.query_length = query_length
    if len(query_coid) == 0:
        query_coid = params.input_coids.copy()

        #查詢績效指標報酬率    
    if len(query_coid)==0 or len(query_code)==0 or len(sample_dates)!=2:
        return None

            #先查詢財報公告日，要已公告日為主要日期
            #查詢季報
    if active_view is False:
        params.active_view = False
        findata_all = get_report(query_code=params.acc_code,
                                 query_coid=query_coid,
                                 sample_dates=sample_dates)                
    else:
        params.active_view = True
        findata_all = get_active_report(query_code=params.acc_code,
                                        query_coid=query_coid,
                                        sample_dates=sample_dates)
    findata_all['mdate'] = findata_all['mdate'].astype(str)
    all_mdate_list = findata_all['mdate'].unique().astype('datetime64')
    params.all_mdate_list = numpy.sort(all_mdate_list)
    params.current_mdate = all_mdate_list[len(all_mdate_list)-1] 

    return findata_all