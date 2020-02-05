import pandas
import numpy
from datetime import datetime, date, timedelta
import time 
import traceback
class method_base(object):

    def calculate_maxdrawback(self,window=None,col_type=True,df=None):
        if df is None:
            self.portfolio_pv = numpy.nan_to_num(self.simple_roi_data.loc[self.simple_roi_data['pname']=='portfolio','present value'].values)
            df = numpy.array(self.portfolio_pv)
        #最大回撤率
        i = numpy.argmax((numpy.maximum.accumulate(df) - df) / numpy.maximum.accumulate(df))  # 結束位置
        if i == 0:
            return 0
        j = numpy.argmax(df[:i])  # 開始位置
        return ((df[j] - df[i]) / (df[j]))
    def reset_strategy(self,reset_date=None):
        print('reset')
        self.famamacbeth_outcome = None
    def fama_macbeth_test(self,this_date_data,alpha_rate,y_name,x_names):
        if this_date_data is not None:
            beta={}
            beta_=[]
            betalist=[]
            famadatelist=this_date_data['zdate'].unique().tolist()
            trials = 0
            while trials < 10:
                trials+=1
                betalist=[]
                parameter_nums = len(x_names)+1
                for i in range(0,len(famadatelist)):

                    sub_df=this_date_data.loc[this_date_data['zdate'].isin([famadatelist[i]]),:]
                    x_sample = sub_df[x_names]
                    y_sample = sub_df[y_name].to_numpy()

                    x_sample = sm.add_constant(x_sample.to_numpy())
                    model = sm.OLS(y_sample, x_sample).fit()
                    print_model = model.summary()
                    beta[i]=list(model.params)
                    betalist+=beta[i]

                betagroup=[betalist[i:i + parameter_nums] for i in range(0, len(betalist), parameter_nums )]
                t_test=scipy.stats.ttest_1samp(betagroup,0)
                mean=pandas.DataFrame(pandas.DataFrame(numpy.array([betalist[i:i + parameter_nums] for i in range(0, len(betalist), parameter_nums)])).mean()).round(decimals=16).T
                pvalue=pandas.DataFrame(t_test[1]).round(decimals=4).T
                t_test_result=pandas.concat([mean,pvalue])
                t_test_result.pop(0)
                t_test_result.columns=x_names
                t_test_result.index=['coef','p-value']
                #根據p-value排序以便後面pop
                p_df=pandas.DataFrame(t_test_result.T['p-value'].sort_values())
                p_df_over_alpha=p_df[p_df['p-value']>(alpha_rate/100)]
                no_significant_number=len(p_df_over_alpha)
                if no_significant_number>0:
                    index=list(p_df.index)
                    variable=index.pop()
                    x_names.remove(variable)
                    print('removing variable:'+variable)
                elif len(x_names) == 0:
                    #讓系統跑出famamacbeth_outcome給後面方法用 但是卻是空的
                    print('no effective variable')
                    t_test_result = None
                    break
                else:
                    print('complete')
                    break
            return t_test_result
        else:
            return None
    def ranking(self,direction,factor,portfolio_data,rank_above=10):
        sub_df = portfolio_data.copy()
        if direction is None or factor is None:
            result_df = sub_df
            result_df['temp'] = numpy.nan
        else:
            score_columns = []
            for i in range(len(factor)):
                sub_df[factor[i]+'_得分']=sub_df[factor[i]]*direction[i]
                score_columns = score_columns + [factor[i]+'_得分']
            if '市值' not in score_columns:
                score_columns = score_columns + ['市值']
            rank_data=sub_df.groupby('coid')[score_columns].mean().rank()
            filter=rank_data[rank_data['市值']>0].drop(columns=['市值'])
            result=filter.sum(axis=1).sort_values().rank(method='max')[:int(len(filter)*rank_above*0.01)+1]
            result_df = pandas.DataFrame(result).rename(columns={0: 'temp'})
        return result_df
    def make_famamacbethmodel(self,col_name='報酬率',check_index=['市值','現金股利率'],window='12m',alpha_rate=5,reset_list='01',keep=None,target_name=None,peer_future=False):
        t1 = time.time()
        ans_val = numpy.nan
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            return ans
        actual_reset_list = self.manage_resetlist(reset_list)
        self.retrain_model[target_name] = self.check_resetdate(actual_reset_list)
        if self.retrain_model.get(target_name) is True:
            #若famamacbeth_outcome沒有順利產生或不存在
            print('重新進行fama macbeth估計')
            input_col_name = [col_name]+check_index
            this_date_data, this_window_type, window = self.get_activedate_data(window,clue_length=0,column_names=input_col_name,peer_future=peer_future)
            this_date_data = this_date_data.dropna(subset=input_col_name)
            this_date_data['temp_date'] = pandas.to_datetime(this_date_data['zdate']).dt.year*100+ pandas.to_datetime(this_date_data['zdate']).dt.month
            if keep is not None:
                keep_method = ['last','first']
                if keep not in keep_method:
                    keep = 'first'
                this_date_data = this_date_data.drop_duplicates(subset=['coid','temp_date'],keep=keep).reset_index()
            if len(this_date_data)>=len(self.input_coids)*window:
                t_test_result = self.fama_macbeth_test(this_date_data,alpha_rate,y_name=col_name,x_names=check_index)
                if t_test_result is None:
                    print('fama_macbeth_test無有效因子')
                else:
                    self.trained_model[target_name] = t_test_result
                    print(t_test_result)
                    print('樣本數'+str(len(this_date_data))+'/'+str(len(self.input_coids)*window)+' 完成估計 因子數:'+str(len(t_test_result.columns)))
                    fama_factors_coef = str(t_test_result.loc['coef',:].values[0])
                    fama_factors_name = str(t_test_result.columns.tolist()[0])
                    for name_index in range(1,len(t_test_result.columns)):
                        fama_factors_coef = fama_factors_coef+'_'+str(t_test_result.loc['coef',:].values[name_index])
                        fama_factors_name = fama_factors_name+'_'+str(t_test_result.columns.tolist()[name_index])
                    ans_val = fama_factors_name+'#'+fama_factors_coef
            else:
                print('樣本數'+str(len(this_date_data))+'/'+str(len(self.input_coids)*window)+' fama_macbeth_test樣本數不足')
        all_coid_data = self.current_coids.copy()
        all_coid_data['zdate'] = self.current_zdate
        all_coid_data['temp'] = ans_val
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        return ans
    def run_famascore(self,col_name=None,rank_above=100,class_count=1,target_name=None):
        t1 = time.time()
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            return ans
        if self.trained_model.get(col_name) is None:
            factor = None
            direction = None
        else:
            famamacbeth_outcome = self.trained_model.get(col_name)
            factor = famamacbeth_outcome.columns.tolist()
            actual_direction = famamacbeth_outcome.loc['coef',:].values
            direction = numpy.abs(actual_direction)/actual_direction
        result_df = self.ranking(direction,factor,self.data[self.data['zdate']==self.current_zdate],rank_above).reset_index(drop=False)
        if self.trained_model.get(col_name) is not None and class_count>1:
            class_interval = int(numpy.floor(result_df['temp'].max()/class_count))
            result_df['temp'] = result_df['temp']
            result_df['temp'] = numpy.ceil(result_df['temp']/class_interval).astype(int)
            result_df.loc[result_df['temp']==0,'temp']  = 1
            result_df = result_df.rename(columns={'temp': col_name})
            result_df = self.moving_ranking(result_df,check_index='coid',group_name=col_name,coid_num=len(result_df['coid']),max_group=class_count,ascending=True)
            self.result_df =  result_df
        all_coid_data = result_df.fillna(0)
        all_coid_data['zdate'] = self.current_zdate
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        return ans
    def moving_ranking(self,portfolio_data,check_index,group_name,coid_num,max_group,ascending=True):
        result_df = portfolio_data
        group_list = result_df[group_name].sort_values(ascending=ascending).unique().tolist()
        unit_num = coid_num/max_group
        this_group_index = 1
        accum_num = 0
        for i in range(0,len(group_list)-1):
            group_val = group_list[i]
            result_df.loc[result_df[group_name]==group_list[i],'temp'] = this_group_index
            next_length = len(result_df.loc[result_df[group_name]==group_list[i+1],check_index].values.tolist())
            this_length = len(result_df.loc[result_df[group_name]==group_list[i],check_index].values.tolist())
            accum_num+=this_length
            if this_group_index<max_group:
                if accum_num>=unit_num :
                    accum_num = 0
                    this_group_index+=1
                elif accum_num<unit_num and next_length+accum_num>=unit_num:
                    accum_num = 0
                    this_group_index+=1
        result_df.loc[result_df[group_name]==group_list[len(group_list)-1],'temp'] = max_group
        return result_df.loc[:,[check_index,'temp']]
    def check_resetdate(self,actual_reset_list):
        #檢查，若重置日期符合實際交易日，且需與該交易日相同
        ans = False
        for d_i in range(0,len(actual_reset_list)):
            #print([pandas.to_datetime(self.current_zdate).strftime('%Y-%m-%d'),actual_reset_list[d_i],reset_list_y[d_i]])
            if pandas.to_datetime(self.current_zdate).strftime('%Y-%m-%d') == actual_reset_list[d_i] and pandas.to_datetime(self.current_zdate)>=pandas.to_datetime(actual_reset_list[d_i]):
                self.famamacbeth_outcome = None
                ans = True
                break
            else:
                ans = False
        return ans
    def manage_resetlist(self,reset_list):
        reset_list_y = []
        if type(reset_list).__name__ == 'str':   
            reset_list = [reset_list]
        for reset_date in reset_list:
            reset_list_split = reset_date.split('-')
            if len(reset_list_split)>2:
                reset_list_y = reset_list_y+ [reset_list]                
            else:
                if len(reset_list_split)>1:
                    list_add_month = [reset_date]
                else:
                    month_list=['01','02','03','04','05','06','07','08','09','10','11','12']
                    list_add_month = [month_list[d_i]+'-'+reset_date for d_i in range(0,len(month_list))]        
                reset_list_y = reset_list_y+ [str(pandas.to_datetime(self.current_zdate).year)+'-'+list_add_month[d_i] for d_i in range(0,len(list_add_month))]
        reset_list_y = list(set(reset_list_y))
        actual_reset_list = [self.cal_zdate(base_date=self.check_available_date(reset_list_y[d_i]),jump_length=-1,jump_kind='D') for d_i in range(0,len(reset_list_y))]        
        return actual_reset_list  
    def abnormal_selection(self,group_data=None,check_index='報酬率',group_name='TEJ子產業名',check_type=True,method='positive',window='12m',alpha_rate=5,keep=None,reset_list=None):
        run_this_date = True
        if reset_list is not None:
            actual_reset_list = self.manage_resetlist(reset_list)
            run_this_date = self.check_resetdate(actual_reset_list)
        if run_this_date is True:
            score_columns = check_index
            if group_data is None:
                input_col_name=[group_name,check_index]
                this_date_data , this_window_type, window = self.get_activedate_data(window=window,clue_length=0,column_names=input_col_name)
            else:
                this_date_data = group_data
                window = len(group_data['zdate'].unique())
            group_data = this_date_data.copy()
            group_data[group_name] = group_data[group_name].fillna(method='bfill')
            group_data[score_columns] = group_data[score_columns].fillna(0)
            group_data['temp_date'] = pandas.to_datetime(group_data['zdate']).dt.year*100+ pandas.to_datetime(group_data['zdate']).dt.month
            if keep is not None:
                keep_method = ['last','first']
                if keep not in keep_method:
                    keep = 'first'
                group_data = group_data.drop_duplicates(subset=['temp_date','coid'],keep=keep).reset_index(drop=True)
            if len(group_data['zdate'].unique())<window:
                ans = self.return_previous_holding()
                print('abnormal return test fail:lack sample')
            else:
                group_data = group_data.sort_values(by=['zdate',group_name]).reset_index(drop=True)
                group_data = group_data.drop_duplicates(subset=[group_name,'zdate'],keep='last').reset_index(drop=True)
                all_group_name = group_data[group_name].unique().tolist()
                all_group_data = [group_data.loc[group_data[group_name]==all_group_name[g_i],score_columns].values.tolist() for g_i in range(0,len(all_group_name))]
                group_val = []
                for i in range(0,len(all_group_name)):
                    check_ok = False
                    if len(all_group_data[i])>1:
                        group_check = scipy.stats.ttest_1samp(all_group_data[i],popmean=0,axis=0)
                        if group_check[1]<alpha_rate/100:
                            check_ok = True if check_type is True else False
                        else:
                            check_ok = False if check_type is True else True
                        if numpy.mean(all_group_data[i])>0:
                            check_ok = True if method=='positive' else False
                        else:
                            check_ok = True if method=='positive' else False
                    group_val = group_val+[check_ok]
                group_val = numpy.array(group_val)
                group_val = pandas.DataFrame(group_val.astype(numpy.bool),columns=['temp'])
                group_val[group_name] = numpy.array(all_group_name)
                group_val =self.data.loc[:,['coid',group_name]].merge(group_val,on=[group_name],how='left')
                self.result_df = group_val
                ans = group_val['temp'].values
        else:
            ans = self.return_previous_holding()
        return ans
    def group_selection(self,group_data=None,check_index='報酬率',ascending=False,group_name='TEJ子產業名',window='12m',keep=None,choose_above=[0,10],reset_list=None):
        if group_data is None:
            input_col_name=[group_name,check_index]
            group_data , this_window_type, window = self.get_activedate_data(window=window,clue_length=0,column_names=input_col_name)
            group_data['temp_date'] = pandas.to_datetime(group_data['zdate']).dt.year*100+ pandas.to_datetime(group_data['zdate']).dt.month
            if keep is not None:
                keep_method = ['last','first']
                if keep not in keep_method:
                    keep = 'first'
                group_data = group_data.drop_duplicates(subset=['coid','temp_date'],keep=keep).reset_index()
        else:
            window = len(group_data['zdate'].unique())
        run_this_date = True
        if reset_list is not None:
            actual_reset_list = self.manage_resetlist(reset_list)
            run_this_date = self.check_resetdate(actual_reset_list)
        if run_this_date is True:
            score_columns = check_index
            group_data[group_name] = group_data[group_name].fillna(method='bfill')
            group_data[score_columns] = group_data[score_columns].fillna(0)
            group_data['temp'] = group_data[score_columns].rolling(window=window).sum()
            group_data = group_data.drop_duplicates(subset=['coid'],keep='last').reset_index()
            rank_data=group_data['temp'].rank(pct=True,ascending=ascending).values
            rank_data[(rank_data<=choose_above[1]/100)&(rank_data>choose_above[0]/100)] = 1
            rank_data[rank_data<1] = 0
            rank_data = rank_data.astype(numpy.bool)
        else:
            rank_data = self.return_previous_holding()
        return rank_data
    def return_previous_holding(self):
        if len(self.hold_coids)>0:
            this_hold_list = pandas.DataFrame(self.hold_coids[0],columns=['coid'])
            this_hold_list['temp'] = True
            this_hold_data = self.data.merge(this_hold_list,on=['coid'],how='left').copy().fillna(False)
            this_hold_data = this_hold_data['temp'].values
        else:
            this_hold_data = False
        return this_hold_data
    def choose_setting(self,check_index='購入',reset_list='01'):
        run_this_date = True
        if reset_list is not None:
            actual_reset_list = self.manage_resetlist(reset_list)
            run_this_date = self.check_resetdate(actual_reset_list)
        if run_this_date is True:
            rank_data=self.data[check_index].values
        else:
            if len(self.hold_coids)>0:
                this_hold_list = pandas.DataFrame(self.hold_coids[0],columns=['coid'])
                this_hold_list['temp'] = True
                rank_data = self.data.merge(this_hold_list,on=['coid'],how='left').copy().fillna(False)
                rank_data = rank_data['temp'].values
            else:
                rank_data = False
        return rank_data
    def equal_pv(self):
        #本方法用來產生投資現值相同的持股方式
        #在持股家數不變下，維持相同持股數，異動之公司，投資現值則與其他各檔平均值相同
        this_coid = self.data.loc[(self.data['購入']==True)&(self.data['coid'].isin(self.listed_coids)),'coid'].values
        this_closed = self.data.loc[(self.data['購入']==True)&(self.data['coid'].isin(self.listed_coids)),self.closed_name].values
        self.hold_coids = [this_coid.tolist()] + self.hold_coids
        if len(self.hold_coids)>1:
            not_same = False
            for i in range(0,len(self.hold_coids[1])):
                if self.hold_coids[1][i] not in self.hold_coids[0]:
                #檢查最新一天股票名單有無改變
                    not_same = True
                    break
            #第一天以後，要進行檢查比對
            if len(self.hold_coids[0]) == len(self.hold_coids[1]) and not_same is False:

                #持股不變，則股數不變
                this_hold_units = self.hold_unit[0]
                this_coid = self.hold_coids[1]
                self.hold_unit = [this_hold_units] + self.hold_unit
            else:
                #持股改變，重調權重

                if len(this_coid)>0:
                    unit_pv = self.cash/len(this_coid)
                    this_hold_units = numpy.floor(unit_pv/this_closed)
                    self.hold_unit = [this_hold_units.tolist()]+ self.hold_unit
                else:
                    this_hold_units = []
                    self.hold_unit = [this_hold_units]+ self.hold_unit
        else:
            if len(this_coid)>0:
                #要有購入才計算
                unit_pv = self.cash/len(this_coid)
                this_hold_units = numpy.floor(unit_pv/this_closed)
                self.hold_unit = [this_hold_units.tolist()]
            else:
                this_hold_units = self.data.loc[self.data['購入']==True,'購入'].astype(float).values
                self.hold_unit = [this_hold_units.tolist()]
        temp_ans = pandas.DataFrame(this_hold_units,columns=['temp'])
        temp_ans['coid'] = this_coid
        temp_ans['zdate'] = self.current_zdate
        self.data = self.data.merge(temp_ans,on=['coid','zdate'],how='left')
        self.data['temp'] = self.data['temp'].fillna(0)
        ans = self.data['temp'].values
        self.by_unit = True
        self.data = self.data.drop(columns=['temp'])
        return ans
    def check_condition(self,conditions,check_type='or'):
        ans = None
        for condition in conditions:
            self.check_columns[condition] = []
        full = ['coid']+conditions
        this_condition = self.data.loc[:,full].fillna(False)
        this_condition = this_condition[conditions].astype(numpy.int).values
        if  'or' in check_type  :
            check_data = numpy.amax(this_condition,axis=1)
        else:
            check_data = numpy.amin(this_condition,axis=1)
        ans = check_data.astype(numpy.bool)
        return ans
    def check_between(self,check_index,up_index,down_index,window='1d',target_name=None):
        edit_cols= self.confirm_checkindex(check_index1=check_index,window=window)
        check_index = edit_cols[0]
        edit_cols= self.confirm_checkindex(check_index1=up_index,check_index2=down_index,window=window)
        up_index = edit_cols[0]
        down_index = edit_cols[1]
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            return ans
        down_data = self.check_above(check_index,down_index,window).astype(numpy.int)
        up_data = self.check_above(up_index,check_index,window).astype(numpy.int)
        all_coid_data = self.current_coids.merge(up_data,on=['coid'],how='left')
        all_coid_data = self.current_coids.merge(down_data,on=['coid'],how='left')
        all_coid_data['temp'] = all_coid_data[down_index]&all_coid_data[check_index]
        all_coid_data = all_date_data.loc[:,['coid','temp']]
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        return ans
    def check_above(self,check_index,down_index,window='1d',target_name=None):
        t1 = time.time()
        edit_cols= self.confirm_checkindex(check_index1=check_index,check_index2=down_index,window=window)
        check_index = edit_cols[0]
        down_index =  edit_cols[1]
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            return ans
        if type(check_index).__name__ != 'str':
            self.data['temp_name_up'] = check_index
            check_index = 'temp_name_up'
        if type(down_index).__name__ != 'str':
            self.data['temp_name_down'] = down_index
            down_index = 'temp_name_down'
        #all_colname = self.confirm_checkindex(check_index,down_index)
        #check_index = all_colname[0]
        #down_index = all_colname[1]
        this_date_data, this_window_type, window = self.get_activedate_data(window,clue_length=0,column_names=[check_index,down_index])
        this_date_data['tempcheck'] = this_date_data[check_index] - this_date_data[down_index]
        this_date_data.loc[this_date_data['tempcheck']<=0,'tempcheck'] = 0
        this_date_data.loc[this_date_data['tempcheck']>0,'tempcheck'] = 1
        this_date_data['temp'] = this_date_data['tempcheck'].rolling(window=window).sum()
        this_date_data['zdate'] = self.current_zdate
        this_date_data = this_date_data.loc[:,['coid','zdate','temp']].drop_duplicates(subset=['coid','zdate'],keep='last').reset_index()
        this_date_data.loc[this_date_data['temp']<window,'temp'] = 0
        this_date_data.loc[this_date_data['temp']>=window,'temp'] = 1
        this_date_data = this_date_data.loc[:,['zdate','coid','temp']]
        all_coid_data = this_date_data
        all_coid_data = all_coid_data.loc[:,['coid','temp']]
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        t2 = time.time()
        elapsed_time = t2-t1
        #print('cost'+str(elapsed_time))
        return ans
    def calculate_growthrate(self,check_index='報酬率',window='1d',fix_date=None,method='arithmetic',target_name=None,sync=True,peer_future=False):
        t1 = time.time()
        edit_cols= self.confirm_checkindex(check_index1=check_index,window=window)
        check_index = edit_cols[0]
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            t2 = time.time()
            elapsed_time = t2-t1
            return ans
        #如果不包含期初那期的變化率 則sync為false
        clue = 1 if sync is True else 0
        if fix_date is not None:
            jumps = 0 if peer_future is False else -1
            this_base_date = self.cal_zdate(jump_length=jumps,jump_kind='M',fix_date=fix_date)
            this_date_data, this_window_type, window = self.get_activedate_data(window,clue_length=clue,column_names=[check_index],peer_future=peer_future,base_date=this_base_date)
        else:
            this_date_data, this_window_type, window = self.get_activedate_data(window,clue_length=clue,column_names=[check_index],peer_future=peer_future)
        roib_list = this_date_data[check_index].values
        #預留計算控制 現在的通用法比較沒效率
        col_type = None
        if col_type is None:
            rolling_window = len(this_date_data['zdate'].unique())
            #特殊的算法 滾動計算中間每期的結果
            roib_y0 = roib_list[1:len(roib_list)]
            roib_y1 = roib_list[0:len(roib_list)-1]
            if method is 'arithmetic':
                this_date_data['temp_mid'] = [0]+((roib_y1 - roib_y0 )/numpy.abs(roib_y0)).tolist()
            elif method is 'geometric':
                this_date_data['temp_mid'] = [0] + numpy.log(roib_y1/roib_y0).tolist()
            this_date_data['temp'] = this_date_data['temp_mid'].rolling(window=(rolling_window-1)).sum()
            if col_type is 'mean':
                this_date_data['temp'] = this_date_data['temp'] / (rolling_window-1)
            #print(this_date_data[this_date_data['coid']=='2881'])
            all_date_data = this_date_data.drop_duplicates(subset=['coid'],keep='last').reset_index()
        all_date_data['zdate'] = self.current_zdate
        all_coid_data = all_date_data.loc[:,['coid','temp']]
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        t2 = time.time()
        elapsed_time = t2-t1
       #print('cost'+str(elapsed_time))
        return ans
    def calculate_volatility(self,check_index='報酬率',window='3M',col_type='SMA',target_name=None):
        t1 = time.time()
        edit_cols= self.confirm_checkindex(check_index1=check_index,window=window)
        check_index = edit_cols[0]
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            return ans
        peer_future = False
        this_date_data, this_window_type, window = self.get_activedate_data(window,clue_length=0,column_names=[check_index],peer_future=peer_future)
        this_coid_list = this_date_data['coid'].unique().tolist()
        roib_list = [this_date_data.loc[this_date_data['coid']==this_coid_list[i],check_index].values.tolist() for i in range(0,len(this_coid_list))]
        roib_list = numpy.array(roib_list)
        roib_std = numpy.std(roib_list,axis=1)
        all_date_data = pandas.DataFrame(this_coid_list,columns=['coid'])
        all_date_data['zdate'] = self.current_zdate
        all_date_data['temp'] = numpy.sqrt(252)*roib_std/100
        all_coid_data = all_date_data.loc[:,['coid','temp']]
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        t2 = time.time()
        elapsed_time = t2-t1
       #print('cost'+str(elapsed_time))
        return ans
    def sort_crossing(self,check_index,window='1d',col_type='average',method='rank',class_interval=None,category=None,ascending=True,target_name=None):
        t1 = time.time()
        if class_interval is not None :
            method = 'rank'
        edit_cols= self.confirm_checkindex(check_index1=check_index,window=window)
        check_index = edit_cols[0]
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            return ans
        input_col_name = [check_index]
        this_date_data, this_window_type, window = self.get_activedate_data(window,column_names=input_col_name)
        all_date_data = pandas.DataFrame(columns=['zdate','coid','temp'])
        #這邊要加上分類清單產生
        if category is None:
            check_coid_category_list = [self.input_coids]
        else:
            current_zdate_data = self.data.loc[self.data['zdate']==self.current_zdate,['coid',category]]
            all_category_list = current_zdate_data[category].unique().tolist()
            check_coid_category_list = [ current_zdate_data.loc[(current_zdate_data[category]==all_category_list[inds]),'coid'].unique().tolist() for inds in range(0,len(all_category_list))]
        for i in range(0,len(check_coid_category_list)):
            coid_category_list = check_coid_category_list[i]
            this_coid_data = this_date_data.loc[(this_date_data['coid'].isin(coid_category_list)),:]

            this_coid_data['temp_val'] = this_coid_data[check_index].rolling(window=window).sum()

            this_coid_data['zdate'] = self.current_zdate
            this_coid_data = this_coid_data.loc[:,['coid','zdate','temp_val']].drop_duplicates(subset=['coid','zdate'],keep='last')
            if method is 'percentile':
                this_coid_data['temp'] = this_coid_data['temp_val'].rank(pct=True,ascending=ascending)
            else:
                this_coid_data['temp'] = this_coid_data['temp_val'].rank(pct=False,ascending=ascending)
            all_date_data = all_date_data.append(this_coid_data,sort=False)
        all_coid_data = all_date_data.loc[:,['coid','temp']]
        if class_interval is not None:
            all_coid_data['temp'] = numpy.ceil(all_coid_data['temp']/class_interval).astype(int)
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        t2 = time.time()
        elapsed_time = t2-t1
        #print('cost'+str(elapsed_time))
        return ans
    def calculate_crossing(self,check_index,window='1d',col_kind='average',category=None,weight=None,target_name=None):
        t1 = time.time()
        edit_cols= self.confirm_checkindex(check_index1=check_index,window=window)
        check_index = edit_cols[0]
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            return ans
        input_col_name = [check_index]
        if weight is not None:
            if weight!=check_index:
                input_col_name = input_col_name + [weight]

        this_date_data, this_window_type, window = self.get_activedate_data(window,column_names=input_col_name)
        all_coid_data = pandas.DataFrame(columns=['zdate','coid','temp'])
        #print(this_date_data[this_date_data['coid']=='2330'])
        #這邊要加上分類清單產生
        if category is None:
            check_coid_category_list = [self.input_coids]
        else:
            current_zdate_data = self.data.loc[self.data['zdate']==self.current_zdate,['coid',category]]
            all_category_list = current_zdate_data[category].unique().tolist()
            check_coid_category_list = [ current_zdate_data.loc[(current_zdate_data[category]==all_category_list[inds]),'coid'].unique().tolist() for inds in range(0,len(all_category_list))]
        for i in range(0,len(check_coid_category_list)):
            coid_category_list = check_coid_category_list[i]
            this_coid_data = this_date_data.loc[(this_date_data['coid'].isin(coid_category_list)),:].reset_index(drop=True)
            if col_kind == 'median':
                this_coid_data['temp'] = this_coid_data[check_index].median()
            elif col_kind == 'max':
                this_coid_data['temp'] = this_coid_data[check_index].max()
            elif col_kind == 'min':
                this_coid_data['temp'] = this_coid_data[check_index].min()
            else:
                this_coid_data[check_index] = this_coid_data[check_index].fillna(this_coid_data[check_index].mean())
                if weight is not None:
                    this_coid_data[weight] = this_coid_data[weight].fillna(this_coid_data[weight].mean())
                    this_coid_data['temp_with_weight'] = this_coid_data[check_index]*this_coid_data[weight]/this_coid_data[weight].sum()
                else:
                    this_coid_data['temp_with_weight'] = this_coid_data[check_index]/len(this_coid_data[check_index])
                this_coid_data['temp'] = this_coid_data['temp_with_weight'].sum()
            this_coid_data['zdate'] = self.current_zdate
            this_coid_data = this_coid_data.loc[:,['coid','zdate','temp']].drop_duplicates(subset=['coid','zdate'],keep='last')
            all_coid_data = all_coid_data.append(this_coid_data,sort=False)
        all_coid_data = all_coid_data.loc[:,['coid','temp']]
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        t2 = time.time()
        elapsed_time = t2-t1
        #print('cost'+str(elapsed_time))
        return ans
    def calculate_moving(self,check_index,window='1d',col_type='average',col_kind='max',target_name=None,peer_future=False):
        t1 = time.time()
        edit_cols= self.confirm_checkindex(check_index1=check_index,window=window)
        check_index = edit_cols[0]
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            t2 = time.time()
            elapsed_time = t2-t1
            return ans
        this_date_data, this_window_type, window = self.get_activedate_data(window,column_names=[check_index],peer_future=peer_future)
        all_coid_data = pandas.DataFrame(columns=['zdate','coid','temp'])
        all_date_data = pandas.DataFrame(columns=['zdate','coid','temp'])
        #print(this_date_data.loc[this_date_data['coid']=='2330',['zdate','coid','mdate',check_index]])
            #year_start = self.sampledates[1] - pandas.DateOffset(years=window)
            #year_end = sampledates[1]
        check_data = []
            #計算

        if col_kind == 'mean':
            this_date_data['temp'] = (this_date_data[check_index].rolling(window=window).mean())
        elif col_kind == 'median':
            this_date_data['temp'] = (this_date_data[check_index].rolling(window=window).median())
        elif col_kind == 'min':
            this_date_data['temp'] = (this_date_data[check_index].rolling(window=window).min())
        elif col_kind == 'max':
            this_date_data['temp'] = (this_date_data[check_index].rolling(window=window).max())
        elif col_kind == 'sum':
            this_date_data['temp'] = (this_date_data[check_index].rolling(window=window).sum())

        this_date_data['zdate'] = self.current_zdate
        this_date_data = this_date_data.loc[:,['coid','mdate','zdate','temp']].drop_duplicates(subset=['coid','zdate'],keep='last')
        all_date_data = this_date_data
        all_coid_data = all_date_data.loc[:,['coid','temp']]
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        t2 = time.time()
        elapsed_time = t2-t1
        return ans
    def revert_view(self,check_index,jump_length=1,jump_mdate=False,jump_kind='Y',fix_date=None,target_name=None,peer_future=False):
        t1 = time.time()
        window='1d'
        edit_cols= self.confirm_checkindex(check_index1=check_index,window=window)
        check_index = edit_cols[0]
        target_name = self.check_function_input(target_name)
        if self.check_data_available(target_name):
            ans = self.data[target_name].values
            t2 = time.time()
            elapsed_time = t2-t1
            return ans
        if peer_future is True:
            jump_length*=-1
        if jump_mdate is True:
            clue = 2
            base_mdate = self.cal_mdate(base_mdate=None,jump_length=jump_length,jump_kind=jump_kind,fix_date=fix_date)
            this_date_data, this_window_type, window = self.get_activedate_data(window=1,clue_length=clue,column_names=[check_index],peer_future=False,base_mdate=base_mdate)
        else:
            clue = 0
            base_zdate = self.cal_zdate(base_date=None,jump_length=jump_length,jump_kind=jump_kind,fix_date=fix_date)
            this_date_data, this_window_type, window = self.get_activedate_data(window='1d',clue_length=clue,column_names=[check_index],peer_future=False,base_date=base_zdate)

            #去掉多餘資料，因為只有最新一筆是有用的
        this_date_data['temp'] = this_date_data[check_index]
        this_date_data['zdate'] = self.current_zdate
        this_date_data = this_date_data.loc[:,['coid','mdate','zdate','temp']].drop_duplicates(subset=['coid','zdate'],keep='last')
        all_date_data = this_date_data
        all_coid_data = all_date_data.loc[:,['coid','temp']]
        ans = self.unify_data(all_coid_data,target_name,check_index='temp')
        t2 = time.time()
        elapsed_time = t2-t1
        return ans
    def unify_data(self,input_data,target_name,check_index='temp'):
        all_coid_data = self.current_coids.merge(input_data,on=['coid'],how='left')
        all_coid_data[check_index] = all_coid_data[check_index].fillna(0)
        all_coid_data[check_index] = all_coid_data[check_index].replace([numpy.inf, -numpy.inf], numpy.nan)
        if target_name is None:
            self.all_date_data[check_index] = numpy.nan
            self.all_date_data.loc[self.all_date_data['zdate']==self.current_zdate,check_index] = all_coid_data[check_index].values
            ans = self.all_date_data[check_index].values
            self.all_date_data = self.all_date_data.drop(columns=[check_index])
        else:
            self.all_date_data.loc[self.all_date_data['zdate']==self.current_zdate,target_name] = all_coid_data[check_index].values
            ans = self.all_date_data[target_name].values
        return ans
    def confirm_checkindex(self,check_index1,check_index2=None,window=1):
        ans = []
        if type(check_index1).__name__ != 'str':
            if 'Series'  in type(check_index1).__name__:
                this_col_name = check_index1.name
                if id(self.data[this_col_name]) == id(check_index1):
                    #假如物件的series id與指定名稱的id相同
                    check_index1 = this_col_name
                else:
                    #不存在的欄位名稱
                    print('invalid column name in dataframe:'+str(this_col_name))
                    sys.exit()
            elif 'int' in type(check_index1).__name__ or 'float' in type(check_index1).__name__:
                self.data['temp_name_up'] = check_index1
                check_index1 = 'temp_name_up'
            else:
                print('invalid column name:'+str(check_index1))
                sys.exit()
        ans.append(check_index1)
        if check_index2 is not None:
            if type(check_index2).__name__ != 'str':
                if 'Series'  in type(check_index2).__name__:
                    this_col_name = check_index2.name
                    if id(self.data[this_col_name]) == id(check_index2):
                        #假如物件的series id與指定名稱的id相同
                         check_index2 = this_col_name
                    else:
                        #不存在的欄位名稱
                        print('invalid column name in dataframe:'+str(this_col_name))
                        sys.exit()
                elif 'int' in type(check_index2).__name__ or 'float' in type(check_index2).__name__:
                    self.data['temp_name_up'] = check_index2
                    check_index1 = 'temp_name_up'
                else:
                    print('invalid column name:'+str(check_index2))
                    sys.exit()
            ans.append(check_index2)
        return ans
    def check_function_input(self,target_name,trace_layer=2):
        #用來查詢上兩層撰寫的target_name的
        if target_name is None:
            current_commands= traceback.format_stack()
            current_trace = current_commands[len(current_commands)-(1+trace_layer)].split('\n')
            current_line = current_trace[len(current_trace)-2]
            if '=' in current_line:
                first_phraes = current_line.split('=')[0]
                right_name = first_phraes.split("tejtool.data['")[1]
                target_name = right_name.split("']")[0]
        return target_name
    #給計算公式用，按照給訂window取出資料以便做計算
    def check_data_available(self,target_name):
        ans = False
        if target_name in self.all_date_data.columns:
            m = self.all_date_data.loc[self.all_date_data['zdate']==self.current_zdate,target_name].copy().dropna()
            if len(m)>0:
                ans = True
        return ans
    def cal_mdate(self,base_mdate=None,jump_length=1,jump_kind='Y',fix_date=None):
        #跟cal_zdate不同，cal_mdate的jump_kind是必填欄位，否則無法區分
        if base_mdate is None:
            base_mdate = self.current_mdate
        else:
            base_mdate = numpy.array([base_mdate]).astype('datetime64')[0]
        if  fix_date is None:
            a_m = str(pandas.to_datetime(base_mdate).month) if len(str(pandas.to_datetime(base_mdate).month))>1 else '0'+str(pandas.to_datetime(base_mdate).month)
            a_d = str(pandas.to_datetime(base_mdate).day) if len(str(pandas.to_datetime(base_mdate).day))>1 else '0'+str(pandas.to_datetime(base_mdate).day)
            if jump_kind is 'Y':
                fix_date = a_m+'-'+a_d
            elif  jump_kind is 'S':
                fix_date = a_d
        elif fix_date is not None:
            quarter_list=['Q1','Q3','Q3','Q4']
            for d_i in range(0,4):
                if fix_date == quarter_list[d_i]:
                    fix_date = '0'+str(d_i+1)+'-01' if d_i<3 else str(d_i+1)+'-01'
                    jump_kind = 'Y'
        if  fix_date is not None:
            #依照給定月日來算出
            if jump_kind is 'Y':
                last_mdate = numpy.array([str(pandas.to_datetime(base_mdate).year - jump_length)+'-'+fix_date]).astype('datetime64')[0]
            elif  jump_kind is 'S':
                jump_year = 0
                jump_length *=3
                jump_month = jump_length
                if pandas.to_datetime(base_mdate).month<=jump_length:
                    if (jump_month)%12!=0:
                        if jump_month>0:
                            jump_year = int(numpy.floor((jump_length - pandas.to_datetime(base_mdate).month)/12)) +1
                            jump_month = (jump_month)%12 -12
                        else:
                            jump_year = int(numpy.floor((jump_length - pandas.to_datetime(base_mdate).month)/12)) +1
                            jump_month = (12+(jump_month)%12)%12
                    else:
                        jump_year = int(jump_length/12)
                        jump_month = 0
                adj_m = str(pandas.to_datetime(base_mdate).month - jump_month)
                if len(adj_m)<2:
                    adj_m = '0'+adj_m
                last_mdate = numpy.array([str(pandas.to_datetime(base_mdate).year - jump_year)+'-'+adj_m+'-'+fix_date]).astype('datetime64')[0]
        #算出日期就是前推日之前最大的日期
        all_mdate_list = pandas.DataFrame(self.all_mdate_list,columns=['mdate'])
        new_base_mdate = all_mdate_list.loc[all_mdate_list['mdate']<last_mdate,'mdate'].max().strftime('%Y-%m-%d')
        return new_base_mdate
    def cal_zdate(self,base_date=None,jump_length=1,jump_kind='Y',fix_date=None,tradeday=True):
        #功能函式，可以獨立使用，沒帶入base_date則取模組的日期
        if base_date is None:
            base_date = self.current_zdate
        else:
            base_date = numpy.array([base_date]).astype('datetime64')[0]
        #用來控制前推日
        if  fix_date is None:
            a_m = str(pandas.to_datetime(base_date).month) if len(str(pandas.to_datetime(base_date).month))>1 else '0'+str(pandas.to_datetime(base_date).month)
            a_d = str(pandas.to_datetime(base_date).day) if len(str(pandas.to_datetime(base_date).day))>1 else '0'+str(pandas.to_datetime(base_date).day)
            if jump_kind is 'Y':
                fix_date = a_m+'-'+a_d
            elif  jump_kind is 'M':
                fix_date = a_d
        if  fix_date is not None:
            #依照給定月日來算出
            if len(fix_date.split('-'))==2:
                jump_kind = 'Y'
                last_date = numpy.array([str(pandas.to_datetime(base_date).year - jump_length)+'-'+fix_date]).astype('datetime64')[0]
            elif len(fix_date.split('-'))==1:
                jump_kind = 'M'
                jump_year = 0
                jump_month = jump_length
                if pandas.to_datetime(base_date).month<=jump_length or pandas.to_datetime(base_date).month-jump_length>12:
                    if (jump_month)%12!=0:
                        if jump_month>0:
                            jump_year = int(numpy.floor((jump_length - pandas.to_datetime(base_date).month)/12)) +1
                            jump_month = (jump_month)%12 -12
                        else:
                            jump_year = int(numpy.floor((jump_length - pandas.to_datetime(base_date).month)/12)) +1
                            jump_month = (12+(jump_month)%12)%12
                    else:
                        jump_year = int(jump_length/12)
                        jump_month = 0
                adj_m = str(pandas.to_datetime(base_date).month - jump_month)
                if len(adj_m)<2:
                    adj_m = '0'+adj_m
                adj_date = str(pandas.to_datetime(base_date).year - jump_year)+'-'+adj_m+'-'+fix_date
                adj_date = self.check_available_date(adj_date)
                last_date = numpy.array([adj_date]).astype('datetime64')[0]
        else:
            if jump_kind is 'W':
                last_date = base_date - numpy.timedelta64(jump_length,jump_kind)
            else:
                if jump_length>0:
                    jump_length-=1
                window = abs(jump_length)
                if jump_length >0:
                    this_datefilter = pandas.DataFrame(self.all_zdate_list,columns=['zdate']).sort_values(by=['zdate'],ascending=False)
                    this_datefilter = this_datefilter[this_datefilter['zdate']<=base_date]
                else:
                    this_datefilter = pandas.DataFrame(self.all_zdate_list,columns=['zdate']).sort_values(by=['zdate'],ascending=True)
                    this_datefilter = this_datefilter[this_datefilter['zdate']>=base_date]

                if len(this_datefilter)>1:
                    last_date = this_datefilter['zdate'].values[0+window]
                elif len(this_datefilter) ==1:
                    last_date = this_datefilter['zdate'].values[0]
                else:
                    last_date = base_date
        if tradeday is False:
            new_base_zdate = last_date
        else:
            #需基於交易日再過濾出資料
            all_zdate_list = pandas.DataFrame(self.prc_basedate['zdate'].unique(),columns=['zdate'])
            if self.all_zdate_list[0] == last_date:
                new_base_zdate = pandas.to_datetime(last_date).strftime('%Y-%m-%d')
            else:
                last_date = pandas.to_datetime(last_date)
                if last_date <=all_zdate_list['zdate'].min():
                    new_base_zdate = all_zdate_list['zdate'].min()
                else:
                    new_base_zdate = all_zdate_list.loc[all_zdate_list['zdate']<last_date,'zdate'].max()
                new_base_zdate = new_base_zdate.strftime('%Y-%m-%d')
        new_base_zdate = numpy.array([new_base_zdate]).astype('datetime64')[0]

        return new_base_zdate
    def check_available_date(self,zdate):
        correctDate = False
        zdate_list = zdate.split('-')
        zdate_list[2] = str(min(31,int(zdate_list[2])))
        if int(zdate_list[2])>28:
            for adj_i in range(0,4):
                try:
                    newDate = datetime(int(zdate_list[0]),int(zdate_list[1]),int(zdate_list[2])-adj_i)
                    correctDate = True
                    zdate = zdate_list[0]+'-'+zdate_list[1]+'-'+str(int(zdate_list[2])-adj_i)
                    break
                except ValueError:
                    correctDate = False
        return zdate