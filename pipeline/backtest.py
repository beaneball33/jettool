import numpy
import pandas
from . import method
import time 
import os
import tempfile
import contextlib
import inspect
import seaborn as sns; sns.set()
sns.set_style("whitegrid", {'axes.grid' : False})

class backtest_base(method.method_base):
    def back_test(self,back_interval=None,cash=1000000,
                       import_data=None,keep_data=False,
                       calculate=None,evaluate=None,
                       roib_name='報酬率-Ln',closed_name ='收盤價(元)'):
        print('跨股模型計算與回顧測試')
        self.roib_name = roib_name
        self.closed_name = closed_name
        t0 = time.time()
        self.set_back_test(back_interval,import_data,keep_data,cash)
        print([self.cash,self.benchmark_cash])
        print('倒推計算日'+str(self.backstart_date ))
        print('回測損益起算日'+str(self.roistart_date) )
        print('back interval:')
        print(self.back_interval)        
        print('開始計算指標')
        calculate = self.create_function_text(calculate)
        evaluate = self.create_function_text(evaluate)
        for t in range(self.back_interval[0],self.back_interval[1],-1):
            t1 = time.time()
            self.manage_report(current_time=t)
            self.exec_lines((line for line in calculate))
            self.manage_data()
            if self.current_zdate>= self.roistart_date:
                self.exec_lines((line for line in evaluate))
                #在當天收盤、資訊充分揭露後才計算持股數量
            self.cal_roi()
            if self.current_zdate>= self.roistart_date:
                print(pandas.to_datetime(str(self.current_zdate)).strftime('%Y-%m-%d')+' 持股現值:'+str(int(self.simple_roi_data.loc[self.simple_roi_data['pname']=='portfolio','present value'].tail(1).values[0])))
            else:
                print(pandas.to_datetime(str(self.current_zdate)).strftime('%Y-%m-%d'))
        print('ok')
        self.manage_backtest_outcome()
        lm = sns.relplot(x="zdate", y="present value",height=5, aspect=3, kind="line", hue='pname',legend="full", data=self.simple_roi_data)
        lm.fig.suptitle('每日結算投資現值', fontsize=18)
        if self.applied == True:
            lm.savefig(self.current_dir+'/backtest_pv_'+str(self.roistart_date)+'.png')
        lm = sns.relplot(x="zdate", y="return",height=5, aspect=3, kind="line", hue='pname', legend="full", data=self.simple_roi_data)
        lm.fig.suptitle('每日損益', fontsize=18)     
        if self.applied == True:
            lm.savefig(self.current_dir+'/backtest_return_'+str(self.roistart_date)+'.png')    
        t3 = time.time()
        elapsed_time = t3-t0
        print('total cost'+str(elapsed_time))
    def create_function_text(self,def_func):
        if inspect.isroutine(def_func) is True:
            code_lines = inspect.getsource(def_func).split('\n')
            code_list = [ code_lines[i].lstrip() for i in range(1,len(code_lines))]
            return code_list
        else:
		            return []
    def get_back_test_index(self,back_interval):
        start_index = 0
        end_index = -1
        if back_interval is not None:
            if type(back_interval).__name__ == 'int':
                #整數型，由最後一天往前推
                back_interval = max(0 , min(back_interval,len(self.all_zdate_list)-2))
                temp_back_date = self.all_zdate_list[0] -  numpy.timedelta64(back_interval,'D')
                back_interval = pandas.to_datetime(temp_back_date).strftime('%Y-%m-%d')
                print(back_interval)
            #若back_interval有傳入，按照型別處理

            if type(back_interval).__name__ == 'list':
                #區間型，自動找出對應日期
                if type(back_interval[0]).__name__ == 'int':
                    start_index = max(0 , back_interval[0])
                    end_index   = max(-1 , back_interval[1])
                elif type(back_interval[0]).__name__ == 'str':
                    calstart_date = numpy.datetime64(back_interval[0])
                    for t in range(0,len(self.all_zdate_list)):
                        if self.all_zdate_list[t]<=calstart_date:
                            start_index = t
                            break
                    calend_date = numpy.datetime64(back_interval[1])
                    for t in range(0,len(self.all_zdate_list)):
                        if self.all_zdate_list[t]<=calend_date:
                            end_index = t
                            break

            elif type(back_interval).__name__ == 'str':
                #字串型，轉換為日期使用，找出對應日期
                calstart_date = numpy.datetime64(back_interval)
                for t in range(0,len(self.all_zdate_list)):
                    if self.all_zdate_list[t]<=calstart_date:
                        start_index = t
                        break
        else:
            start_index = len(self.all_zdate_list)-2
        return [start_index,end_index]
    def set_back_test(self,back_interval=None,import_data=None,keep_data=False,cash=1000000):
        self.data = None
        self.last_data = None
        if keep_data is False:
            self.combine_query(import_data=import_data)
            self.hold_coids = []
            self.hold_data = pandas.DataFrame(columns=['zdate','coid','unit','現值'])
        if back_interval is None:
            back_interval = pandas.to_datetime(self.sampledates[0]).strftime('%Y-%m-%d')
        back_indexs = self.get_back_test_index(back_interval)
        start_index = back_indexs[0]
        end_index = back_indexs[1]
        self.backtest_message = {'current_t':start_index}
        #初始化回測所需參數
        self.cash  = cash
        self.benchmark_cash = self.cash
        self.backstart_date = self.all_zdate_list[start_index] #back_date_list
        
        self.roistart_date = self.all_zdate_list[0] -  numpy.timedelta64(self.back_length,'D')
        if self.roistart_date < self.backstart_date:
            self.roistart_date = self.backstart_date        
        self.simple_roi_data = pandas.DataFrame(columns=['zdate','pname','present value','return','roi'])
        portfolio_data = pandas.DataFrame([[self.roistart_date,'portfolio',self.cash,0.0,0.0]],columns=['zdate','pname','present value','return','roi'])
        benchmark_data = pandas.DataFrame([[self.roistart_date,'benchmark',self.benchmark_cash,0.0,0.0]],columns=['zdate','pname','present value','return','roi'])
        self.simple_roi_data = self.simple_roi_data.append(portfolio_data,sort=False)
        self.simple_roi_data = self.simple_roi_data.append(benchmark_data,sort=False)

        self.back_date_list = self.all_zdate_list[end_index+1:start_index+1]
        self.back_interval = [start_index,end_index]
        self.current_zdate = self.all_zdate_list[start_index]
        self.change_report = True
        if self.active_view is True:
            self.chg_zdate_list = self.findata_all.loc[self.findata_all['zdate']>self.current_zdate,'zdate'].unique()
            self.drop_col = ['semester', 'fin_od','mdate'] + self.acc_code_name
            self.fin_data = self.findata_all[self.findata_all['fin_od']==1].sort_values(by=['coid','mdate'])
            fin_data_col = self.fin_data.columns
            #取出回測最初日以前的重編財報，排序並去除重複
            self.part_data = self.findata_all[(self.findata_all['zdate']<self.current_zdate)&(self.findata_all['fin_od']>1)].sort_values(by=['coid','mdate','fin_od']).drop_duplicates(subset=['coid','mdate'],keep='last')
        self.famamacbeth_outcome = None
    def reform_report(self):
        self.part_data = self.part_data.append(self.findata_all[(self.findata_all['zdate']==self.current_zdate)&(self.findata_all['fin_od']>1)],sort=False).sort_values(by=['coid','mdate','fin_od'])
        #消掉zdate，因為是錯的
        self.part_data['zdate'] = pandas.NaT
        #將兩段OD合併
        fin_data_temp = self.fin_data.append(self.part_data,sort=False).sort_values(by=['coid','mdate','fin_od'])
        #zdate補值，保留最後一個版，完成
        fin_data_temp['zdate'] = fin_data_temp['zdate'].fillna(method='ffill')
        fin_data_temp = fin_data_temp.drop_duplicates(subset=['coid','mdate'],keep='last')
        #如果不是剛初始化的，要刪掉所有fin_data
        if 'fin_od' in self.all_date_data.columns:
            self.all_date_data = self.all_date_data.drop(self.drop_col, axis=1)

        self.all_date_data = self.all_date_data.merge(fin_data_temp,on=['coid','zdate'],how='left')
        self.all_date_data[self.acc_code_name] = self.all_date_data[self.acc_code_name].fillna(method='ffill')
        self.all_date_data[['semester','fin_od','mdate']] = self.all_date_data[['semester','fin_od','mdate']].fillna(method='ffill')
        if 'fin_od' in self.all_date_data.columns:
            self.all_date_data = self.all_date_data.drop_duplicates(subset=['coid','zdate'],keep='last')
    def manage_report(self,current_time=0):
        self.current_zdate = self.all_zdate_list[current_time]
        delist_list = self.basic_info.loc[self.basic_info['list_day2']<=str(self.current_zdate),'coid'].values.tolist()
        self.all_date_data = self.all_date_data.loc[self.all_date_data['coid'].isin(delist_list)==False,:].reset_index(drop=True)
        if current_time is not None:
            self.backtest_message['current_t'] = current_time
        if self.active_view is True:
            #只有異動日期
            if self.current_zdate in self.chg_zdate_list or self.change_report is True:
                self.reform_report()
        else:
            #如果不是動態報表，且尚未進去合併全日期資料，則進行合併，每次回測只會進行一次
            if  self.change_report is True:
                actual_ciod = self.all_date_data['coid'].unique().tolist()
                prc_zdate = self.prc_basedate.groupby('coid')['zdate'].min().reset_index()
                findata_mdate = self.findata_all.groupby('coid')['mdate'].min().reset_index()
                df_minmdate = prc_zdate.merge(findata_mdate,on=['coid'],how='left').rename(columns={'mdate':'temp'})
                self.all_date_data = self.all_date_data.merge(df_minmdate,on=['coid','zdate'],how='left')
                self.all_date_data = self.all_date_data.merge(self.findata_all,on=['coid','zdate'],how='left')
                self.all_date_data.loc[self.all_date_data['temp'].isnull()==False,'mdate']=self.all_date_data.loc[self.all_date_data['temp'].isnull()==False,'temp']
                self.all_date_data = self.all_date_data.drop(columns=['temp'])
                self.all_date_data = self.all_date_data.sort_values(by=['coid','zdate'], ascending=True).reset_index(drop=True)
                fill_col = self.findata_all.columns.tolist()
                fill_col.remove('coid')
                fill_col.remove('zdate')
                self.all_date_data.loc[self.all_date_data['zdate']==self.all_date_data['zdate'].min(),fill_col] = numpy.inf
                self.all_date_data = self.all_date_data.fillna(method='ffill')
                self.all_date_data[fill_col] = self.all_date_data[fill_col].replace([numpy.inf, -numpy.inf], numpy.nan)
                self.all_date_data[fill_col] = self.all_date_data[fill_col].fillna(method='bfill')
                #濾掉不該有的日期的資料
                self.all_date_data = self.all_date_data.loc[self.all_date_data['zdate'].isin(self.all_zdate_list),:]
                self.all_date_data = self.all_date_data.drop_duplicates(subset=['coid','zdate'])

        self.set_listed_coid(self.all_date_data)
        self.change_report = False
        self.data = self.all_date_data
    def set_listed_coid(self,df):
        listed_coids = self.basic_info.loc[self.basic_info['list_day1']<=self.current_zdate,'coid'].values.tolist()
        self.current_coids = df.loc[(df['zdate']==self.current_zdate),['zdate','coid']]
        self.listed_coids = df.loc[(df['zdate']==self.current_zdate)&(df['coid'].isin(listed_coids)),'coid'].values.tolist()    
    def manage_data(self):
        self.all_date_data = self.data
        self.data  = self.all_date_data.loc[self.all_date_data['zdate']==self.current_zdate,:].copy().reset_index(drop=True)
        self.data['持股權重分子'] = 0
        self.data['投資比重'] = 0
        self.data['購入'] = False
    def cal_roi(self,back_index=None,force_cal=False,fee=True):
        if self.current_zdate>= self.roistart_date or force_cal is True:
            if back_index is None:
                for t in range(0,len(self.all_zdate_list)):
                    if self.all_zdate_list[t]<=self.current_zdate:
                        back_index = t
                        break
            if self.by_unit is True:
                self.data['投資比重'] = self.data['unit']*self.data[self.closed_name]/self.cash
            else:
                self.data['unit'] = self.cash*self.data['投資比重']/ self.data[self.closed_name]
            self.data['現值'] = self.data['unit']*self.data[self.closed_name]
            self.data['總現值'] = self.data['現值'].sum()
            self.data['損益'] = 0
            if back_index ==0: #if t ==0:
                #當遞迴日期就是回測最晚一天(basedate)時，報酬率無法計算，填入0
                this_hold_data = self.data.loc[:,['zdate','coid','unit','現值']]
                this_hold_data['roibNext'] = 0
                self.hold_data = self.hold_data.append(this_hold_data,sort=False)
            else:
                #最新一期不算損益
                next_date = self.all_zdate_list[back_index-1] #取出下一個日期，由於當日沒有報酬率，所以沒有
                next_date_roib = self.prc_basedate.loc[self.prc_basedate['zdate']==next_date,['coid',self.roib_name]].rename(index=str, columns={self.roib_name:'roibNext'})
                self.data = self.data.merge(next_date_roib,on=['coid'],how='left').drop_duplicates(subset=['coid']).reset_index(drop=True)
                this_hold_data = self.data.loc[:,['zdate','coid','unit','現值','roibNext']]
                self.hold_data = self.hold_data.append(this_hold_data,sort=False)
                try:
                    benchmark_roi_rate = self.benchmark_roi.loc[self.benchmark_roi['zdate']==next_date,'績效指標報酬率'].values[0]
                except:
                    print(str(next_date)+' no benchmark roi')
                    benchmark_roi_rate = 0
                self.benchmark_return = self.benchmark_cash*(numpy.exp(benchmark_roi_rate/100) -1)
                self.benchmark_cash = self.benchmark_cash + self.benchmark_return
                if self.last_data is not None:
                    self.data = self.data.merge(self.last_data,on=['coid'],how='left')
                else:
                    self.data['前期持股']=0
                self.data['手續費'] = numpy.maximum(0,self.data['unit'] - self.data['前期持股'])*self.data[self.closed_name]*self.long_fee+numpy.maximum(0,self.data['前期持股']-self.data['unit'])*self.data[self.closed_name]*self.short_fee
                self.last_data = self.data.loc[:,['coid','unit']].copy().rename(index=str, columns={'unit':'前期持股'})
                self.data['損益'] = self.data['現值']*(numpy.exp(self.data['roibNext']/100) - 1 ) - self.data['手續費']
                self.data['總損益'] = self.data['損益'].sum()
                self.cash = self.cash + self.data['損益'].sum()
                self.data['總報酬率'] = self.data['總損益']/self.data['現值'].sum()
                portfolio_sum_roi = self.data['總損益'].unique()[0]/self.data['現值'].sum()
                portfolio_data = pandas.DataFrame([[next_date,'portfolio',self.cash,self.data['總損益'].unique()[0],portfolio_sum_roi]],columns=['zdate','pname','present value','return','roi'])
                benchmark_data = pandas.DataFrame([[next_date,'benchmark',self.benchmark_cash,self.benchmark_return,benchmark_roi_rate/100]],columns=['zdate','pname','present value','return','roi'])
                self.simple_roi_data = self.simple_roi_data.append(portfolio_data,sort=False)
                self.simple_roi_data = self.simple_roi_data.append(benchmark_data,sort=False)
    def manage_backtest_outcome(self):
        self.simple_roi_data['present value'] = self.simple_roi_data['present value'].astype(numpy.float32)
        self.simple_roi_data['return'] = self.simple_roi_data['return'].astype(numpy.float32)
        # Plot the lines on two facets
        self.portfolio_pv = numpy.nan_to_num(self.simple_roi_data.loc[self.simple_roi_data['pname']=='portfolio','present value'].values)
        self.benchmark_pv = self.simple_roi_data.loc[self.simple_roi_data['pname']=='benchmark','present value'].values
        self.portfolio_roi_rate = numpy.nan_to_num(self.simple_roi_data.loc[self.simple_roi_data['pname']=='portfolio','roi'].values)
        self.benchmark_roi_rate = self.simple_roi_data.loc[self.simple_roi_data['pname']=='benchmark','roi'].values
        self.portfolio_std = numpy.std(self.portfolio_roi_rate)
        self.benchmark_std = numpy.std(self.benchmark_roi_rate)
        self.maxdrawback = self.calculate_maxdrawback()
        print('投資組合標準差:'+str(self.portfolio_std))
        print('績效指標標準差:'+str(self.benchmark_std))
        print('最大回撤率:'+str(self.maxdrawback))
    def exec_lines(self,lines):
        fd, name = tempfile.mkstemp(suffix=".py")

        with open(fd, "r+", encoding='utf-8') as f:
            for line in lines:
                f.write(line)
                f.write("\n")
            f.seek(0)
            source = f.read()

        context = {"tejtool": self, "__file__": name, "__name__": "__main__"}
        try:
            exec(compile(source, name, "exec"), context)
        finally:
            with contextlib.suppress(OSError):
                os.remove(name)


    def overwrite_data(self,input_data=None,exist_data=None):
        if input_data is not None and exist_data is not None:
            mkey = ['zdate','coid']
            for name in input_data:
                if exist_data.get(name) is not None:
                    exit_df = exist_data.get(name)
                input_data[name] = input_data[name].append(exit_df,sort=False)
                input_data[name] = input_data[name].drop_duplicates(subset=['coid','zdate'],keep='first')
                input_data[name] = input_data[name].sort_values(by=['coid','zdate']).reset_index(drop=True)
        return input_data
    def load_cal_data(self,input_data,columns=[]):
        output_data = self.prc_basedate[['zdate','coid']]
        if len(columns)>0:
            for col in columns:
                mkey = ['zdate','coid']
                temp_df = input_data[col['hash']]
                if temp_df is not None:
                    temp_df = temp_df.rename(columns={"value": col['name']})
                    temp_df['zdate'] = temp_df['zdate'].astype(str).astype('datetime64')
                    temp_df['coid'] = temp_df['coid'].astype(str)
                    temp_df[col['name']] = temp_df[col['name']].astype(numpy.float32,errors='ignore')
                    begin_date = temp_df['zdate'].min()
                    end_date = temp_df['zdate'].max()
                    self.hash_range[col['name']] = {'begin_date':begin_date,'end_date':end_date}
                    output_data = output_data.merge(temp_df,on=mkey,how='left')
                    output_data[col['name']] = output_data[col['name']].fillna(method='ffill')
                    output_data.loc[output_data['zdate']>end_date, col['name']] =numpy.nan
        output_data = output_data.dropna()
        return  output_data
    def get_cal_data(self,columns=[]):
        ans = ['zdate','coid']
        if len(columns)>0:
            for col in columns:
                if col['name'] in self.all_date_data.columns:
                    ans = ans + [col['name'] ]
        return self.all_date_data.loc[self.all_date_data['zdate']>=self.backstart_date,ans].fillna(0)
    def get_calc_data(self,input_data=None,columns=[],start_date=None,review_data=False):
        if input_data is None:
            input_data = self.all_date_data
        if start_date is None:
            start_date = self.backstart_date
        output_data = {}
        if len(columns)>0:
            for col in columns:
                ans = ['zdate','sdate','coid']
                if col['name'] in input_data.columns:
                    ans = ans + [col['name'] ]
                temp_data = input_data.loc[input_data['zdate']>=start_date,ans].dropna()
                if self.hash_range.get(col['name']) is not None and review_data is False:
                    hash_range = self.hash_range.get(col['name'])
                    begin_date = hash_range.get('begin_date')
                    end_date = hash_range.get('end_date')
                    temp_data = temp_data.loc[(temp_data['zdate']>end_date)|(temp_data['zdate']<begin_date),ans]
                temp_data = temp_data.drop_duplicates(subset=['coid','sdate',col['name']],keep='first').reset_index(drop=True)
                output_data[col['hash']] = temp_data.loc[:,['zdate','coid',col['name']]].rename(columns={col['name']:"value"})
        return output_data
    def get_holddata(self,back_index=-1):
        df = self.hold_data[self.hold_data['unit']!=0].reset_index(drop=True)
        df = df.rename(columns={"現值": "value", "roibNext": "roibnext"})
        df['roibnext'] = df['roibnext'].fillna(0)
        if back_index == -1:
            df = df[df['zdate']==self.current_zdate]
        return df[['zdate','coid','unit','value','roibnext']]
    def combine_query(self,import_data=None):
        self.all_date_data = self.prc_basedate.copy()
        
        if import_data is not None:
            self.all_date_data = self.all_date_data.merge(import_data,on=['coid','zdate'],how='left')
            

    def do_outputfile(self):
        temp_output_list = self.all_date_data.columns.tolist()
        for code in self.indicator_attr:
            this_name = self.indicator_attr[code]['name']
            temp_output_list.remove(this_name)
        temp_output = self.all_date_data[temp_output_list]
        temp_output.to_csv('all_date_data.csv',encoding='big5',index=False)
    def do_backupdata(self,backup_data=None):
        if backup_data is None:
            backup_data = {}
        backup_data['input_coids'] = self.input_coids
        backup_data['active_view'] = self.active_view
        backup_data['indicator_attr'] = self.indicator_attr
        backup_data['findata_all'] = self.findata_all
        backup_data['prc_basedate'] = self.prc_basedate
        backup_data['all_date_data'] = self.all_date_data
        backup_data['hold_data'] = self.hold_data
        backup_data['simple_roi_data'] = self.simple_roi_data
        backup_data['all_zdate_list'] = self.all_zdate_list
        backup_data['back_date_list'] = self.back_date_list
        backup_data['benchmark_roi'] = self.benchmark_roi
        backup_data['check_columns_relation'] = self.check_columns_relation
        backup_data['check_columns'] = self.check_columns
        backup_data['backstart_date'] = self.backstart_date
        backup_data['current_zdate'] = self.current_zdate
        backup_data['current_mdate'] = self.current_mdate
        backup_data['basic_info'] = self.basic_info
        backup_data['acc_code_name'] = self.acc_code_name
        backup_data['back_interval'] = self.back_interval
        backup_data['listed_coids'] = self.listed_coids
        return backup_data
    def do_reloadbackup(self,backup_data,reload_outcome=False):
        self.input_coids = backup_data['input_coids']
        self.indicator_attr = backup_data['indicator_attr']
        self.prc_basedate = backup_data['prc_basedate']
        self.findata_all = backup_data['findata_all']
        self.basic_info = backup_data['basic_info']
        self.active_view  = backup_data['active_view']
        self.acc_code_name = backup_data['acc_code_name']
        self.back_interval = backup_data['back_interval']
        self.listed_coids = backup_data['listed_coids']
        if reload_outcome is True:
            self.backstart_date = backup_data['backstart_date']
            self.all_date_data = backup_data['all_date_data']
            self.check_columns_relation = backup_data['check_columns_relation']
            self.check_columns = backup_data['check_columns']
            self.current_zdate = backup_data['current_zdate']
            self.current_mdate = backup_data['current_mdate']
            self.hold_data = backup_data['hold_data']
            self.simple_roi_data = backup_data['simple_roi_data']
        self.all_zdate_list = backup_data['all_zdate_list']
        self.back_date_list = backup_data['back_date_list']
        self.benchmark_roi = backup_data['benchmark_roi']