﻿import pandas
import os
import tejapi
tejapi.ApiConfig.api_key = "your_API_key"

back_length = 365
query_length = 365
coid_length_index = 300
input_coids = None
cash = 1000000
benchmark_cash = 1000000
benchmark_id = 'Y9997'
back_date_list = None
base_date = '2019-12-31'
data = pandas.DataFrame(columns=['zdate','coid'])
show_coid = '2330'
active_view = False
long_fee = 0.001425
short_fee = 0.001425+0.003
indicators = []
hold_coids = []
hold_unit = []
prc_basedate = None
basic_info = None
fin_cover = None
findata_all = None
all_date_data = None
trained_model = {}
hold_data = pandas.DataFrame(columns=['zdate','coid','unit','現值'])
backtest_message={}
current_dir = 'none'
indicators = []
indicator_attr = {}
indicator_name = {}
new_columns = []
lack_data_msg = {}
acc_code = []
hash_range = {}
acc_code_name = []
all_mdate_list = None
all_zdate_list = None
simple_roi_data = None
benchmark_roi = None
maxdrawback = [0,0,0]
retrain_model = {}
applied = False
by_unit = False
check_columns_relation = {}
check_columns = {}
back_interval = []
check_correlation = {}
sampledates = [0,pandas.Timestamp(base_date),0]
sampledates[0] = sampledates[1] - pandas.DateOffset(days=back_length) #回顧起日
sampledates[2] = sampledates[1] - pandas.DateOffset(days=query_length) #最少6年樣本起日
input_date = pandas.DataFrame(sampledates,columns=['zdate'])
input_date['zdate'] = input_date['zdate'].astype(str).astype('datetime64')
datastart_date = input_date.loc[2,'zdate']
current_zdate = input_date.loc[1,'zdate']
current_mdate = None
listed_coids = []
sampledates = sampledates
accountData = None
activeAccountData = None