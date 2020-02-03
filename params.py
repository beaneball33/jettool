import pandas
import numpy
import os
import tejapi
tejapi.ApiConfig.api_key = "your_API_key"

"""
在tejtool中，所有的日期皆必須維持datetime64且最小單位到日，ex:"2019-11-23"的格式
模組初始化後自動產生預設的日期組
current_zdate        資料觀測日
datastart_date       資料查詢起日
backstart_date       回顧測試起始日
"""
back_length = 365
query_length = 365*2

current_zdate  = numpy.datetime64('today') 
dataend_date  = numpy.datetime64('today') 
datastart_date = dataend_date 
backstart_date = current_zdate - numpy.timedelta64(back_length,'D')

data_attr = {'datastart_date':str(datastart_date),'dataend_date':str(dataend_date)}

coid_length_index = 300
input_coids = None
cash = 1000000
benchmark_cash = 1000000
benchmark_id = 'Y9997'
back_date_list = None
market = 'TWN'

all_prc_dataset = ['APRCD','AFF_RAW','AMT1','ABSTN1','ASALE']
mdate_name_dict = {"ASALE":"annd_s"}
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
table_info = {}

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



current_mdate = None
listed_coids = []

accountData = None
activeAccountData = None