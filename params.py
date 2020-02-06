import pandas
import numpy
import os
import tejapi
tejapi.ApiConfig.api_key = "your_API_key"
# https://tw-google-styleguide.readthedocs.io/en/latest/google-python-styleguide/python_style_rules.html
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
current_mdate = None
listed_coids = []
dataend_date  = numpy.datetime64('today') 
datastart_date = dataend_date 
backstart_date = current_zdate - numpy.timedelta64(back_length,'D')
data_attr = {'datastart_date':str(datastart_date),
             'dataend_date':str(dataend_date)}

coid_length_index = 300
input_coids = None
cash = 1000000
benchmark_cash = 1000000
benchmark_id = 'Y9997'
back_date_list = None
market = 'TWN'
roib_name ='報酬率-Ln'
closed_name ='收盤價(元)'

# 可以查詢的資料表國別分類清單
api_tables = {}
# 可以查詢的日資料清單
all_prc_dataset = ['APRCD','AFF_RAW','AMT1','ABSTN1','ASALE']
# 不同table的zdate名稱對照表
mdate_name_dict = {"ASALE":"annd_s"}

account_table = {'TWN':{'cover':'AIFINQA','data':'AIFINQ'}}
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



accountData = None
activeAccountData = None
# TODO(zyx) 有版本別與無版本別的科目需要mapping
transfer_acccode_list = [{'acc_code':'0400','cname':'不動產及設備淨額','new_acc_code':'BP51','new_name':'固定資產'},{'acc_code':'0820','cname':'無形資產','new_acc_code':'BP53','new_name':'無形資產'},{'acc_code':'032A','cname':'投資性不動產淨額','new_acc_code':'BV31','new_name':'投資性不動產'},{'acc_code':'2110','cname':'普通股股本','new_acc_code':'BF41','new_name':'普通股股本'},{'acc_code':'2310','cname':'資本公積合計','new_acc_code':'BF42','new_name':'資本公積'},{'acc_code':'2341','cname':'保留盈餘','new_acc_code':'BF43','new_name':'保留盈餘'},{'acc_code':'2120','cname':'特別股股本','new_acc_code':'BF44','new_name':'特別股股本'},{'acc_code':'2900','cname':'非控制權益','new_acc_code':'BF45','new_name':'非控制權益'},{'acc_code':'2480','cname':'其他權益','new_acc_code':'BF99','new_name':'其他權益'},{'acc_code':'0100','cname':'流動資產','new_acc_code':'BSCA','new_name':'流動資產合計'},{'acc_code':'0960','cname':'非流動資產','new_acc_code':'BSNCA','new_name':'非流動資產合計'},{'acc_code':'0010','cname':'資產總額','new_acc_code':'BSTA','new_name':'資產總計'},{'acc_code':'1100','cname':'流動負債','new_acc_code':'BSCL','new_name':'流動負債合計'},{'acc_code':'1800','cname':'非流動負債','new_acc_code':'BSNCL','new_name':'非流動負債合計'},{'acc_code':'1000','cname':'負債總額','new_acc_code':'BSTL','new_name':'負債總額'},{'acc_code':'2000','cname':'股東權益總額','new_acc_code':'BSSE','new_name':'股東權益總計'},{'acc_code':'3100','cname':'營業收入','new_acc_code':'IP11','new_name':'營業收入'},{'acc_code':'3200','cname':'營業成本','new_acc_code':'IP21','new_name':'營業成本'},{'acc_code':'3300','cname':'營業費用','new_acc_code':'IP31','new_name':'營業費用'},{'acc_code':'3910','cname':'所得稅費用','new_acc_code':'IP51','new_name':'所得稅'},{'acc_code':'3501','cname':'財務成本','new_acc_code':'IF11','new_name':'利息支出'},{'acc_code':'3900','cname':'稅前淨利','new_acc_code':'ISIBT','new_name':'利潤總額'},{'acc_code':'3970','cname':'合併總損益','new_acc_code':'ISNI','new_name':'淨利潤'},{'acc_code':'3950','cname':'歸屬母公司淨利(損)','new_acc_code':'ISNIP','new_name':'母公司淨利'},{'acc_code':'3990','cname':'每股盈餘','new_acc_code':'EPS','new_name':'母公司每股盈餘'},{'acc_code':'3295','cname':'營業毛利','new_acc_code':'GM','new_name':'毛利'},{'acc_code':'3395','cname':'營業利益','new_acc_code':'OPI','new_name':'營業利益'},{'acc_code':'R531','cname':'常續性利益','new_acc_code':'RI','new_name':'常續性利益'},{'acc_code':'7910','cname':'期初現金及約當現金','new_acc_code':'CSBCH','new_name':'期初現金及等價物淨額'},{'acc_code':'7210','cname':'來自營運之現金流量','new_acc_code':'CSCFO','new_name':'營運產生現金流量'},{'acc_code':'7300','cname':'投資活動之現金流量','new_acc_code':'CSCFI','new_name':'投資產生現金流量'},{'acc_code':'7400','cname':'籌資活動之現金流量','new_acc_code':'CSCFF','new_name':'融資產生現金流量'},{'acc_code':'7920','cname':'期末現金及約當現金','new_acc_code':'CSECH','new_name':'期末現金及等價物淨額'}]    