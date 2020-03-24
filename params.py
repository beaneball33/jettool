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
current_mdate = numpy.datetime64('today') 
listed_coids = []
dataend_date  = numpy.datetime64('today') 
datastart_date = dataend_date 
backstart_date = current_zdate - numpy.timedelta64(back_length,'D')
data_attr = {'datastart_date':str(datastart_date),
             'dataend_date':str(dataend_date)}

coid_length_index = 300
input_coids = ['2330','2002','2882']
cash = 1000000
benchmark_cash = 1000000
benchmark_id = 'Y9997'
back_date_list = numpy.array([numpy.datetime64('today')])
market = 'TWN'
roib_name ='報酬率-Ln'
closed_name ='收盤價(元)'

# 可以查詢的資料表國別分類清單
api_tables = {}
# 可以查詢的日資料清單
#all_prc_dataset = ['APRCD','AFF_RAW','AMT1','ABSTN1','ASALE']
all_prc_dataset = []
all_marco_dataset = []
all_indicator_dataset = []
all_prc_dataset_freq = ['D','W','S','M','Y','N']
# 不同table的zdate名稱對照表
mdate_name_dict = {'ASALE':{'mdate':'annd_s','frequency':'M'},'ABSTN1':{'mdate':'mdate','frequency':'D'},'ACRQMTAB':{'mdate':'rmk_d','frequency':'S'}}
category_list = {}
market_list = {}
table_list = {}

account_table = {'TWN':{'cover':'AIFINQA','data':'AIFINQ'}}

# kind=1代表有對照表，kind=2代表沒有
macro_table = {'ANMAR':{'dbCode':'GLOBAL','cover':'ABMAR','kind':1},'GCURR':{'dbCode':'GLOBAL','kind':2},'ARATE':{'dbCode':'TWN','kind':2}}

data = pandas.DataFrame(columns=['zdate','coid'])
show_coid = '2330'
active_view = False
long_fee = 0.001425
short_fee = 0.001425+0.003
indicators = []
hold_coids = []
hold_unit = []
prc_basedate = pandas.DataFrame(columns=['zdate','coid'])
macro_basedate = pandas.DataFrame(columns=['zdate'])
basic_info = pandas.DataFrame(columns=['zdate','coid'])
fin_cover = pandas.DataFrame(columns=['zdate','coid'])
findata_all = pandas.DataFrame(columns=['zdate','coid'])
all_date_data = pandas.DataFrame(columns=['zdate','coid'])
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
hash_range = {}
acc_code = []
acc_code_name = []
all_mdate_list = numpy.array([numpy.datetime64('today')])
all_zdate_list = numpy.array([numpy.datetime64('today')])
simple_roi_data = pandas.DataFrame(columns=['zdate','coid'])
benchmark_roi = pandas.DataFrame(columns=['zdate','coid'])
maxdrawback = [0,0,0]
retrain_model = {}
applied = False
by_unit = False
check_columns_relation = {}
check_columns = {}
back_interval = []
check_correlation = {}
announceTable = 'TWN/AIFINQA'
findataTable = 'TWN/AIFINQ'
activeAnnounceTable = 'TWN/AINVFINQA'
activeFindataTable='TWN/AINVFINQ'
accountData = 'na'
activeAccountData = 'na'
# TODO(zyx) 有版本別與無版本別的科目需要mapping
transfer_acccode_list = [{'acc_code':'0400','cname':'不動產及設備淨額','new_acc_code':'BP51','new_name':'固定資產'},{'acc_code':'0820','cname':'無形資產','new_acc_code':'BP53','new_name':'無形資產'},{'acc_code':'032A','cname':'投資性不動產淨額','new_acc_code':'BV31','new_name':'投資性不動產'},{'acc_code':'2110','cname':'普通股股本','new_acc_code':'BF41','new_name':'普通股股本'},{'acc_code':'2310','cname':'資本公積合計','new_acc_code':'BF42','new_name':'資本公積'},{'acc_code':'2341','cname':'保留盈餘','new_acc_code':'BF43','new_name':'保留盈餘'},{'acc_code':'2120','cname':'特別股股本','new_acc_code':'BF44','new_name':'特別股股本'},{'acc_code':'2900','cname':'非控制權益','new_acc_code':'BF45','new_name':'非控制權益'},{'acc_code':'2480','cname':'其他權益','new_acc_code':'BF99','new_name':'其他權益'},{'acc_code':'0100','cname':'流動資產','new_acc_code':'BSCA','new_name':'流動資產合計'},{'acc_code':'0960','cname':'非流動資產','new_acc_code':'BSNCA','new_name':'非流動資產合計'},{'acc_code':'0010','cname':'資產總額','new_acc_code':'BSTA','new_name':'資產總計'},{'acc_code':'1100','cname':'流動負債','new_acc_code':'BSCL','new_name':'流動負債合計'},{'acc_code':'1800','cname':'非流動負債','new_acc_code':'BSNCL','new_name':'非流動負債合計'},{'acc_code':'1000','cname':'負債總額','new_acc_code':'BSTL','new_name':'負債總額'},{'acc_code':'2000','cname':'股東權益總額','new_acc_code':'BSSE','new_name':'股東權益總計'},{'acc_code':'3100','cname':'營業收入','new_acc_code':'IP11','new_name':'營業收入'},{'acc_code':'3200','cname':'營業成本','new_acc_code':'IP21','new_name':'營業成本'},{'acc_code':'3300','cname':'營業費用','new_acc_code':'IP31','new_name':'營業費用'},{'acc_code':'3910','cname':'所得稅費用','new_acc_code':'IP51','new_name':'所得稅'},{'acc_code':'3501','cname':'財務成本','new_acc_code':'IF11','new_name':'利息支出'},{'acc_code':'3900','cname':'稅前淨利','new_acc_code':'ISIBT','new_name':'利潤總額'},{'acc_code':'3970','cname':'合併總損益','new_acc_code':'ISNI','new_name':'淨利潤'},{'acc_code':'3950','cname':'歸屬母公司淨利(損)','new_acc_code':'ISNIP','new_name':'母公司淨利'},{'acc_code':'3990','cname':'每股盈餘','new_acc_code':'EPS','new_name':'母公司每股盈餘'},{'acc_code':'3295','cname':'營業毛利','new_acc_code':'GM','new_name':'毛利'},{'acc_code':'3395','cname':'營業利益','new_acc_code':'OPI','new_name':'營業利益'},{'acc_code':'R531','cname':'常續性利益','new_acc_code':'RI','new_name':'常續性利益'},{'acc_code':'7910','cname':'期初現金及約當現金','new_acc_code':'CSBCH','new_name':'期初現金及等價物淨額'},{'acc_code':'7210','cname':'來自營運之現金流量','new_acc_code':'CSCFO','new_name':'營運產生現金流量'},{'acc_code':'7300','cname':'投資活動之現金流量','new_acc_code':'CSCFI','new_name':'投資產生現金流量'},{'acc_code':'7400','cname':'籌資活動之現金流量','new_acc_code':'CSCFF','new_name':'融資產生現金流量'},{'acc_code':'7920','cname':'期末現金及約當現金','new_acc_code':'CSECH','new_name':'期末現金及等價物淨額'}]    
active_list = []
not_active_list = []

macro_mapping_coids = {
   'GLOBAL/GIDX':{
    'cname':'cname',
    'val':'val',   
    'coid_table':None,
    'coid_list':{'台灣加權股價指數': 'SB01',
             '美國紐約道瓊工業平均數': 'SB03',
             '日本東京日經225指數': 'SB04',
             '香港恆生指數': 'SB05',
             '香港恆生中國企業指數': 'SB0502',
             '香港恆生綜合指數': 'SB0503',
             '香港恆生中資指數': 'SB0504',
             '新加坡富時海峽時報指數': 'SB07',
             '德國DAX指數': 'SB08',
             '泰國曼谷SET股價指數': 'SB09',
             '泰國曼谷SET50股價指數': 'SB0902',
             '泰國曼谷SET100股價指數': 'SB0903',
             '馬來西亞吉隆坡綜合股價指數': 'SB10',
             '奧地利ATX指數': 'SB1080',
             '丹麥OMXC20指數': 'SB1081',
             '芬蘭OMXHelsinki指數': 'SB1082',
             '捷克PX指數': 'SB1083',
             '沙烏地阿拉伯TASI指數': 'SB1084',
             '科威特KWSE指數': 'SB1085',
             '卡達QEGeneral指數': 'SB1086',
             '菲律賓馬尼拉綜合股價指數': 'SB11',
             '韓國綜合股價指數': 'SB12',
             '韓國KOSPI200指數': 'SB1201',
             '南韓KOSDAQ指數': 'SB1202',
             '加拿大-多倫多綜合股價指數': 'SB14',
             '法國巴黎SBF250股價指數': 'SB15',
             '英國倫敦金融時報一百種股價指數': 'SB16',
             '臺灣加權股價指數月均': 'SB19',
             'OTC指數月均值': 'SB1902',
             'OTC指數月底值': 'SB2002',
             '美國紐約綜合股價指數': 'SB21',
             '美國綜合股價指數': 'SB22',
             '美國紐約史坦普爾500股價指數': 'SB23',
             '日本東京東證股價指數': 'SB24',
             '日本東京東證二部股價指數': 'SB2402',
             '澳洲雪梨ASX200股價指數': 'SB2501',
             '澳洲雪梨ASX300股價指數': 'SB2502',
             '澳洲雪梨綜合股價指數': 'SB2503',
             '比利時布魯塞爾綜合股價指數': 'SB27',
             '荷蘭阿姆斯特丹Aex股價指數': 'SB28',
             '西班牙馬德里綜合股價指數': 'SB30',
             '南非約翰尼斯堡綜合股價指數': 'SB31',
             '義大利米蘭FTSEMIB指數': 'SB3301',
             '義大利富時FTSEITALIAALLSHARE指數': 'SB3302',
             '美國NASDAQComposite指數': 'SB56',
             '美國NASDAQ100': 'SB5602',
             '美國NASDAQComputer指數': 'SB57',
             '墨西哥IPC指數': 'SB58',
             '墨西哥MexicoInmexIndex指數': 'SB5802',
             '墨西哥MexicoImc30Index指數': 'SB5803',
             '美國羅素Russell2000': 'SB60',
             '巴西聖保羅Bovesp指數': 'SB61',
             '巴西bov/valscap指數': 'SB6102',
             '巴西IBX指數': 'SB6105',
             '巴西IBX-50指數': 'SB6106',
             '智利IPSA指數': 'SB62',
             '阿根廷Merval指數': 'SB63',
             '中國上海A股指數': 'SB64',
             '中國上海B股指數': 'SB65',
             '中國上海綜合股價指數': 'SB66',
             '中國上證50指數': 'SB6603',
             '中國深圳A股指數': 'SB67',
             '中國深圳B股指數': 'SB68',
             '中國深圳綜合股價指數': 'SB6902',
             '中國滬深300指數': 'SB6903',
             '中國深圳成份股指數': 'SB6904',
             '中國深圳成份A股指數': 'SB6905',
             '中國深圳成份B股指數': 'SB6906',
             '印尼雅加達JSX指數': 'SB70',
             '美國費城半導體指數': 'SB71',
             '瑞士蘇黎世市場指數SMI': 'SB72',
             '瑞典斯德哥爾摩股價指數': 'SB73',
             '挪威奧斯陸OBX股價指數': 'SB74',
             '挪威奧斯陸OBXPRICEINDEX': 'SB7401',
             '法國巴黎CAC40指數': 'SB75',
             '紐西蘭威靈頓NZSE--50股價指數': 'SB76',
             '紐西蘭威靈頓NZX--15股價指數': 'SB7602',
             '印度孟買100股價指數': 'SB7702',
             '印度孟買200股價指數': 'SB7703',
             '印度孟買500股價指數': 'SB7704',
             '印度孟買mumbaisensex30股價指數': 'SB7705',
             '莫斯科10股價指數': 'SB7802',
             '俄羅斯RTS股價指數': 'SB7803',
             '莫斯科MICEXINDEX指數': 'SB7805',
             'MSCI-AC世界指數(Local)': 'SB79',
             'MSCI-AC世界指數USD': 'SB7902',
             'MSCI台股(Local)': 'SB7903',
             'MSCI-AC世界指數-ERU': 'SB7904',
             'MSCI-新興市場金磚四國指數(Local)': 'SB7905',
             'MSCI-新興金磚四國指數以美元計價': 'SB7906',
             'MSCI-新興市場EmergingMarkets以Local計價': 'SB7907',
             'MSCI-新興市場EmergingMarkets以美元計價': 'SB7908',
             'MSCI-新興市場亞洲以Local計價': 'SB7909',
             'MSCI-新興市場亞洲以美元計價': 'SB7910',
             'MSCI-新興市場歐洲以Local計價': 'SB7911',
             'MSCI-新興市場歐洲以美元計價': 'SB7912',
             'MSCI-新興市場遠東地區以Local計價': 'SB7915',
             'MSCI-新興市場遠東地區以美元計價': 'SB7916',
             'MSCI-新興前緣市場指數以Local計價': 'SB7917',
             'MSCI-新興前緣市場指數以美元計價': 'SB7918',
             'MSCI-新興前緣亞洲市場以Local計價': 'SB7919',
             'MSCI-新興前緣亞洲市場以美元計價': 'SB7920',
             'MSCI已開發EAFE(歐洲澳洲遠東紐西蘭)Local': 'SB7921',
             'MSCI-已開發EAFE(歐洲澳洲遠東紐西蘭)USD': 'SB7922',
             'MSCI-已開發國家遠東市場指數以Local計價': 'SB7925',
             'MSCI-已開發國家遠東市場以美元計價': 'SB7926',
             'MSCI拉丁美洲指數-美元計價': 'SB7935',
             'MSCI拉丁美洲指數-歐元計價': 'SB7936',
             'MSCI拉丁美洲指數-Local價': 'SB7937',
             'MSCI中國指數(美元計)': 'SB7944',
             'MSCI中國指數(歐元計)': 'SB7945',
             'MSCI中國指數(Local)': 'SB7946',
             'MSCI-DM世界指數(已開發市場)-USD': 'SB7948',
             'MSCI-DM世界指數-(已開發市場)-(EUR)': 'SB7949',
             'MSCI-DM世界指數-(已開發市場)-((Local)': 'SB7950',
             'MSCI-已開發亞太平洋(除日本)指數Local': 'SB7951',
             'MSCI-已開發亞太平洋(除日本)指數USD': 'SB7952',
             'MSCI-已開發亞太平洋(除日本)指數EUR': 'SB7953',
             'MSCI-已開發市場歐洲指數-LOCAL': 'SB7954',
             'MSCI-已開發市場歐洲指數-USD': 'SB7955',
             'MSCI-已開發市場歐洲指數-Eur': 'SB7956',
             'MSCI-新興市場東歐指數-Local': 'SB7957',
             'MSCI-新興市場東歐指數-USD': 'SB7958',
             'MSCI-新興市場東歐指數-EUR': 'SB7959',
             'MSCI-印度指數-EM新興市場(Local)': 'SB7960',
             'MSCI-印度指數-EM新興市場-USD': 'SB7961',
             'MSCI-印度指數-EM新興市場-EUR': 'SB7962',
             'MSCI-DM北美指數USD': 'SB7973',
             '阿布達比證券市場指數': 'SB80',
             '越南胡志明易證券指數': 'SB81',
             '芝加哥擇權交易所波動率指數(恐慌指標)': 'SB82',
             '盧森堡LuxXIndex指數': 'SB83',
             '盧森堡LuxXReturn': 'SB8302',
             '百慕達BeremudaStockExchange指數': 'SB86',
             '美元指數(現貨)': 'SB89',
             '泰德價差': 'SB90',
             '土耳其ISE國家100指數': 'SB91',
             '土耳其ISE國家30指數': 'SB9102',
             '土耳其DJTurkeyTitans20': 'SB9103',
             '': '',
             '泛歐道瓊600指數': 'SB92',
             '希臘ASE指數': 'SB93',
             '葡萄牙PSI指數': 'SB9303',
             '愛爾蘭Overall指數': 'SB9305',
             'MSCI美國房地產信託投資基金指數': 'SB96',
             'S&P/TSX房地產信託投資基金指數': 'SB9602',
             '波蘭WIG指數': 'SB97',
             '匈牙利BUX指數': 'SB98',
             '北歐指數USD': 'SB99',
             '北歐指數EUR': 'SB9902',
             '北歐指數LOCAL': 'SB9903',
             '東協指數USD': 'SB9904',
             '東協指數EUR': 'SB9905',
             '東協指數LOCAL': 'SB9906',
             '金龍指數USD': 'SB9907',
             '金龍指數EUR': 'SB9908',
             '金龍指數LOCAL': 'SB9909',
             '歐菲中東指數USD': 'SB9910',
             '歐菲中東指數EUR': 'SB9911',
             '歐菲中東指數LOCAL': 'SB9912'}
   },
   'GLOBAL/GCURR':{
    'cname':'cname',
    'val':'close_ny',   
    'coid_table':None,
    'coid_list':{
            '南非蘭特': 'ZAR',
             '越南盾': 'VND',
             '烏拉圭比索': 'UYU',
             '美元': 'USD',
             '烏克蘭格里夫納': 'UAH',
             '新台幣': 'TWD',
             '土耳其里拉': 'TRL',
             '泰銖': 'THB',
             '新加坡元': 'SGD',
             '瑞典克朗': 'SEK',
             '特別提款權': 'SDR',
             '沙特里亞爾': 'SAR',
             '俄羅斯盧布': 'RUB',
             '羅馬尼亞列伊': 'RON',
             '卡達里亞爾': 'QAR',
             '波蘭茲羅提': 'PLN',
             '巴基斯坦盧比': 'PKR',
             '菲律賓披索': 'PHP',
             '阿曼里亞爾': 'OMR',
             '紐西蘭元': 'NZD',
             '挪威克朗': 'NOK',
             '馬來西亞令吉': 'MYR',
             '墨西哥比索': 'MXN',
             '澳門幣': 'MOP',
             '斯里蘭卡盧比': 'LKR',
             '哈薩克斯坦堅戈': 'KZT',
             '科威特戴納': 'KWD',
             '韓圓': 'KRW',
             '日圓': 'JPY',
             '冰島克朗': 'ISK',
             '印度盧比': 'INR',
             '以色列塞克': 'ILS',
             '印尼盾': 'IDR',
             '匈牙利福林': 'HUF',
             '克羅埃西亞庫納': 'HRK',
             '港元': 'HKD',
             '英鎊': 'GBP',
             '歐元': 'EUR',
             '埃及鎊': 'EGP',
             '厄瓜多蘇克雷': 'ECS',
             '丹麥克朗': 'DKK',
             '捷克克朗': 'CZK',
             '哥倫比亞比索': 'COP',
             'CNYM': 'CNYM',
             '人民幣元': 'CNY',
             '智利比索': 'CLP',
             '瑞士法郎': 'CHF',
             '加拿大元': 'CAD',
             '巴西雷亞爾': 'BRL',
             '巴林戴納': 'BHD',
             '保加利亞列弗': 'BGL',
             '澳大利亞元': 'AUD',
             '阿根廷比索': 'ARS'
            }
    }
}