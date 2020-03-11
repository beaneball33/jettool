"""
這是查詢api資料索引目錄的整合工具
"""
import tejapi
import pandas
import numpy
import requests
import json
api_key = ''

# 取得該使用者能存取的所有table的資訊
def get_info(my_key=None):
    if my_key is None:
        my_key = api_key
    tejapi.ApiConfig.api_key = my_key
    info = tejapi.ApiConfig.info()
    print_info = [
                  '使用者名稱：'+str(info.get('user').get('name'))+'('+str(info.get('user').get('shortName'))+')',
                  '使用權限日期：'+str(info.get('user').get('subscritionStartDate'))+'/'+str(info.get('user').get('subscritionEndDate')),
                  '日連線次數狀態：'+str(info.get('todayReqCount'))+'/'+str(info.get('reqDayLimit')),
                  '日查詢資料量狀態：'+str(info.get('todayRows'))+'/'+str(info.get('rowsDayLimit')),
                  '月查 詢資料量狀態：'+str(info.get('monthRows'))+'/'+str(info.get('rowsMonthLimit')),
                  ]
        
    print(print_info)
    return info  
# 取得按照索引目錄分層的完整資料表清單
def set_tablelist(tables):
        
    api_tables = {}
    for table_name in tables:
        name_list = table_name.split('/')
        market = name_list[0]
        table = name_list[1]
        if api_tables.get(market) is None:
            api_tables[market] = [table]
        else:
            api_tables[market].append(table)

    return api_tables
# 查詢所有的國別    
def get_market(my_key=None):
    if my_key is None:
        my_key = api_key
        
    db_names = ('https://api.tej.com.tw/info/database/list?api_key='
                +my_key)
    response = requests.get(db_names)
    data = json.loads(response.text)['result']
    market_list = data
    return market_list
# 查詢所有資料分類    
def get_category(my_key=None):
    if my_key is None:
        my_key = api_key 
        
    list_names = ('https://api.tej.com.tw/info/category/list?api_key='
                  +my_key)
    response = requests.get(list_names)
    data = json.loads(response.text)['result']
    category_list = { data[k].get('categoryId'):data[k] for k in data}
    return category_list
# 查詢按國別分類的資料表完整清單   
def get_tables(my_key=None):
    if my_key is None:
        my_key = api_key

    table_names = ('https://api.tej.com.tw/info/tables/list?api_key='
                   +my_key)
    response = requests.get(table_names)
    data = json.loads(response.text)['result']
    table_list = {}
    for market_code in data.keys():
        this_market_table = {}
        for table_attr in data.get(market_code):
            this_market_table[table_attr['tableCode']] = table_attr  
          
        for table in this_market_table:
            this_table = this_market_table.get(table)
            table_des = this_table.get('description').split('<br />')
            data_freq = 'U'
            for des_col in table_des:
                if '資料頻率' in des_col:
                    data_freq = 'N'
                    if '日' in des_col:
                        data_freq = 'D'
                    if '週' in des_col:
                        data_freq = 'W'
                    if '月' in des_col:
                        data_freq = 'M'
                    if '季' in des_col:
                        data_freq = 'S'
                    if '年' in des_col:
                        data_freq = 'Y'
                    break
            this_table['frequency'] = data_freq
            this_market_table[table] = this_table
        table_list[market_code] = this_market_table
    return table_list

# 根據索引目錄的資料表清單構造，回傳dataframe方便檢視
def get_tables_info(*,market='TWN',table_list={}):            
    df = None
    
    # 把所有國別資料表都回傳
    if market is None:            
        for market in table_list:
            if df is None:
                df = pandas.DataFrame.from_dict(table_list.get(market),
                                                orient='index')
            else:
                df = df.append(
                        pandas.DataFrame.from_dict(
                                table_list.get(market),
                                            orient='index'),sort=False)
        return df 
    # 僅回傳指定國別
    else:
        df = pandas.DataFrame.from_dict(table_list.get(market),orient='index')
         
    return df.rename(columns={'id':'資料集名稱','dbCode':'國別碼',
                              'tableCode':'資料表代碼','name':'名稱',
                              'description':'描述','enabled':'存取權限',
                              'frequency':'頻率'})


# 根據已知的table名稱，查詢mapping到別的國家的資料表清單
def get_table_mapping(*,my_key=None,market='TWN',category_list=None,id='AIND'):
    if my_key is None:
        my_key = api_key
    if category_list is None:
        category_list = get_category(my_key)
    
    for catefory_index in category_list:
        for tableMap in category_list[catefory_index]['subs']:
            for table in tableMap.get('tableMap'):
                if (market in table.get('dbCode') and
                    market+'/'+id in table.get('tableId')):
                    return tableMap
# 使用tejapi.search_table進行交集或聯集查詢
def search_column(*,my_key,market='TWN',keyword='報酬率',condition='and',current_market=True):
    if my_key is None:
        my_key = api_key
    tejapi.ApiConfig.api_key = my_key
    k_name_list = keyword.split(' ')
    k_name = k_name_list[0]
    match_dict = { search['tableId']:search for search in tejapi.search_table(k_name)}
    match_outcome = []
    for i in range(0,len(k_name_list)):
        for match_table in match_dict:
            for columns in match_dict.get(match_table).get('columns'):
                cname = columns.get('cname')
                if k_name in cname:
                    match_table_code = match_table.split('/')[1]
                    match_table_market = match_table.split('/')[0]
                    if current_market is False or match_table_market==market:
                        match_outcome.append([cname,
                                              match_dict.get(match_table).get('tableName'),
                                              match_table])
        if condition =='and':
            break
    match_df = pandas.DataFrame(match_outcome,
                                columns=['cname','tableName','tableCode'])
    if condition =='and':
        for i in range(1,len(k_name_list)):
            if len(match_df)>0:
                for j,row in match_df.iterrows():
                    if k_name_list[i] not in match_df.loc[j,'cname']:
                        match_df.loc[j,'tableCode'] = None
    match_df = match_df.dropna().reset_index(drop=True)
    current_market
    return match_df
    
# 取得指定表單的完整欄位表
def get_table_columns(*,my_key=None,table_name='TWN/AAPRCDA'):
    if my_key is None:
        my_key = api_key

    tejapi.ApiConfig.api_key = my_key
    columns_name = []
    table_info = tejapi.table_info(table_name)
    pk = table_info.get('primaryKey')
    columns = table_info.get('columns')
    for col in columns:
        if columns.get(col).get('name') not in pk:
            columns_name.append(columns.get(col).get('cname'))
    return columns_name