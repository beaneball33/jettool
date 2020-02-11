import tejapi
import pandas
import numpy
import requests
import json

class db_attr(object):
    def __init__(self):
        print('存取tejapi資料全屬性')

    def set_tablelist(self,tables):
        # 儲存該使用者能存取的所有table的資訊
        
        for table_name in tables:
            name_list = table_name.split('/')
            market = name_list[0]
            table = name_list[1]
            if self.api_tables.get(market) is None:
                self.api_tables[market] = [table]
            else:
                self.api_tables[market].append(table)
        self.set_market(self.market)
        
    def get_category(self):
        # 查詢資料分類
        
        list_names = ('https://api.tej.com.tw/info/category/list?api_key='
                      +self.tejapi.ApiConfig.api_key)
        response = requests.get(list_names)
        data = json.loads(response.text)['result']
        self.category_list = { data[k].get('categoryId'):data[k] for k in data}
        return self.category_list
    def get_tables(self,market=None):
        # 查詢按國別分類的資料表完整清單

        table_names = ('https://api.tej.com.tw/info/tables/list?api_key='
                       +self.tejapi.ApiConfig.api_key)
        response = requests.get(table_names)
        data = json.loads(response.text)['result']
        self.table_list = {}
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
            self.table_list[market_code] = this_market_table
        df = None
        if market is None:            
            for market in self.table_list:
                if df is None:
                    df = pandas.DataFrame.from_dict(self.table_list.get(market),
                                                    orient='index')
                else:
                    df = df.append(
                            pandas.DataFrame.from_dict(
                                    self.table_list.get(market),
                                                orient='index'),sort=False)
            return df 
        else:
            df = pandas.DataFrame.from_dict(self.table_list.get(market),orient='index')
            
        return df.rename(columns={'id':'資料集名稱','dbCode':'國別碼',
                                  'tableCode':'資料表代碼','name':'名稱',
                                  'description':'描述','enabled':'存取權限',
                                  'frequency':'頻率'})
    def get_market(self):
        # 查詢所有的國別
        
        db_names = ('https://api.tej.com.tw/info/database/list?api_key='
                    +self.tejapi.ApiConfig.api_key)
        response = requests.get(db_names)
        data = json.loads(response.text)['result']
        self.market_list = data
        return self.market_list
    def set_query_ordinal(self):
        #按照category_list中的順序，將可查詢的表拼湊
        tempordinal = []
        
        # 加入交易屬性類，以頻率決定


        for subcategory in self.category_list.get(4).get('subs'):
            if len(subcategory.get('tableMap'))>0:
                for table_attr in subcategory.get('tableMap'):
                    if table_attr.get('dbCode') == self.market:
                        table_id = table_attr.get('tableId').split('/')[1]
                        if table_id in self.api_tables.get(self.market):
                            table_data = self.table_list.get(self.market)
                            data_freq = table_data.get(table_id).get('frequency')
                            if data_freq  in self.all_prc_dataset_freq:
                                tempordinal.append(table_id)
        # 加入基本屬性類，需要正面表類
        for subcategory in self.category_list.get(3).get('subs'):
            if len(subcategory.get('tableMap'))>0:
                for table_attr in subcategory.get('tableMap'):
                    if table_attr.get('dbCode') == self.market:
                        table_id = table_attr.get('tableId').split('/')[1]
                        if table_id in self.api_tables.get(self.market):
                            if table_id in self.mdate_name_dict.keys():
                                mdate_dict = self.mdate_name_dict.get(table_id)
                                frequency = mdate_dict.get('frequency')
                                if frequency  in self.all_prc_dataset_freq:
                                    tempordinal.append(table_id)
        self.all_prc_dataset = tempordinal
        
    def get_table_mapping(self,market='TWN',id='AIND'):
        #根據已知的table名稱，查詢mapping
        for catefory_index in self.category_list:
            for tableMap in self.category_list[catefory_index]['subs']:
                for table in tableMap.get('tableMap'):
                    if market in table.get('dbCode') and market+'/'+id in table.get('tableId'):
                        return tableMap
                    
    def search_column(self,keyword='報酬率',condition='and',current_market=True):

        k_name_list = keyword.split(' ')
        k_name = k_name_list[0]
        match_dict = { search['tableId']:search for search in self.tejapi.search_table(k_name)}
        match_outcome = []
        for i in range(0,len(k_name_list)):
            for match_table in match_dict:
                for columns in match_dict.get(match_table).get('columns'):
                    cname = columns.get('cname')
                    if k_name in cname:
                        match_table_code = match_table.split('/')[1]
                        match_table_market = match_table.split('/')[0]
                        if current_market is False or match_table_market==self.market:
                            match_outcome.append([cname,
                                                  match_dict.get(match_table).get('tableName'),
                                                  match_table])
            if condition =='and':
                break
        match_df = pandas.DataFrame(match_outcome,columns=['cname','tableName','tableCode'])
        if condition =='and':
            for i in range(1,len(k_name_list)):
                if len(match_df)>0:
                    for j,row in match_df.iterrows():
                        if k_name_list[i] not in match_df.loc[j,'cname']:
                            match_df.loc[j,'tableCode'] = None
        match_df = match_df.dropna().reset_index(drop=True)
        current_market
        return match_df