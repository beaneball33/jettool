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
        
        list_names = 'https://api.tej.com.tw/info/category/list?api_key='+self.tejapi.ApiConfig.api_key
        response = requests.get(list_names)
        data = json.loads(response.text)['result']
        self.category_list = { data[k].get('categoryId'):data[k] for k in data}
        
    def get_tables(self):
        # 查詢按國別分類的資料表完整清單
    
        table_names = 'https://api.tej.com.tw/info/tables/list?api_key='+self.tejapi.ApiConfig.api_key
        response = requests.get(table_names)
        data = json.loads(response.text)['result']
        self.table_list = {}
        for market in data.keys():
            this_market_table = {table_attr['tableCode']:table_attr   for table_attr in data.get(market)}
            
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
            self.table_list[market] = this_market_table
            
    def get_market(self):
        # 查詢所有的國別
        
        db_names = 'https://api.tej.com.tw/info/database/list?api_key='+self.tejapi.ApiConfig.api_key
        response = requests.get(db_names)
        data = json.loads(response.text)['result']
        self.market_list = data
    
    def set_query_ordinal(self):
        #按照category_list中的順序，將可查詢的表拼湊
        tempordinal = []
        all_prc_dataset_freq = ['D','W','S','M','Y']
        # 加入交易屬性類，以頻率決定
        for subcategory in self.category_list.get(4).get('subs'):
            if len(subcategory.get('tableMap'))>0:
                for table_attr in subcategory.get('tableMap'):
                    if table_attr.get('dbCode') == self.market:
                        table_id = table_attr.get('tableId').split('/')[1]
                        if table_id in self.api_tables.get(self.market):
                            data_freq = self.table_list.get(self.market).get(table_id).get('frequency')
                            if data_freq  in all_prc_dataset_freq:
                                tempordinal.append(table_id)
        # 加入基本屬性類，需要正面表類
        for subcategory in self.category_list.get(3).get('subs'):
            if len(subcategory.get('tableMap'))>0:
                for table_attr in subcategory.get('tableMap'):
                    if table_attr.get('dbCode') == self.market:
                        table_id = table_attr.get('tableId').split('/')[1]
                        if table_id in self.api_tables.get(self.market):
                            if table_id in self.mdate_name_dict.keys():
                                frequency = self.mdate_name_dict.get(table_id).get('frequency')
                                if frequency  in all_prc_dataset_freq:
                                    tempordinal.append(table_id)
        self.all_prc_dataset = tempordinal