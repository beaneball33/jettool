import pandas
import numpy
#http://www.tadoc.org/indicator/MOM.htm
date_name = 'date'
coid_name = 'coid'


def merge_output(df, coid_date_array, window):

    if type(coid_date_array) is pandas.core.indexes.datetimes.DatetimeIndex:
        df[date_name] = coid_date_array
        df = df.set_index('date')
        df[0:window] = numpy.nan
    else:
        df = coid_date_array.merge(df, on=[coid_name, date_name], how='left')
        df['is_start'] = df['is_start'].fillna(0)
        df['drop_index'] = df['is_start'].rolling(window).sum()
        df['drop_index'] = df['drop_index'].fillna(1)
        col_name = numpy.setdiff1d(df.columns, [coid_name, date_name]).tolist()
        df.loc[df['drop_index']>0, col_name] = numpy.nan
    return df

def combine_data(data_list:dict):
    data = None
    for dataname in data_list:
        if data is None:
            coid_date_array = data_list.get(dataname).index

            data = pandas.DataFrame(data_list.get(dataname), columns=[dataname])
            if type(coid_date_array) is pandas.core.indexes.datetimes.DatetimeIndex:
                data['date'] = pandas.DatetimeIndex(coid_date_array)
                data = data.set_index('date')
        else:
            data[dataname] = data_list.get(dataname)
    data[date_name] = '2001-01-01'
    data[coid_name] = 'na'
    
    return data.reset_index(drop=True), coid_date_array

def process_sample(data, columns:list):
    pks = [coid_name, date_name]
    if isinstance(data, pandas.DataFrame):        
        coid_date_array = data[[coid_name, date_name]]
        
        columns = pks + columns
        df = data.loc[:, columns].sort_values(by=pks).reset_index(drop=True)
        start_date = coid_date_array.groupby(coid_name).min()[date_name].reset_index()
        start_date['is_start'] = 1
        coid_date_array = coid_date_array.merge(start_date, on=[coid_name, date_name], how='left')
    elif isinstance(data, dict):
        
        df, coid_date_array = combine_data(data)
            
    return df, coid_date_array
    
def MOM(close=None, roi=None, data=None, timeperiod = 10):
    col_names = ['close']
    if data is None:
        data = {'close':close}
        if roi is not None:
            data['roi']=roi
            col_names += ['roi']
    df, coid_date_array = process_sample(data, col_names)
    
    if 'roi' in df.columns: 
        mom = df['roi'].rolling(timeperiod).sum() - numpy.equal(df['index'] , 0) * df['roi']
        df['mot'] = df['close'] * (1 - 1 / (1+mom/100))
    else:
        mom = df['close'].values[timeperiod:len(df)] - df['close'].values[0:len(df) - timeperiod] 
        df['mot'] = numpy.append(numpy.array([numpy.nan] * timeperiod), mom)
        
    df = merge_output(df, coid_date_array, window=timeperiod)
    return df[['mot']]

def RSI(close=None, data=None, timeperiod = 14):
    
    if data is None:
        data = {'close':close}
    df, coid_date_array = process_sample(data, ['close','roi'])

    if 'roi' in df.columns: 
        diff = numpy.append([numpy.nan], df['close'].values[:-1] * (numpy.exp(df['roi'].values[1:]/100) - 1))
    else:
        diff = df['close'].diff()
    df['diff'] = diff
    if 'is_start' in df.columns:
        df.loc[df['is_start']==True, 'diff']=numpy.nan
        df = df.dropna(subset=['diff'])
    df['up'] = 0
    df.loc[df['diff']>=0, 'up'] = 1
    df['up'] = df['up'] * df['diff']
    df['down'] = 0
    df.loc[df['diff']<0, 'down'] = -1
    df['down'] = df['down'] * df['diff']
    
    df['ema_u'] = df['up'].ewm(timeperiod-1).mean()
    df['ema_d'] = df['down'].ewm(timeperiod-1).mean()
    rs = df['ema_u'] / df['ema_d']
    df['rsi'] = 100 * df['ema_u'] / (df['ema_u'] + df['ema_d'])
    df = merge_output(df, coid_date_array, window=timeperiod)
    return df[['rsi']]

def MACD(close=None, data=None, fastperiod = 12, slowperiod = 26, signalperiod = 9):

    if data is None:
        data = {'close':close}
    df, coid_date_array = process_sample(data, ['close'])
    
    ema_fast = df['close'].ewm(span=fastperiod).mean()
    ema_slow = df['close'].ewm(span=slowperiod).mean()
    
    df['macd'] = ema_fast- ema_slow
    df['macdsignal'] = df['macd'].ewm(span=signalperiod).mean()
    df['macdhist'] = df['macd'] - df['macdsignal']
    timeperiod = max([fastperiod, slowperiod]) + signalperiod
    df = merge_output(df,coid_date_array,window=timeperiod)
    return df[['macd','macdsignal','macdhist']]

def STOCH(high=None, low=None, close=None, data=None, fastk_period=5, slowk_period=3, slowd_period=3, alpha=1/3):

    if data is None:
        data = {'high':high,'low':low,'close':close}
    df, coid_date_array = process_sample(data,['high','low','close'])

    k_9_high = df['high'].rolling(fastk_period).max()
    k_9_low = df['low'].rolling(fastk_period).min()
    df['rsv'] = (df['close'].values - k_9_low)*100/(k_9_high - k_9_low)
    df['k'] = df['rsv'].rolling(slowk_period).mean()
    df['d'] = df['k'].rolling(slowd_period).mean()
    timeperiod = max([fastk_period, slowk_period, slowd_period])
    df = merge_output(df, coid_date_array, window=timeperiod)
    return df[['k','d']]

'''
要補上roi產生
'''
def BBANDS(close=None, roi=None, data=None, nbdevup = 1, nbdevdn = 1, timeperiod = 20):
    
    if data is None:
        data = {'close':close, 'roi':roi}
    df, coid_date_array = process_sample(data, ['close','roi'])
    
    closed_mean = df['close'].rolling(window=timeperiod).mean()
    #注意看前幾列，數值特別小，但不會發生error
    roi_x2 = numpy.power(df['roi']/100, 2).rolling(window=timeperiod).sum()
    roi_mean = df['roi'].rolling(window=timeperiod).mean()/100
    roi_vol  = numpy.sqrt(roi_x2/timeperiod - numpy.power(roi_mean, 2))

    df['BBANDup'] = closed_mean * numpy.exp(roi_vol * nbdevup)
    df['BBANDdown'] = closed_mean * numpy.exp(-1 * roi_vol * nbdevdn)
    df = merge_output(df, coid_date_array, window=timeperiod)
    return df[['BBANDup', 'BBANDdown']]   
