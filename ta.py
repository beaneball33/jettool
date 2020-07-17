import pandas
import numpy
#http://www.tadoc.org/indicator/MOM.htm
date_name = 'zdate'
coid_name = 'coid'

def merge_output(df,coid_date_array):
    if len(coid_date_array)>0:
        df = coid_date_array.merge(df,on=[coid_name,date_name],how='left')
    return df

def combine_data(data_list:dict):
    data = None
    for dataname in data_list:
        if data is None:
            data = pandas.DataFrame(data_list.get(dataname),columns=[dataname])
        else:
            data[dataname] = data_list.get(dataname)
    data[date_name] = '2001-01-01'
    data[coid_name] = 'na'
    
    return data.reset_index(drop=True)

def process_sample(data,columns:list):
    pks = [coid_name,date_name]
    if isinstance(data,pandas.DataFrame):        
        coid_date_array = data[['coid',date_name]]
        
        columns = pks + columns
        df = data.loc[:,columns].sort_values(by=pks).reset_index(drop=True)
                
    elif isinstance(data,dict):
        coid_date_array = pandas.DataFrame([],columns=pks)
        df = combine_data(data)
        
    return df,coid_date_array
    
def MOM(close=None,roi=None,data=None,timeperiod = 9,method='roi'):
    if data is None:
        data = {'close_d':close,'roi':roi}
    df, coid_date_array = process_sample(data,['close_d','roi'])
    
    if method=='roi':
        df['mo'] = df['roi'].rolling(timeperiod).sum() - numpy.equal(df['index'],0)*df['roi']
        df['mot'] = df['close_d']*(1 - 1/(1+df['mo']/100))
    else:
        mom = df['close_d'].values[timeperiod:len(df)] - df['close_d'].values[0:len(df)-timeperiod] 
        df['mot'] = numpy.append(numpy.array([numpy.nan]*timeperiod),mom)
        
    df = merge_output(df,coid_date_array)
    return df[['mot']]

def RSI(roi=None,data=None,timeperiod = 6):

    if data is None:
        data = {'roi':roi}
    df, coid_date_array = process_sample(data,['roi'])

    df['up'] = 0
    df.loc[df['roi']>=0,'up'] = 1
    df['up'] = df['up']*df['roi']
    df['down'] = 0
    df.loc[df['roi']<0,'down'] = -1
    df['down'] = df['down']*df['roi']
    df['ema_u'] = df['up'].rolling(timeperiod).mean()
    df['ema_d'] = df['down'].rolling(timeperiod).mean()
    df['rsi'] = df['ema_u']/(df['ema_u']+df['ema_d'])
    df = merge_output(df,coid_date_array)
    return df[['rsi']]

def MACD(close=None,data=None,fastperiod = 12,slowperiod = 26,signalperiod = 9):

    if data is None:
        data = {'close_d':close}
    df, coid_date_array = process_sample(data,['close_d'])
    
    df['dif'] = df['close_d'].rolling(fastperiod).mean() - df['close_d'].rolling(slowperiod).mean()
    df['macd'] = df['dif'].rolling(signalperiod).mean()
    df = merge_output(df,coid_date_array)
    return df[['dif','macd']]

def STOCH(high=None,low=None,close=None,data=None,fastk_period=5, slowk_period=3, slowd_period=3,alpha = 1/3):

    if data is None:
        data = {'high_d':high,'low_d':low,'close_d':close}
    df, coid_date_array = process_sample(data,['high_d','low_d','close_d'])

    zdate = df[[date_name]].drop_duplicates().sort_values(by=[date_name],ascending=False).reset_index(drop=True).reset_index()
    df = df.merge(zdate,on=[date_name],how='left')
    k_9_high = df['high_d'].rolling(fastk_period).max()
    k_9_low = df['low_d'].rolling(fastk_period).min()
    df['rsv'] = (df['close_d'].values - k_9_low)*100/(k_9_high - k_9_low)
    df['k'] = df['rsv'].rolling(slowk_period).mean()
    df['d'] = df['k'].rolling(slowd_period).mean()
    df = merge_output(df,coid_date_array)
    return df[['k','d']]

'''
要補上roi產生
'''
def BBANDS(close=None,roi=None,data=None,nbdevup = 1,nbdevdn = 1,timeperiod = 20):
    
    if data is None:
        data = {'close_d':close,'roi':roi}
    df, coid_date_array = process_sample(data,['close_d','roi'])
    
    closed_mean = df['close_d'].rolling(window=timeperiod).mean()
    #注意看前幾列，數值特別小，但不會發生error
    roi_x2 = numpy.power(df['roi']/100,2).rolling(window=timeperiod).sum()
    roi_mean = df['roi'].rolling(window=timeperiod).mean()/100
    roi_vol  = numpy.sqrt(roi_x2/timeperiod - numpy.power(roi_mean,2))

    df['BBANDup'] = closed_mean*numpy.exp(roi_vol*nbdevup)
    df['BBANDdown'] = closed_mean*numpy.exp(-1*roi_vol*nbdevdn)
    df = merge_output(df,coid_date_array)
    return df[['BBANDup','BBANDdown']]   
