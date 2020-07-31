**STOCH**  
功能概述：計算隨機震盪指標 Stochastic Oscillator，即KD指標。
範例：ta.STOCH(df.high_d, df.low_d, df.close_d)
說明：參數說明：<ul>
<li>high:(numpy array) 日股價最高價</li>
<li>low:(numpy array) 日股價最低價</li>
<li>close:(numpy array) 日股價收盤價</li>
<li>fastk_period:(int,5) 快速k平均日數, </li>
<li>slowk_period:(int,3) 慢速k平均日數</li>
<li>slowd_period:(int,3) 快速d平均日數</li>
</ul>  
  