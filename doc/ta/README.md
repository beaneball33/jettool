**STOCH**  
功能概述：計算隨機震盪指標 Stochastic Oscillator，即KD指標。
使用範例：
```
ta.STOCH(df.high_d, df.low_d, df.close_d)
```
說明：參數說明：<ul>
<li>high:(numpy array, not None) 日股價最高價</li>
<li>low:(numpy array, not None) 日股價最低價</li>
<li>close:(numpy array, not None) 日股價收盤價</li>
<li>fastk_period:(int,5) 快速k區間日數, </li>
<li>slowk_period:(int,3) 慢速k區間日數</li>
<li>slowd_period:(int,3) 快速d區間日數</li>
</ul>  



**MACD**  
功能概述：計算指數平滑移動平均線 Moving Average Convergence / Divergence。
使用範例：
```
ta.MACD(df.close_d)
```
說明：參數說明：<ul>
<li>close:(numpy array, not None) 日股價收盤價</li>
<li>fastperiod:(int,12) 快線區間日數, </li>
<li>slowperiod:(int,26) 慢線區間日數</li>
<li>signalperiod:(int,9) 訊號區間日數</li>
</ul>  


**RSI**  
功能概述：計算相對強弱指標 Relative Strength Index。
使用範例：
```
ta.RSI(df.close_d)
```
說明：參數說明：<ul>
<li>close:(numpy array, not None) 日股價收盤價</li>
<li>timeperiod:(int,14) 快線區間日數, </li>
</ul>  


**MOM**  
功能概述：計算運動量指標 Momentum。
使用範例：
```
ta.MOM(df.close_d)
```
說明：參數說明：<ul>
<li>close:(numpy array, not None) 日股價收盤價</li>
<li>roi:(numpy array) 日股價報酬率，若停工此資料，則改以此做為報酬率，可考慮除權息問題。</li>
<li>timeperiod:(int,10) 快線區間日數, </li>
</ul>  
