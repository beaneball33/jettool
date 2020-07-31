**revert_view**  
功能概述：取得過去資料，讓過去某一天的資料與當日的資料可以在最新一日做為一個欄位使用  
範例：tejtool.revert_view(check_index=<欄位>, jump_length = <數字>, jump_kind=<方法>, fix_datec=<日期>)   
說明：參數說明：<ul><li>check_index:(str, not None) 要取得過去資料的欄位名稱</li><li>jump_length:(str,default 1) 往過去移動的時間長度，需搭配jump_kind使用</li><li>jump_kind:(str,default 'Y')，支援以下幾種 'Y':以年為單位移動日期 'M':以月為單位 'W': 以週為單位'D':以日為單位</li><li>fix_date:(str)選擇性欄位，用來控制移動日期時，要顧定移動到哪個日期(或月/日)</li></ul>  
  
  
**calculate_moving**  
功能概述：計算移動平均、移動加總、移動窗口內最大值(最小值、中位數)  
範例：tejtool.calculate_moving(check_index=<欄位>, window = <數字>, col_kind=<方法>)   
說明：參數說明：<ul><li>check_index:(str, not None) 要進行計算的欄位名稱</li><li>window:(str,default '1d') 移動窗口長度，可輸入整數，即代表以季為移動窗口的單位，(d代表「日」,w代表「周」,m代表「月」)</li><li>col_kind:(str,default 'mean')，支援以下幾種計算 'max':最大值 'min':最小值 'mean':平均值 'median':中位數 'sum':加總</li></ul>  
  
  
**calculate_crossing**  
功能概述：計算整體股票的平均、加總、最大值、最小值、中位數  
範例：tejtool.calculate_crossing(check_index=<欄位>,window= <數字>,weight=<加權欄位>,col_kind=<方法>)   
說明：參數說明：<ul><li>check_index:(str, not None) 要進行計算的欄位名稱</li><li>window:(str,default '1d') 移動窗口長度，可輸入整數，即代表以季為移動窗口的單位，(d代表「日」,w代表「周」,m代表「月」)。</li><li>weight:(str,default None)用來加權的欄位名稱，如可指定weight='市值'，即用市值加權。</li><li>col_kind:(str,default 'max')，支援以下幾種計算 'max':最大值 'min':最小值 'mean':平均值 'median':中位數 'sum':加總</li></ul>  
  
  
**check_above**  
功能概述：欄位條件檢查  
範例：tejtool.check_above(check_index=<檢查欄位>,down_index=<下界欄位>,up_index=<上界欄位>,window=<數字>)   
說明：參數說明：<ul><li>check_index:(str, not None) 要進行檢查的欄位名稱，若check_index欄位的值大於down_index欄位的值，則為True。</li><li>down_index:(str or float, not None) 下界欄位；檢查所指定欄位的另一個欄位名稱，或是可以傳入數值。</li><li>up_index:(str or float, allow None) 上界欄位；僅使用check_between()函式時傳入。也可以直接傳入數值，此為額外增加的檢查條件的上界。</li><li>window:(int or str,default '1d') 移動窗口長度，可輸入整數，即代表以季為移動窗口的單位，可輸入整數，即代表以季為移動窗口的單位，(d代表「日」,w代表「周」,m代表「月」)</li></ul>  
  
  
**calculate_volatility**  
功能概述：計算股票的報酬率或指定欄位的波動度  
範例：tejtool.calculate_volatility(check_index=<檢查欄位>,window = <數字>,col_type=<計算波動度的方法>)   
說明：參數說明：<ul><li>check_index:(str, default '報酬率') 要進行計算的欄位名稱，若未輸入，預設值為報酬率。</li><li>window:(str,default '1d') 移動窗口長度，可輸入整數，即代表以季為移動窗口的單位，(d代表「日」,w代表「周」,m代表「月」)。</li><li>col_type:(str,default 'SMA') 要針對此欄位的移動窗口區間內的資料計算波動度的方法，目前僅支援SMA，未來會支援EWMA、GARCH族。</li></ul>  
  
  
**sort_crossing**  
功能概述：將股票按照指標進行排名  
範例：tejtool.sort_crossing(check_index=<檢查欄位>,window = <數字>,method=<排序後回傳方法>,ascending=<是否>)   
說明：參數說明：<ul><li>check_index:(str, not None) 要進行計算的欄位名稱</li><li>window:(int or str,default '1d') 移動窗口長度，可輸入整數，即代表以季為移動窗口的單位，(d代表「日」,w代表「周」,m代表「月」)。</li><li>method:(str,default 'percentile') 針對此欄位進行排序後，回傳的結果。 'rank':回傳值為該股票的此一欄位，在整體股票中，所在的排名 'percentile':回傳結果為百分位數</li><li>ascending:(boolean,default False) 排序方式為順向或逆向，若ascending=True，則該欄位數值越大，計算結果的值(排序或百分位數)越小。</li></ul>  
  
  
**calculate_grwothrate**  
功能概述：針對指定之欄位，依照指定之窗口計算該欄位數值的成長率，亦可藉此計算出『未來』的累積報酬率供模型建構之用。  
範例：tejtool.calculate_grwothrate(check_index=<檢查欄位>,window = <數字>,col_type=<計算報酬率的方法>,method=<幾合或算數報酬率>,sync=<是否包含最前日之報酬率>,peer_future=<是否>)   
說明：參數說明：<ul><li>check_index:(str, not None) 要進行計算的欄位名稱</li><li>window:(int or str,default '1d') 移動窗口長度，可輸入整數，即代表以季為移動窗口的單位，(d代表「日」,w代表「周」,m代表「月」)。</li><li>col_type:(str,default None) 計算報酬率的特殊方法，預設不輸入只計算期初跟期末之間的變化，輸入'rolling'則滾動計算每一天的報酬率後進行累積。</li><li>method:(str,default 'arithmetic') 計算報酬率的數學方法，'arithmetic'為算數報酬率，'geometric'是幾何報酬率。</li><li>sync:(boolean,default True) 是否包含窗口最前面日期當天的報酬率。</li><li>peer_future:(boolean,default False) 是否為往未來移動窗口來計算數值。</li></ul>  
  
  
**make_famamacbethmodel**  
功能概述：進行fama-macbeth估計，並儲存計算結果，依照回歸結果對各股票給分排序，只會在回測起始時進行fama-macbeth估計，若要固定期間重新估計，需要進行額外設定。  
範例：tejtool.make_famamacbethmodel(col_name=<欄位>,check_index=<[list]>,window = <數字>,alpha_rate = <數字>,reset_list = <文字>)  
說明：<ul><li>check_index，格式為list，list中儲存要進行fama估計的各解釋變數欄位名稱。</li><li>col_name，格式為str，為進行fama估計的被解釋變數欄位名稱。</li><li>alpha_rate:(int ,default 5) fama-macbeth估計的顯著水準。</li><li>reset_list:(str ,default '01') 用來設定每個月進行模型重新估計的日期，亦可透過list給定['01-01','06-01']，則會在每年1/1與6/1重估模型。</li></ul>  
  
  
**run_famascore**  
功能概述：透過訓練好的fama-macbeth模型，計算各檔股票的得分。  
範例：tejtool.run_famascore(rank_above=<數字>,class_interval = <數字>)   
說明：<ul><li>rank_above，格式為int，代表總分若低於此分位數的門檻就歸零。</li><li>class_interval，格式為int，用來進行分數相近者的分組，如class_interval=10，則每10間公司一組。</li></ul>  


**group_selection**  
功能概述：在回測時間點，按照目前的分組，以給定的移動窗口計算各組股票指定欄位的數值，選擇該數值排序最高的組別做為選股目標。  
範例：tejtool.group_selection(check_index=<欄位>,ascending=<是否>,group_name=<欄位>,window=<數值>,keep=<文字>,choose_above=<[list]>):  
說明：<ul><li>check_index，格式為str，預設值為'報酬率'；為用來優化的目標。</li><li>group_name，格式為str，為用來分組的標籤名稱。</li><li>window:(int or str,default '1d') 移動窗口長度，可輸入整數，即代表以季為移動窗口的單位，(d代表「日」,w代表「周」,m代表「月」)。</li><li>keep，格式為str，若給定'first'，則每個月資料只留最初那筆，'last'則相反。</li><li>ascending，格式為boolean，若給定True，則改以數值小的進行排序</li><li>choose_above，格式為list，若給定[10,20]，則選取表現第10%~20%的組別</li></ul>  
  
  
**equal_pv**  
功能概述：等權重持股權重計算函式：只以「同類持股權重計算函式」最後一次執行結果進行回測，重複執行無用。  
範例：tejtool.equal_pv(check_column=<欄位>,hold_unit=<單位>)   
說明：<ul><li>check_column，格式為str，預設值為'購入'；為用來確認各檔股票是否要買入得確認用欄位名稱。</li><li>hold_unit，格式為str，預設值為'unit'；為要購入的股票，經計算後持股的數量。</li></ul>  
  
  
  
**check_condition**  
功能概述：選股條件篩選函式  
範例：tejtool.check_condition(conditions=<[list]>,check_type=<邏輯>)   
說明：<ul><li>conditions，格式為list，list中儲存要檢查的各欄位名稱。</li><li>check_type，格式為str，'or'或'and'，檢查conditions時，各結果為True或False後，取聯集或交集。</li></ul>  