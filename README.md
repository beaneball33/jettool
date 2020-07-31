tejapi (https://api.tej.com.tw/)

# jettool


jettool 是由 [TEJ](https://www.tej.com.tw/)開發，提供基於TEJ 資料庫之技術指標計算、財金專用數學計算、整合投資回測之工具，並具有整合簡易查詢tej資料庫之功能。



![simple tear 0](https://github.com/quantopian/pyfolio/raw/master/docs/simple_tear_0.png "Example tear sheet created from a Zipline algo")
![simple tear 1](https://github.com/quantopian/pyfolio/raw/master/docs/simple_tear_1.png "Example tear sheet created from a Zipline algo")

Also see [slides of a talk about
pyfolio](https://nbviewer.jupyter.org/format/slides/github/quantopian/pyfolio/blob/master/pyfolio/examples/pyfolio_talk_slides.ipynb#/).

## 使用套件

使用套件前，建議先匯入tejapi模組，並設定tej api key。
tej api key須申請，否則此套件無法進行查詢功能。
```
import tejapi
tejapi.ApiConfig.api_key ="你的tej api key"
```
#### 初始化整合工具

輸入以下的指令，可以產生整合查詢、計算與回測工具物件：
```
tejtool = jettool.jet.engine()
```

#### 匯入財報查詢工具

輸入以下指令，可以匯入財報查詢工具：

```
 import jettool.dataset.finreport as fp
```

## 使用

請參考[各工具使用說明](https://github.com/beaneball33/jettool/tree/master/docs) 


## Questions?

如果你遇到問題，請回報 [open an issue](https://github.com/beaneball33/jettool/issues)



## Support

請到 [open an issue](https://github.com/beaneball33/jettool/issues) 尋求支援。.
