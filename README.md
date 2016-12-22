# 基于MapReduce的中文词语预测

### 原理
通过搜狗新闻语料库，计算概率P(w2|w1) 或 P(w3|w1w2)，取最大的做为预测结果

### 使用方法
- hadoop jar xxx.jar cmlin input output 中国

或

- hadoop jar xxx.jar cmlin input output 中国 的

### 输入数据格式
```
<doc>
<url>页面URL</url>
<docno>页面ID</docno>
<contenttitle>页面标题</contenttitle>
<content>页面内容</content>
</doc>
```
