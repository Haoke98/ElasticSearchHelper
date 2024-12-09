# ElasticSearchHelper

## 使用&教程

### Task

按百分比展示任务的进度,并估算出预计需要时间和预计完成时间.

```shell
python main.py task --id "zJ9FtKuNSqi4mycedlvMRg:467516690"
状态：正在运行.....
已运行：0:58:30.512872
进度：23.071283420651266 %
速率：
      2.438675005900036e-06/纳秒(nanosecond)
      2.438675005900036/毫秒(millisecond)
      73.16025017700107/秒(second)
      4389.615010620065/分(minute)
预估需要：3:15:05.428124
预估完成于：2024-12-09 22:38:21.975248
```

### Map

从CSV倒推索引map

```shell
python main.py generate-map -i index-meaning-guessed-field-table-generated-hzxy_nation_global_enterprise-fixed.csv --obj2nested
  1 abnormalOperationData                                                            :  abnormalOperationData object:1
  2 abnormalOperationData.totalNum                                                   :                   totalNum long:2
  3 administrativeLicensingData                                                      :  administrativeLicensingData object:1
  4 administrativeLicensingData.dataList                                             :                   dataList object:2
  5 administrativeLicensingData.dataList.fileName                                    :                                           fileName text:3
  6 administrativeLicensingData.dataList.fileName.keyword                            :                                                                   keyword keyword:4
  7 administrativeLicensingData.dataList.fileNo                                      :                                           fileNo text:3
  8 administrativeLicensingData.dataList.fileNo.keyword                              :                                                                   keyword keyword:4
  9 administrativeLicensingData.dataList.firmName                                    :                                           firmName text:3
 10 administrativeLicensingData.dataList.firmName.keyword                            :                                                                   keyword keyword:4
 11 administrativeLicensingData.dataList.licensingContent                            :                                           licensingContent text:3
 12 administrativeLicensingData.dataList.licensingContent.keyword                    :                                                                   keyword keyword:4
 13 administrativeLicensingData.dataList.licensingUnit                               :                                           licensingUnit text:3
 14 administrativeLicensingData.dataList.licensingUnit.keyword                       :                                                                   keyword keyword:4
 15 administrativeLicensingData.dataList.valFrom                                     :                                           valFrom date:3
 16 administrativeLicensingData.dataList.valTo                                       :                                           valTo date:3
 17 administrativeLicensingData.totalNum                                             :                   totalNum long:2
·······························································································································································································
670 tagGazelle.checkYear                                                             :                   checkYear text:2
671 tagGazelle.checkYear.keyword                                                     :                                           keyword keyword:3
672 tagHightech                                                                      :  tagHightech object:1
673 tagHightech.checkYear                                                            :                   checkYear text:2
674 tagHightech.checkYear.keyword                                                    :                                           keyword keyword:3
·······························································································································································································
703 taxCreditRatingData                                                              :  taxCreditRatingData object:1
704 taxCreditRatingData.dataList                                                     :                   dataList object:2
705 taxCreditRatingData.dataList.firmName                                            :                                           firmName text:3
706 taxCreditRatingData.dataList.firmName.keyword                                    :                                                                   keyword keyword:4
707 taxCreditRatingData.dataList.ratingInfo                                          :                                           ratingInfo text:3
708 taxCreditRatingData.dataList.ratingInfo.keyword                                  :                                                                   keyword keyword:4
709 taxCreditRatingData.dataList.yearInfo                                            :                                           yearInfo text:3
710 taxCreditRatingData.dataList.yearInfo.keyword                                    :                                                                   keyword keyword:4
711 taxCreditRatingData.totalNum                                                     :                   totalNum long:2
712 updateDateInfo                                                                   :  updateDateInfo text:1
713 updateDateInfo.keyword                                                           :                   keyword keyword:2
714 wechatListData                                                                   :  wechatListData object:1
715 wechatListData.wechatAccount                                                     :                   wechatAccount text:2
716 wechatListData.wechatAccount.keyword                                             :                                           keyword keyword:3
717 wechatListData.wechatDescription                                                 :                   wechatDescription text:2
718 wechatListData.wechatDescription.keyword                                         :                                           keyword keyword:3
719 wechatListData.wechatImageUrl                                                    :                   wechatImageUrl text:2
720 wechatListData.wechatImageUrl.keyword                                            :                                           keyword keyword:3
721 wechatListData.wechatName                                                        :                   wechatName text:2
722 wechatListData.wechatName.keyword                                                :                                           keyword keyword:3
723 yearTagArr                                                                       :  yearTagArr text:1
724 yearTagArr.keyword                                                               :                   keyword keyword:2
725 yearTagStr                                                                       :  yearTagStr text:1
```

## 脚本&功能

* `map/generate_field_table.py`: 从map.json解析出字段表格
* `map/gues-field-meaning.py`: 基于LLM根据字段名推理出其实际意义和用处
* `health-monitoring/es-health-monitoring`: 实时监控ES集群健康状态
* `health-monitoring/check-analyzer-health.py`: 挨个节点检查集群中的IK分词器能否正常执行分词任务 