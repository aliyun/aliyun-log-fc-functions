# oss-shipper-csv（构建csv投递OSS） 

## 描述

* function_name

oss-shipper-csv

* description

该函数订阅日志服务logstore的实时数据，对于配置的日志字段，按序取出其值构建出一个csv日志行，最终由一批数据构成目标csv文件。该csv文件可以选择直接存入OSS，或者使用snappy/gzip做文件做整体压缩后写入OSS。使用前请阅读<a href="https://github.com/aliyun/aliyun-log-fc-functions/blob/master/oss_shipper_csv/README.md" target="#">oss-shipper-csv函数说明</a>。

* input

函数event是在用户配置基础上增加logstore的shard curosr等信息后得到。输入用于指示function从shard的什么位置开始、停止读取数据，并为函数自定义逻辑提供额外配置（例如数据做什么加工、写出到哪里）。当shard有数据写入时，日志服务会定时触发函数执行。

* output

执行成功的函数会将源logstore数据处理为csv后写到OSS。

* mark

构建csv文件需要在内存中暂存本次函数调用所需处理的全部日志数据，请根据日志量为函数设置足够的内存规格。

*  policies

AliyunLogFullAccess

AliyunOSSFullAccess

## 配置

| 配置项 | 值 |
|-----|-------|
| runtime | java8 |
| handler | com.aliyun.log.etl_function.OssShipperCsv::handleRequest |
| memory | 768MB |
| timeout | 120s |
| code | https://github.com/aliyun/aliyun-log-fc-functions/blob/master/oss_shipper_csv/jar/latest/log-etl-oss-shipper-csv.jar |
