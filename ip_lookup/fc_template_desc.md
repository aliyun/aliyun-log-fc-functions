# ip-lookup（ip归属查找）

## 模板描述

* functionName

ip-lookup

* description

该函数订阅日志服务logstore的实时数据，根据日志字段中的ip值，查找ip数据库获得ip归属的国家、省、市、ISP信息。最终在原始数据基础上添加ip归属信息后写入另一个logsotre。使用前请阅读<a href="https://github.com/aliyun/aliyun-log-fc-functions/blob/master/ip_lookup/README.md" target="#">ip-lookup函数说明</a>。 

* input

函数event是在用户配置基础上增加logstore的shard curosr等信息后得到。输入用于指示function从shard的什么位置开始、停止读取数据，并为函数自定义逻辑提供额外配置（例如数据做什么加工、写出到哪里）。当shard有数据写入时，日志服务会定时触发函数执行。

* output

执行成功的函数会将源logstore数据做加工后再写到目标logstore。

* mark

该函数会初始化一个数组用于存储ipdata资源字典，建议为函数设置768MB+内存规格。同时请注意为目标logstore设置足够的shard防止因写入配额不足导致函数执行失败。

* policies

AliyunLogFullAccess

AliyunOSSReadOnlyAccess

## 函数配置

| 配置项 | 值 |
|-----|-------|
| runtime | java8 |
| handler | com.aliyun.log.etl_function.IpLookup::handleRequest |
| memory | 768MB |
| timeout | 120s |
| code | https://github.com/aliyun/aliyun-log-fc-functions/blob/master/ip_lookup/jar/latest/log-etl-ip-lookup.jar |