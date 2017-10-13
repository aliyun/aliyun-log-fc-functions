# logstore replication（logstore数据复制）

* 模板描述

```
{
    "functionName": "logstore-replication", 
    "description": "该函数订阅日志服务logstore的实时数据，并复制数据到另一个logstore。了解更多使用细节请访问： https://github.com/aliyun/aliyun-log-fc-functions/blob/master/logstore_replication/README.md 。", 
    "input": [
        "functioin event根据用户配置增加logstore的shard curosr等信息后得到。输入用于指示function从shard的什么位置开始、停止读取数据，并为函数自定义逻辑提供额外配置（例如数据做什么加工、写出到哪里）。当shard有数据写入时，日志服务会定时触发函数执行。"
    ], 
    "output": [
        "执行成功的函数会将源logstore数据复制到目标logstore，并返回一个JSON Object序列化的字符串，包含：ingestLines（读取日志行数）、ingestBytes（读取日志字节数）、shipLines（输出日志行数）、shipBytes（输出日志字节数）。如执行过程中发生异常（例如读写logstore失败），会抛出IOException终止函数运行。"
    ], 
    "mark": "该函数读取数据并批量写到logstore，建议为函数设置384MB+内存规格。如果logstore单shard日志流量较大，建议设置120s以上超时时间。", 
    "policies": [
        "AliyunLogFullAccess"
    ]
}
```

* 代码地址

```
https://github.com/aliyun/aliyun-log-fc-functions/blob/master/logstore_replication/jar/0.1.0/log-etl-logstore-replication.jar
```


# ip lookup（ip归属查找）

* 模板描述

```
{
    "functionName": "ip-lookup", 
    "description": "该函数订阅日志服务logstore的实时数据，根据日志字段中的ip值，查找ip数据库获得ip归属的国家、省、市、ISP信息。最终在原始数据基础上添加ip归属信息后写入另一个logsotre。了解更多使用细节请访问： https://github.com/aliyun/aliyun-log-fc-functions/blob/master/ip_lookup/README.md 。", 
    "input": [
        "functioin event根据用户配置增加logstore的shard curosr等信息后得到。输入用于指示function从shard的什么位置开始、停止读取数据，并为函数自定义逻辑提供额外配置（例如数据做什么加工、写出到哪里）。当shard有数据写入时，日志服务会定时触发函数执行。"
    ], 
    "output": [
        "执行成功的函数会将源logstore数据做加工后再写到目标logstore，并返回一个JSON Object序列化的字符串，包含：ingestLines（读取日志行数）、ingestBytes（读取日志字节数）、shipLines（输出日志行数）、shipBytes（输出日志字节数）。如执行过程中发生异常（例如读写logstore失败），会抛出IOException终止函数运行。"
    ], 
    "mark": "该函数会初始化一个数组用于存储ipdata资源字典，建议为函数设置768MB+内存规格。如果logstore单shard日志流量较大，建议设置120s以上超时时间。", 
    "policies": [
        "AliyunLogFullAccess", 
        "AliyunOSSReadOnlyAccess"
    ]
}
```

* 代码地址

```
https://github.com/aliyun/aliyun-log-fc-functions/blob/master/ip_lookup/jar/0.1.0/log-etl-ip-lookup.jar
```