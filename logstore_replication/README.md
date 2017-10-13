# 功能 

该函数订阅日志服务logstore的实时数据，并复制数据到另一个logstore。

如果把函数比作一个管道，数据输入、输出如下：

* 输入

数据源logstore的<shard_id, begin_cursor, shard_cursor>三元组，指示function需要读取哪些数据。

* 输出

复制的源数据写入函数配置中设置的目标日志服务project、logstore。

# 函数输入（[函数event](https://help.aliyun.com/document_detail/51885.html)）

functioin event根据用户配置增加logstore的shard curosr等信息后得到。输入用于指示function从shard的什么位置开始、停止读取数据，并为函数自定义逻辑提供额外配置（例如数据做什么加工、写出到哪里）。当shard有数据写入时，日志服务会定时触发函数执行。

在[函数服务控制台](https://fc.console.aliyun.com/#/serviceList/)需要填写以下配置项：

* 通用配置

请参考[日志服务-自定义ETL-用户指南](https://help.aliyun.com/document_detail/60291.html)。

* 函数配置（JSON Object类型，与具体的函数实现有关）

```
{
  "source": {
    "endpoint": "http://cn-shanghai-intranet.log.aliyuncs.com",
  },
  "target": {
    "endpoint": "http://cn-shanghai-intranet.log.aliyuncs.com",
    "logstoreName": "logstore-replication",
    "projectName": "etl-test"
  }
}
```

| 一级key | 二级key | 是否必填 | 描述 |
|--------|---------|--------|------|
| source | endpoint | N | 如不填写，使用默认的本Region内网域名 |
| target | endpoint | N | 如不填写，使用默认的本Region内网域名 |
| target | projectName | Y | 日志输出的project名字 |
| target |  logstoreName | Y | 日志输出的logstore名字 |

# 函数输出

执行成功的函数会将源logstore数据复制到目标logstore，并返回一个JSON Object序列化的字符串，包含：ingestLines（读取日志行数）、ingestBytes（读取日志字节数）、shipLines（输出日志行数）、shipBytes（输出日志字节数）。

如执行过程中发生异常（例如读写logstore失败），会抛出IOException终止函数运行。

# 注意事项

该函数读取数据并批量写到logstore，建议为函数设置384MB+内存规格。

如果logstore单shard日志流量较大，建议设置120s以上超时时间。

# 权限策略

AliyunLogFullAccess。
