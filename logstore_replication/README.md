# 功能 

该函数订阅日志服务logstore的实时数据，并复制数据到另一个logstore。

如果把函数比作一个管道，数据输入、输出如下：

* 输入

数据源logstore的<shard_id, begin_cursor, shard_cursor>三元组，指示function需要读取哪些数据。

* 输出

复制的源数据写入函数配置中设置的目标日志服务project、logstore。

# 函数输入

* [函数event](https://help.aliyun.com/document_detail/51885.html)

该[event](https://help.aliyun.com/document_detail/60781.html?spm=5176.product28958.6.634.kEFuYC#h1--event)由日志服务生成并在数据到来时自动触发函数执行。

* 配置

前往[函数计算控制台](https://fc.console.aliyun.com/#/serviceList/)，配置方法参考[使用指南](https://help.aliyun.com/document_detail/60291.html?spm=5176.doc60781.6.633.YgBNLD#h1-u4F7Fu7528u6307u5357)。

> 请参考下面JSON填写自己的函数配置：

```
{
  "source": {
    "endpoint": "http://cn-shanghai-intranet.log.aliyuncs.com"
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

| 建议的默认权限 | 说明 |
|--------------|-----|
| AliyunLogFullAccess | 读、写日志服务 |

# 函数配置

| 配置项 | 值 |
|-----|-------|
| runtime | java8 |
| handler | com.aliyun.log.etl_function.LogstoreReplication::handleRequest |
| memory | 384MB |
| timeout | 120s |
| code | https://github.com/aliyun/aliyun-log-fc-functions/blob/master/logstore_replication/jar/latest/log-etl-logstore-replication.jar |

