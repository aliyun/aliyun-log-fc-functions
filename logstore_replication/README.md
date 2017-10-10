# 描述

该函数订阅日志服务logstore的实时数据，并复制数据到另一个logstore。

# 输入参数

通用配置：请参考[日志服务-自定义ETL-用户指南]（https://help.aliyun.com/document_detail/60291.html）。

函数配置（JSON Object类型，与具体的函数实现有关）如下：

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

# 输出参数

输出为一个JSON Object，包含：ingestLines（读取日志行数）、ingestBytes（读取日志字节数）、shipLines（输出日志行数）、shipBytes（输出日志字节数）。

如执行过程中发生异常（例如读写logstore失败），会抛出IOException终止函数运行。

# 注意事项

该函数读取数据并批量写到logstore，建议为函数设置512MB内存规格。

# 权限策略

AliyunLogFullAccess。
