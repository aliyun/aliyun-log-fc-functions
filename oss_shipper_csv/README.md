# 功能

该函数订阅日志服务logstore的实时数据，对于配置的日志字段，按序取出其值构建出一个csv日志行，最终由一批数据构成csv文件。该csv文件可以选择直接存入OSS，或者使用snappy/gzip做文件做整体压缩后写入OSS。

如果把函数比作一个管道，数据输入、输出如下：

* 输入

数据源logstore的<shard_id, begin_cursor, shard_cursor>三元组，指示function需要读取哪些数据。

* 输出

写出csv文件（可选择压缩）到OSS。

# 函数输入

* [函数event](https://help.aliyun.com/document_detail/51885.html)

该[event](https://help.aliyun.com/document_detail/60781.html?spm=5176.product28958.6.634.kEFuYC#h1--event)由日志服务生成并在数据到来时自动触发函数执行。

* 配置

前往[函数计算控制台](https://fc.console.aliyun.com/#/serviceList/)，配置方法参考[使用指南](https://help.aliyun.com/document_detail/60291.html?spm=5176.doc60781.6.633.YgBNLD#h1-u4F7Fu7528u6307u5357)。

> 请参考下面JSON填写自己的函数参数：

```
{
    "target": {
        "ossBucket": "test-bucket",
        "ossEndpoint": "http://oss-cn-shanghai-internal.aliyuncs.com",
        "ossPrefix": "csv",
        "ossPostfix": ".csv",
        "ossDateFormat": "yyyyMMdd/HH/"
    },
    "transform": {
        "compressType": "gzip",
        "delimiterChar": ",",
        "enableHeader": false,
        "escapeChar": "\"",
        "quoteChar": "\"",
        "nullColumnValue": "",
        "columnNames": [
            "file",
            "level",
            "msg",
            "__tag__:__hostname__",
            "__tag__:__path__",
            "__tag__:__user_defined_id__",
            "__source__",
            "__topic__",
            "microtime"
        ]
    }
}
```

| 一级key | 二级key | 是否必填 | 描述 |
|--------|---------|--------|------|
| target | ossEndpoint | Y | OSS服务[访问入口](https://help.aliyun.com/document_detail/31837.html?spm=5176.product31815.6.577.ZMT6qZ) |
| target | ossBucket | Y | OSS bucket |
| target | ossPrefix | Y | 文件存储到bucket下面的哪个目录前缀下 |
| target | ossPostfix | Y | 文件的后缀 |
| target | ossDateFormat | Y | 将函数event中的cursorTime（本次调用要处理的数据的最大服务端数据接收时间）使用[Java SimpleDateFormat](https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html?spm=5176.doc29001.2.7.ktAp3Z)定义一个日志格式字符串，以此来定义写到OSS的Object文件所在的目录层次结构，其中斜线/表示一级OSS目录。 |
| transform | compressType | Y | 文件级别整体压缩，支持：""、"gzip"、"snappy" | 
| transform | delimiterChar | Y | 长度为1的字符串，用于分割不同字段 | 
| transform | quoteChar | Y | 长度为1的字符串，字段内出现分隔符（delimiter）或换行符等情况时，需要用quote前后包裹这个字段，避免读数据时造成字段错误切分 |
| transform | escapeChar | Y | 长度为1的字符串，默认设置与quote相同，暂不支持修改。字段内部出现quote（当成正常字符而不是转义符）时需要在quote前面加上escape做转义 | 
| transform | nullColumnValue | Y | 当指定Key值不存在时，字段填写该字符串表示该字段无值 | 
| transform | columnNames | Y | 可以在日志服务数据预览或索引查询页面查看一条日志的多个Key-Value，将你需要投递到OSS的字段名（Key）有序填入。如您配置的Key名称在日志中找不到，csv行中这里一列值将设置为nullColumnValue | 
| transform | enableHeader | Y | 是否在csv文件的首行加上字段名的描述 | 

举例说明OSS目录格式如下：

| ossPrefix | ossDateFormat | ossPostfix | 目录 |
|-----------|---------------|------------|-----|
| test1/ | yyyy_MM_dd_HH_mm_00	| .csv | oss://bucket/test1/2016_01_27_20_30_00/278_MTUxMTI1NDk5Mjg0NTU1MTQ0MQ==_MTUxMTI1NDk5Mjg0NTU1MTQ0Mg==.csv |
| test1/ | yyyyMMdd | .csv.gzip | oss://bucket/test1/20160127/297_MTUxMTI1NTAxNDUyNTM4NTM3NQ==_MTUxMTI1NTAxNDUyNTM4NTM3Ng==.csv.gzip |
| test2 | yyyyMMddHHmm | .csv.snappy | oss://bucket/test2201601272000/303_MTUxMTI1NDQwNzUxMzMzMjMyOQ==_MTUxMTI1NDQwNzUxMzMzMjMzMQ==.csv.snappy |

# 函数输出 

执行成功的函数会将源logstore数据做加工后再写到目标logstore，并返回一个JSON Object序列化的字符串，包含：ingestLines（读取日志行数）、shipLines（输出日志行数）。

如执行过程中发生异常（例如写OSS失败），会抛出IOException终止函数运行。

# 注意事项

构建csv文件需要在内存中暂存本次函数调用所需处理的全部日志数据。

建议为函数设置768MB左右内存规格，如果每次函数执行所需要处理的数据较少可以配置512MB内存。

如果logstore单shard日志流量较大，建议设置120s以上超时时间。

# 权限策略

| 建议的默认权限 | 说明 |
|--------------|-----|
| AliyunLogFullAccess | 读、写日志服务 |
| AliyunOSSFullAccess | 写OSS |

# 函数配置

| 配置项 | 值 |
|-----|-------|
| runtime | java8 |
| handler | com.aliyun.log.etl_function.OssShipperCsv::handleRequest |
| memory | 768MB |
| timeout | 120s |
| code | https://github.com/aliyun/aliyun-log-fc-functions/raw/master/oss_shipper_csv/jar/latest/log-etl-oss-shipper-csv.jar |
| code md5sum | e6dea98e0c47918ddcbc8bfde08e7d24 |
