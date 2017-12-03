# 说明 

由[日志服务](https://www.aliyun.com/product/sls)维护的[函数计算](https://www.aliyun.com/product/fc)function code仓库。

了解更多，请阅读[日志服务-自定义ETL](https://help.aliyun.com/document_detail/60291.html)。

# 函数模板

| 函数名 | 简介 | README |
|-------|-----|-------|
| ip_lookup | 订阅日志服务Logstore的实时数据，根据日志字段中的IP值，查找IP数据库获得IP归属的国家、省、市、ISP信息。最终在原始数据基础上添加IP归属信息后写入另一个Logsotre。 | [README](ip_lookup/README.md)|
| logstore_replication | 订阅日志服务Logstore的实时数据，并复制数据到另一个Logstore。 | [README](logstore_replication/README.md) |
| oss_shipper_csv | 订阅日志服务logstore的实时数据，对于配置的日志字段，按序取出其值构建出一个csv日志行，最终由一批数据构成csv文件。该csv文件可以选择直接存入OSS，或者使用snappy/gzip做文件做整体压缩后写入OSS。 | [README](oss_shipper_csv/README.md) |
