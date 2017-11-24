# 功能

该函数订阅日志服务logstore的实时数据，根据日志字段中的ip值，查找ip数据库获得ip归属的国家、省、市、ISP信息。最终在原始数据基础上添加ip归属信息后写入另一个logsotre。

加工前后的数据区别在于处理后的数据新增了几个key-value，效果如下：

* 订阅的logstore数据源

```
__topic__:
ip:125.118.224.222
msg:"POST /PutData?Category=YunOsAccountOpLog&AccessKeyId=******&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=pD12XYLmGxKQ%2Bmkd6x7hAgQ7b1c%3D HTTP/1.1" 0.024 18204 200 37 "-" "aliyun-sdk-java" 1320203493
time:09/Oct/2017:06:12:03 +0800
```

* 加工后的数据输出到目标logstore

```
__topic__:
city:杭州市
ip:125.118.224.222
isp:电信
msg:"POST /PutData?Category=YunOsAccountOpLog&AccessKeyId=******&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=pD12XYLmGxKQ%2Bmkd6x7hAgQ7b1c%3D HTTP/1.1" 0.024 18204 200 37 "-" "aliyun-sdk-java" 1320203493
province:浙江省
time:09/Oct/2017:06:12:03 +0800
```

如果把函数比作一个管道，数据输入、输出如下：

* 数据输入

数据源logstore的<shard_id, begin_cursor, shard_cursor>三元组，指示function需要读取哪些数据。

* 数据输出

源数据在根据函数配置添加相关ip归属字段后写往目标日志服务project、logstore。

# 函数输入

* [函数event](https://help.aliyun.com/document_detail/51885.html)

该[event](https://help.aliyun.com/document_detail/60781.html?spm=5176.product28958.6.634.kEFuYC#h1--event)由日志服务生成并在数据到来时自动触发函数执行。

* 配置

前往[函数计算控制台](https://fc.console.aliyun.com/#/serviceList/)，配置方法参考[使用指南](https://help.aliyun.com/document_detail/60291.html?spm=5176.doc60781.6.633.YgBNLD#h1-u4F7Fu7528u6307u5357)。

> 请参考下面JSON填写自己的函数配置：

```
{
  "source": {
    "endpoint": "http://cn-shanghai-intranet.log.aliyuncs.com",
  },
  "target": {
    "endpoint": "http://cn-shanghai-intranet.log.aliyuncs.com",
    "logstoreName": "ip-location-mapping",
    "projectName": "etl-test"
  },
  "transform": {
    "cityKeyName": "city",
    "countryKeyName": "",
    "ipKeyName": "ip",
    "ispKeyName": "isp",
    "ossBucketOfIpData": "log-etl-resources",
    "ossEndpointOfIpData": "http://oss-cn-hangzhou-internal.aliyuncs.com",
    "ossObjectOfIpData": "ipdata/ipdata_geo_isp_code.txt.utf8.gz",
    "provinceKeyName": "province"
    "regionKeyName": ""
  }
}
```

| 一级key | 二级key | 是否必填 | 描述 |
|--------|---------|--------|------|
| source | endpoint | N | 如不填写，使用默认的本Region内网域名 |
| target | endpoint | N | 如不填写，使用默认的本Region内网域名 |
| target | projectName | Y | 日志输出的project名字 |
| target |  logstoreName | Y | 日志输出的logstore名字 |
| transform | ossEndpointOfIpData | Y | ipdata资源文件所在OSS endpoint | 
| transform | ossBucketOfIpData | Y | ipdata资源文件所在OSS bucket | 
| transform | ossObjectOfIpData | Y | ipdata资源文件所在OSS object | 
| transform | ipKeyName | Y | 输入日志的哪个key包含需要处理的ip |
| transform | countryKeyName | N | 加工生成的ip归属国家字段key名，如留空或该配置不存在则不输出 | 
| transform | provinceKeyName | N | 加工生成的ip归属省份字段key名，如留空或该配置不存在则不输出  | 
| transform | cityKeyName | N | 加工生成的ip归属城市字段key名，如留空或该配置不存在则不输出  | 
| transform | ispKeyName | N | 加工生成的ip归属ISP字段key名，如留空或该配置不存在则不输出  | 

> 请下载字典文件 http://log-etl-resources.oss-cn-hangzhou.aliyuncs.com/ipdata/ipdata_geo_isp_code.txt.utf8.gz 并上传到您账号下的OSS bucket，填写相应的ossEndpointOfIpData/ossBucketOfIpData/ossObjectOfIpData配置。建议将OSS Bucket与函数服务Service放在相同Region，使用内网下载以避免函数执行过程中公网传输产生的费用（函数实现已做优化，如果函数在5分钟内会执行一次，将会复用之前的文件内容并不需要重新下载字典文件）。

# 函数输出 

执行成功的函数会将源logstore数据做加工后再写到目标logstore，并返回一个JSON Object序列化的字符串，包含：ingestLines（读取日志行数）、ingestBytes（读取日志字节数）、shipLines（输出日志行数）、shipBytes（输出日志字节数）。

如执行过程中发生异常（例如读写logstore失败），会抛出IOException终止函数运行。

# 注意事项

该函数会初始化一个数组用于存储ipdata资源字典，建议为函数设置768MB+内存规格。

如果logstore单shard日志流量较大，建议设置120s以上超时时间。

# 权限策略

| 建议的默认权限 | 说明 |
|--------------|-----|
| AliyunLogFullAccess | 读、写日志服务 |
| AliyunOSSReadOnlyAccess | 读OSS |

# 函数配置

| 配置项 | 值 |
|-----|-------|
| runtime | java8 |
| handler | com.aliyun.log.etl_function.IpLookup::handleRequest |
| memory | 768MB |
| timeout | 120s |
| code | https://github.com/aliyun/aliyun-log-fc-functions/blob/master/ip_lookup/jar/latest/log-etl-ip-lookup.jar |
