# 描述

该函数订阅日志服务logstore的实时数据，根据日志字段中的ip值，查找ip数据库获得ip归属的国家、大区、省、市、ISP信息。最终在原始数据基础上添加ip归属信息后写入另一个logsotre。

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

# 输入参数

通用配置：请参考[日志服务-自定义ETL-用户指南]（https://help.aliyun.com/document_detail/60291.html）。

函数配置（JSON Object类型，与具体的函数实现有关）：

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
| transform | ossEndpointOfIpData | Y | ipdata资源文件所在OSS endpoint | 
| transform | ossBucketOfIpData | Y | ipdata资源文件所在OSS bucket | 
| transform | ossObjectOfIpData | Y | ipdata资源文件所在OSS object | 
| transform | ipKeyName | Y | 输入日志的哪个key包含需要处理的ip |
| transform | countryKeyName | N | 加工生成的ip归属国家字段key名，如留空或该配置不存在则不输出 | 
| transform | regionKeyName | N | 加工生成的ip归属大区字段key名，如留空或该配置不存在则不输出  | 
| transform | provinceKeyName | N | 加工生成的ip归属省份字段key名，如留空或该配置不存在则不输出  | 
| transform | cityKeyName | N | 加工生成的ip归属城市字段key名，如留空或该配置不存在则不输出  | 
| transform | ispKeyName | N | 加工生成的ip归属ISP字段key名，如留空或该配置不存在则不输出  | 

# 输出参数

输出为一个JSON Object，包含：ingestLines（读取日志行数）、ingestBytes（读取日志字节数）、shipLines（输出日志行数）、shipBytes（输出日志字节数）。

如执行过程中发生异常（例如读写logstore失败），会抛出IOException终止函数运行。

# 注意事项

该函数会初始化一个数组用于存储ipdata资源字典，会占用较大内存，建议为函数设置1.5GB内存规格。

# 权限策略

AliyunLogFullAccess

AliyunOSSReadOnlyAccess