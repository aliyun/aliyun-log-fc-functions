# logstore-replication（logstore数据复制）

## 描述

* functionName

logstore-replication 

* description

该函数订阅日志服务logstore的实时数据，并复制数据到另一个logstore。使用前请阅读<a href="https://github.com/aliyun/aliyun-log-fc-functions/blob/master/logstore_replication/README.md" target="#">logstore-replication函数说明</a>。

* input

函数event是在用户配置基础上增加logstore的shard curosr等信息后得到。输入用于指示function从shard的什么位置开始、停止读取数据，并为函数自定义逻辑提供额外配置（例如数据做什么加工、写出到哪里）。当shard有数据写入时，日志服务会定时触发函数执行。

* output

执行成功的函数会将源logstore数据复制到目标logstore。

* mark

注意为目标logstore配置足够的shard，防止写入超过配额导致函数失败。

* policies

AliyunLogFullAccess

## 配置

| 配置项 | 值 |
|-----|-------|
| runtime | java8 |
| handler | com.aliyun.log.etl_function.LogstoreReplication::handleRequest |
| memory | 384MB |
| timeout | 120s |
| code | https://github.com/aliyun/aliyun-log-fc-functions/blob/master/logstore_replication/jar/latest/log-etl-logstore-replication.jar |