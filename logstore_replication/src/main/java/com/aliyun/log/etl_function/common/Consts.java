package com.aliyun.log.etl_function.common;

public class Consts {

    public final static String EVENT_SOURCE_FIELD_NAME = "source";
    public final static String EVENT_PARAMETER_FIELD_NAME = "parameter";
    public final static String EVENT_TARGET_FIELD_NAME = "target";
    public final static String EVENT_JOB_NAME_FIELD_NAME = "jobName";
    public final static String EVENT_TASK_ID_FIELD_NAME = "taskId";

    public final static String EVENT_LOG_ENDPOINT_FIELD_NAME = "endpoint";
    public final static String EVENT_LOG_PROJECT_FIELD_NAME = "projectName";
    public final static String EVENT_LOG_LOGSTORE_FIELD_NAME = "logstoreName";
    public final static String EVENT_LOG_SHARD_FIELD_NAME = "shardId";
    public final static String EVENT_LOG_BEGIN_CURSOR_FIELD_NAME = "beginCursor";
    public final static String EVENT_LOG_END_CURSOR_FIELD_NAME = "endCursor";

    public final static String RET_INGEST_LINES_FIELD_NAME = "ingestLines";
    public final static String RET_INGEST_BYTES_FIELD_NAME = "ingestBytes";
    public final static String RET_SHIP_LINES_FIELD_NAME = "shipLines";
    public final static String RET_SHIP_BYTES_FIELD_NAME = "shipBytes";
}
