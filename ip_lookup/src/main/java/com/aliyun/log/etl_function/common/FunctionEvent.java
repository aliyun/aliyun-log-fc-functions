package com.aliyun.log.etl_function.common;

import com.aliyun.fc.runtime.FunctionComputeLogger;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;

public class FunctionEvent {

    private FunctionComputeLogger logger;
    private String jobName;
    private String taskId;
    private String logEndpoint;
    private String logProjectName;
    private String logLogstoreName;
    private int logShardId;
    private String logBeginCursor;
    private String logEndCursor;
    private JSONObject parameterJsonObject = null;

    public FunctionEvent(FunctionComputeLogger logger) {
        this.logger = logger;
    }

    public void parseFromInputStream(InputStream inputStream) throws IOException {

        byte[] buffer = new byte[4096];
        int readBytes = 0;
        StringBuilder stringBuilder = new StringBuilder();
        try {
            while((readBytes = inputStream.read(buffer)) > 0){
                stringBuilder.append(new String(buffer, 0, readBytes));
            }
        } catch (IOException e) {
            this.logger.error("read inputStream fail, exception: " + e.getMessage());
            throw new IOException("read inputStream fail, exception: " + e.getMessage());
        }

        String inputString = stringBuilder.toString();
        JSONObject rootObj = null;
        try {
            rootObj = JSONObject.fromObject(inputString);
            JSONObject sourceObj = rootObj.getJSONObject(Consts.EVENT_SOURCE_FIELD_NAME);
            this.logEndpoint = sourceObj.getString(Consts.EVENT_LOG_ENDPOINT_FIELD_NAME);
            this.logProjectName = sourceObj.getString(Consts.EVENT_LOG_PROJECT_FIELD_NAME);
            this.logLogstoreName = sourceObj.getString(Consts.EVENT_LOG_LOGSTORE_FIELD_NAME);
            this.logShardId = sourceObj.getInt(Consts.EVENT_LOG_SHARD_FIELD_NAME);
            this.logBeginCursor = sourceObj.getString(Consts.EVENT_LOG_BEGIN_CURSOR_FIELD_NAME);
            this.logEndCursor = sourceObj.getString(Consts.EVENT_LOG_END_CURSOR_FIELD_NAME);
            parameterJsonObject = rootObj.getJSONObject(Consts.EVENT_PARAMETER_FIELD_NAME);
        } catch (JSONException e) {
            this.logger.error("parse inputStream to json event fail, exception: " + e.getMessage());
            throw new IOException("parse inputStream to json event fail, exception: " + e.getMessage());
        }
        try {
            this.jobName = rootObj.getString(Consts.EVENT_JOB_NAME_FIELD_NAME);
        } catch (JSONException e) {
            this.jobName = "";
        }
        try {
            this.taskId = rootObj.getString(Consts.EVENT_TASK_ID_FIELD_NAME);
        } catch (JSONException e) {
            this.taskId = "";
        }
    }

    public String getJobName() {
        return jobName;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getLogEndpoint() {
        return logEndpoint;
    }

    public String getLogProjectName() {
        return logProjectName;
    }

    public String getLogLogstoreName() {
        return logLogstoreName;
    }

    public int getLogShardId() {
        return logShardId;
    }

    public String getLogBeginCursor() {
        return logBeginCursor;
    }

    public String getLogEndCursor() {
        return logEndCursor;
    }

    public JSONObject getParameterJsonObject() {
        return parameterJsonObject;
    }
}
