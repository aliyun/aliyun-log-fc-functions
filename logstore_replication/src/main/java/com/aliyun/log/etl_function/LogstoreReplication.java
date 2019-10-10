package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.StreamRequestHandler;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.aliyun.fc.runtime.*;
import com.aliyun.log.etl_function.common.FunctionEvent;
import com.aliyun.log.etl_function.common.FunctionResponse;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.request.PutLogsRequest;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;

public class LogstoreReplication implements StreamRequestHandler {

    private final static int MAX_RETRY_TIMES = 30;
    private final static int RETRY_SLEEP_MILLIS = 200;
    private final static int QUOTA_RETRY_SLEEP_MILLIS = 2000;
    private final static Boolean IGNORE_FAIL = false;
    private FunctionComputeLogger logger = null;
    private FunctionEvent event = null;
    private LogstoreReplicationParameter parameter = null;
    private Client targetClient = null;
    private String accessKeyId = "";
    private String accessKeySecret = "";
    private String securityToken = "";

    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        this.logger = context.getLogger();
        this.event = new FunctionEvent(this.logger);
        this.event.parseFromInputStream(inputStream);
        this.parameter = new LogstoreReplicationParameter(this.logger);
        this.parameter.parseFromJsonObject(this.event.getParameterJsonObject());

        Credentials credentials = context.getExecutionCredentials();
        this.accessKeyId = credentials.getAccessKeyId();
        this.accessKeySecret = credentials.getAccessKeySecret();
        this.securityToken = credentials.getSecurityToken();

        String logProjectName = this.event.getLogProjectName();
        String logLogstoreName = this.event.getLogLogstoreName();
        int logShardId = this.event.getLogShardId();
        String logBeginCursor = this.event.getLogBeginCursor();
        String logEndCursor = this.event.getLogEndCursor();

        Client sourceClient = new Client(this.event.getLogEndpoint(), this.accessKeyId, this.accessKeySecret);
        sourceClient.SetSecurityToken(this.securityToken);

        this.targetClient = new Client(this.parameter.getTargetLogEndpoint(), this.accessKeyId, this.accessKeySecret);
        this.targetClient.SetSecurityToken(this.securityToken);

        String cursor = logBeginCursor;
        FunctionResponse response = new FunctionResponse();

        while (true) {
            List<LogGroupData> logGroupDataList = null;
            String nextCursor = "";
            int retryTime = 0;
            int invalidRetryTime = 0;
            while (true) {
                String errorCode, errorMessage;
                int sleepMillis = RETRY_SLEEP_MILLIS;
                try {
                    BatchGetLogResponse logDataRes = sourceClient.BatchGetLog(logProjectName, logLogstoreName, logShardId,
                            3, cursor, logEndCursor);
                    logGroupDataList = logDataRes.GetLogGroups();
                    nextCursor = logDataRes.GetNextCursor();
                    /*
                    this.logger.debug("BatchGetLog success, project_name: " + logProjectName + ", job_name: " + this.event.getJobName()
                            + ", task_id: " + this.event.getTaskId() + ", cursor: " + cursor + ", logGroup count: " + logGroupDataList.size());
                    */
                    break;
                } catch (LogException le) {
                    errorCode = le.GetErrorCode();
                    errorMessage = le.GetErrorMessage().replaceAll("\\n", " ");
                    this.logger.warn("BatchGetLog fail, project_name: " + logProjectName + ", job_name: " + this.event.getJobName()
                            + ", task_id: " + this.event.getTaskId() + ", retry_time: " + retryTime + "/" + MAX_RETRY_TIMES + ", error_code: "
                            + errorCode + ", error_message: " + errorMessage + ", request_id: " + le.GetRequestId());
                    if (errorCode.equalsIgnoreCase("ReadQuotaExceed") || errorCode.equalsIgnoreCase("ShardReadQuotaExceed")) {
                        sleepMillis = QUOTA_RETRY_SLEEP_MILLIS;
                    } else if (errorCode.equalsIgnoreCase("Unauthorized") || errorCode.equalsIgnoreCase("InvalidAccessKeyId")
                            || errorCode.equalsIgnoreCase("ProjectNotExist") || errorCode.equalsIgnoreCase("LogStoreNotExist")
                            || errorCode.equalsIgnoreCase("ShardNotExist")) {
                        ++invalidRetryTime;
                    }
                } catch (Exception e) {
                    errorCode = "UnknownException";
                    errorMessage = e.getMessage().replaceAll("\\n", " ");
                    this.logger.warn("BatchGetLog fail, project_name: " + logProjectName + ", job_name: " + this.event.getJobName()
                            + ", task_id: " + this.event.getTaskId() + ", retry_time: " + retryTime + "/" + MAX_RETRY_TIMES
                            + ", error_code: " + errorCode + ", error_message: " + errorMessage);
                }
                if (invalidRetryTime >= 2 || retryTime >= MAX_RETRY_TIMES) {
                    throw new IOException("BatchGetLog fail, retry_time: " + retryTime + ", error_code: " + errorCode
                            + ", error_message: " + errorMessage);
                }
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
                ++retryTime;
            }
            for (LogGroupData logGroupData : logGroupDataList) {
                FastLogGroup fastLogGroup = logGroupData.GetFastLogGroup();
                byte[] logGroupBytes = fastLogGroup.getBytes();
                response.addIngestLines(fastLogGroup.getLogsCount());
                response.addIngestBytes(logGroupBytes.length);
                if (processData(fastLogGroup, logGroupBytes)) {
                    response.addShipLines(fastLogGroup.getLogsCount());
                    response.addShipBytes(logGroupBytes.length);
                }
            }
            if (cursor.equals(nextCursor)) {
                /*
                this.logger.debug("read logstore shard to defined cursor done, project_name: " + logProjectName +
                        ", job_name: " + this.event.getJobName() + ", task_id: " + this.event.getTaskId()
                        + ", current cursor: " + cursor + ", defined cursor: " + logEndCurosr);
                */
                break;
            }
            cursor = nextCursor;
        }
        outputStream.write(response.toJsonString().getBytes());
    }

    private boolean processData(FastLogGroup fastLogGroup, byte[] logGroupBytes) throws IOException {
        String targetProjectName = this.parameter.getTargetLogProjectName();
        String targetLogstoreName = this.parameter.getTargetLogLogstoreName();
        PutLogsRequest req = new PutLogsRequest(targetProjectName, targetLogstoreName, fastLogGroup.hasTopic() ? fastLogGroup.getTopic() : "",
                fastLogGroup.hasSource() ? fastLogGroup.getSource() : "", logGroupBytes, null);
        int retryTime = 0;
        int invalidRetryTime = 0;
        while (true) {
            String errorCode, errorMessage;
            int sleepMillis = RETRY_SLEEP_MILLIS;
            try {
                this.targetClient.PutLogs(req);
                /*
                this.logger.debug("PutLogs success, project_name: " + this.event.getLogProjectName() + ", job_name: " + this.event.getJobName()
                        + ", task_id: " + this.event.getTaskId() + ", target(" + targetProjectName + "/" + targetLogstoreName +
                        "), logs: " + fastLogGroup.getLogsCount());
                */
                return true;
            } catch (LogException le) {
                errorCode = le.GetErrorCode();
                errorMessage = le.GetErrorMessage().replaceAll("\\n", " ");
                this.logger.warn("PutLogs fail, project_name: " + targetProjectName + ", job_name: " + this.event.getJobName()
                        + ", task_id: " + this.event.getTaskId() + ", retry_time: " + retryTime + "/" + MAX_RETRY_TIMES + ", error_code: "
                        + errorCode + ", error_message: " + errorMessage + ", request_id: " + le.GetRequestId());
                if (errorCode.equalsIgnoreCase("WriteQuotaExceed") || errorCode.equalsIgnoreCase("ShardWriteQuotaExceed")) {
                    sleepMillis = QUOTA_RETRY_SLEEP_MILLIS;
                } else if (errorCode.equalsIgnoreCase("Unauthorized") || errorCode.equalsIgnoreCase("InvalidAccessKeyId")
                        || errorCode.equalsIgnoreCase("ProjectNotExist") || errorCode.equalsIgnoreCase("LogStoreNotExist")
                        || errorCode.equalsIgnoreCase("ShardNotExist") || errorCode.equalsIgnoreCase("PostBodyInvalid")) {
                    ++invalidRetryTime;
                }
            } catch (Exception e) {
                errorCode = "UnknownException";
                errorMessage = e.getMessage().replaceAll("\\n", " ");
                this.logger.warn("PutLogs fail, project_name: " + targetProjectName + ", job_name: " + this.event.getJobName()
                        + ", task_id: " + this.event.getTaskId() + ", retry_time: " + retryTime + "/" + MAX_RETRY_TIMES
                        + ", error_code: " + errorCode + ", error_message: " + errorMessage);
            }
            if (invalidRetryTime >= 2 || retryTime >= MAX_RETRY_TIMES) {
                if (IGNORE_FAIL) {
                    break;
                } else {
                    throw new IOException("PutLogs fail, retry_time: " + retryTime + ", error_code: " + errorCode
                            + ", error_message: " + errorMessage);
                }
            }
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
            ++retryTime;
        }
        return false;
    }

}