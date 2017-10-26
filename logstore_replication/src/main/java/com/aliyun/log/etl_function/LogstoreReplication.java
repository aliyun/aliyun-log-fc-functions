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

    private final static int MAX_RETRY_TIMES = 20;
    private final static int RETRY_SLEEP_MILLIS = 100;
    private final static Boolean IGNORE_FAIL = false;
    private FunctionComputeLogger logger = null;
    private FunctionEvent event = null;
    private LogstoreReplicationParameter parameter = null;
    private Client targetClient = null;
    private String accessKeyId = "";
    private String accessKeySecret = "";
    private String securityToken = "";
    private Random random = new Random(System.currentTimeMillis());

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
        String logEndCurosr = this.event.getLogEndCursor();

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
            while (true) {
                ++retryTime;
                try {
                    BatchGetLogResponse logDataRes = sourceClient.BatchGetLog(logProjectName, logLogstoreName, logShardId,
                            3, cursor, logEndCurosr);
                    logGroupDataList = logDataRes.GetLogGroups();
                    nextCursor = logDataRes.GetNextCursor();
                    /*
                    this.logger.debug("BatchGetLog success, project_name: " + logProjectName + ", job_name: " + this.event.getJobName()
                            + ", task_id: " + this.event.getTaskId() + ", cursor: " + cursor + ", logGroup count: " + logGroupDataList.size());
                    */
                    break;
                } catch (LogException e) {
                    String errorCode = e.GetErrorCode();
                    this.logger.warn("BatchGetLog fail, project_name: " + logProjectName + ", job_name: " + this.event.getJobName()
                            + ", task_id: " + this.event.getTaskId() + ", retry_time: " + retryTime + "/" + MAX_RETRY_TIMES + ", error_code: "
                            + errorCode + ", error_message: " + e.GetErrorMessage().replaceAll("\\n", " ") + ", request_id: " + e.GetRequestId());
                    if (retryTime >= MAX_RETRY_TIMES) {
                        throw new IOException("BatchGetLog fail, retry_time: " + retryTime + ", error_code: " + errorCode
                                + ", error_message: " + e.GetErrorMessage().replaceAll("\\n", " ") + ", request_id: " + e.GetRequestId());
                    }
                    int sleepMillis = RETRY_SLEEP_MILLIS;
                    if (errorCode.equalsIgnoreCase("ReadQuotaExceed") || errorCode.equalsIgnoreCase("ShardReadQuotaExceed")) {
                        sleepMillis *= this.random.nextInt(5) + 2;
                    }
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException ie) {
                    }
                }
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
            cursor = nextCursor;
            if (cursor.equals(logEndCurosr)) {
                /*
                this.logger.debug("read logstore shard to defined cursor done, project_name: " + logProjectName +
                        ", job_name: " + this.event.getJobName() + ", task_id: " + this.event.getTaskId()
                        + ", current cursor: " + cursor + ", defined cursor: " + logEndCurosr);
                */
                break;
            }
        }
        outputStream.write(response.toJsonString().getBytes());
    }

    private boolean processData(FastLogGroup fastLogGroup, byte[] logGroupBytes) throws IOException {
        String targetProjectName = this.parameter.getTargetLogProjectName();
        String targetLogstoreName = this.parameter.getTargetLogLogstoreName();
        PutLogsRequest req = new PutLogsRequest(targetProjectName, targetLogstoreName, fastLogGroup.hasTopic() ? fastLogGroup.getTopic() : "",
                fastLogGroup.hasSource() ? fastLogGroup.getSource() : "", logGroupBytes, null);
        int retryTime = 0;
        while (true) {
            ++retryTime;
            try {
                this.targetClient.PutLogs(req);
                /*
                this.logger.debug("PutLogs success, project_name: " + this.event.getLogProjectName() + ", job_name: " + this.event.getJobName()
                        + ", task_id: " + this.event.getTaskId() + ", target(" + targetProjectName + "/" + targetLogstoreName +
                        "), logs: " + fastLogGroup.getLogsCount());
                */
                return true;
            } catch (LogException e) {
                String errorCode = e.GetErrorCode();
                this.logger.warn("PutLogs fail, project_name: " + this.event.getLogProjectName() + ", job_name: " + this.event.getJobName()
                        + ", task_id: " + this.event.getTaskId() + ", target(" + targetProjectName + "/" + targetLogstoreName +
                        "), retry_time: " + retryTime + "/" + MAX_RETRY_TIMES + ", error_code: " + e.GetErrorCode() +
                        ", error_message: " + e.GetErrorMessage().replaceAll("\\n", " ") + ", request_id: " + e.GetRequestId());
                if (retryTime >= MAX_RETRY_TIMES) {
                    if (errorCode.equalsIgnoreCase("PostBodyInvalid") || (IGNORE_FAIL && !errorCode.equalsIgnoreCase("Unauthorized"))) {
                        this.logger.error("ignore this fail PostLogStoreLogs request, discard data, request_id: " + e.GetRequestId());
                        break;
                    }
                    throw new IOException("PutLogs fail, retryTime: " + retryTime + ", error_code: " + errorCode
                            + ", error_message: " + e.GetErrorMessage().replaceAll("\\n", " ") + ", request_id: " + e.GetRequestId());
                }
                int sleepMillis = RETRY_SLEEP_MILLIS;
                if (errorCode.equalsIgnoreCase("WriteQuotaExceed") || errorCode.equalsIgnoreCase("ShardWriteQuotaExceed")) {
                    sleepMillis *= this.random.nextInt(5) + 2;
                }
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ie) {
                }
            }
        }
        return false;
    }

}