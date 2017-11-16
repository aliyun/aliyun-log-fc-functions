package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.StreamRequestHandler;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import com.aliyun.fc.runtime.*;
import com.aliyun.log.etl_function.common.FunctionEvent;
import com.aliyun.log.etl_function.common.FunctionResponse;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.*;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.opencsv.CSVWriter;
import org.xerial.snappy.Snappy;

public class OssShipperCsv implements StreamRequestHandler {

    private final static int MAX_RETRY_TIMES = 30;
    private final static int LOG_RETRY_SLEEP_MILLIS = 200;
    private final static int LOG_QUOTA_RETRY_SLEEP_MILLIS = 2000;
    private final static int OSS_RETRY_SLEEP_MILLIS = 500;
    private final static String DEFAULT_OSS_DATEFORMAT = "yyyy/MM/dd/HH/mm";
    private FunctionComputeLogger logger = null;
    private FunctionEvent event = null;
    private OssShipperCsvParameter parameter = null;
    private String accessKeyId = "";
    private String accessKeySecret = "";
    private String securityToken = "";
    private CSVWriter csvWriter;

    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        this.logger = context.getLogger();
        this.event = new FunctionEvent(this.logger);
        this.event.parseFromInputStream(inputStream);
        this.parameter = new OssShipperCsvParameter(this.logger);
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

        String cursor = logBeginCursor;
        FunctionResponse response = new FunctionResponse();
        Writer sWriter = new StringWriter(100 * 1024 * 1024);
        this.csvWriter = new CSVWriter(sWriter, this.parameter.getDelimiterChar(), this.parameter.getQuoteChar(), this.parameter.getEscapeChar(), "\n");
        if (this.parameter.isEnableHeader()) {
            String[] header = new String[this.parameter.getColumnNames().size()];
            int idx = 0;
            for (String name : this.parameter.getColumnNames()) {
                header[idx++] = name;
            }
            csvWriter.writeNext(header, false);
        }
        while (true) {
            List<LogGroupData> logGroupDataList = null;
            String nextCursor = "";
            int invalidRetryTime = 0;
            int retryTime = 0;
            while (true) {
                String errorCode, errorMessage;
                int sleepMillis = LOG_RETRY_SLEEP_MILLIS;
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
                } catch (LogException le) {
                    errorCode = le.GetErrorCode();
                    errorMessage = le.GetErrorMessage().replaceAll("\\n", " ");
                    this.logger.warn("BatchGetLog fail, project_name: " + logProjectName + ", job_name: " + this.event.getJobName()
                            + ", task_id: " + this.event.getTaskId() + ", retry_time: " + retryTime + "/" + MAX_RETRY_TIMES + ", error_code: "
                            + errorCode + ", error_message: " + errorMessage + ", request_id: " + le.GetRequestId());
                    if (errorCode.equalsIgnoreCase("ReadQuotaExceed") || errorCode.equalsIgnoreCase("ShardReadQuotaExceed")) {
                        sleepMillis = LOG_QUOTA_RETRY_SLEEP_MILLIS;
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
                response.addIngestLines(fastLogGroup.getLogsCount());
                if (processData(fastLogGroup)) {
                    response.addShipLines(fastLogGroup.getLogsCount());
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
        csvWriter.flush();
        csvWriter.close();
        csvWriter = null;
        putOssObject(sWriter);
        outputStream.write(response.toJsonString().getBytes());
    }

    private boolean processData(FastLogGroup fastLogGroup) throws IOException {

        ArrayList<String> columnNames = this.parameter.getColumnNames();
        int logCount = fastLogGroup.getLogsCount();
        int tagCount = fastLogGroup.getLogTagsCount();
        for (int i = 0; i < logCount; ++i) {
            FastLog log = fastLogGroup.getLogs(i);
            int contentCount = log.getContentsCount();
            String[] line = new String[columnNames.size()];
            int idx = 0;
            for (String name : columnNames) {
                if (name.equals("__source__")) {
                    line[idx++] = fastLogGroup.getSource();
                } else if (name.equals("__topic__")) {
                    line[idx++] = fastLogGroup.getTopic();
                } else if (name.startsWith("__tag__:") && name.length() > 8) {
                    int j = 0;
                    for ( ; j < tagCount; ++j) {
                        FastLogTag tag = fastLogGroup.getLogTags(j);
                        if (tag.getKey().equals(name.substring(8))) {
                            line[idx++] = tag.getValue();
                            break;
                        }
                        if (j == tagCount) {
                            line[idx++] = this.parameter.getNullColumnValue();
                        }
                    }
                } else {
                    int j = 0;
                    for (; j < contentCount; ++j) {
                        FastLogContent content = log.getContents(j);
                        if (content.getKey().equals(name)) {
                            line[idx++] = content.getValue();
                            break;
                        }
                    }
                    if (j == contentCount) {
                        line[idx++] = this.parameter.getNullColumnValue();
                    }
                }
            }
            this.csvWriter.writeNext(line, false);
        }
        return true;
    }

    private String calculateObjectName() {

        String ossPrefix = this.parameter.getOssPrefix();
        String ossDateFormat = this.parameter.getOssDateFormat();
        SimpleDateFormat dateFormat = null;
        try {
            dateFormat = new SimpleDateFormat(ossDateFormat);
        } catch (Exception e) {
            dateFormat = new SimpleDateFormat(DEFAULT_OSS_DATEFORMAT);
            this.logger.warn("ossDateFormat" + ossDateFormat + " not valid, use " + DEFAULT_OSS_DATEFORMAT + " as default");
        }
        StringBuilder sb = new StringBuilder(ossPrefix);
        sb.append(dateFormat.format((long) this.event.getCursorTime() * 1000).toString())
                .append("/").append(this.event.getLogShardId())
                .append("_").append(this.event.getLogBeginCursor())
                .append("_").append(this.event.getLogEndCursor())
                .append(this.parameter.getOssPostfix());
        return sb.toString();
    }

    private ByteArrayInputStream compressObject(String rawStr) {
        switch (this.parameter.getCompressType()) {
            case SNAPPY:
                try {
                    byte[] compressed = Snappy.compress(rawStr.getBytes("UTF-8"));
                    return new ByteArrayInputStream(compressed);
                } catch (IOException e) {
                    this.logger.error("convert string to utf8 bytes fail, exception: " + e.getMessage());
                    return null;
                }
            case GZIP:
                try {
                    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                    GZIPOutputStream gzipOutStream = new GZIPOutputStream(outStream);
                    gzipOutStream.write(rawStr.getBytes("UTF-8"));
                    gzipOutStream.close();
                    return new ByteArrayInputStream(outStream.toByteArray());
                } catch (IOException e) {
                    this.logger.error("gzip compress fail, exception: " + e.getMessage());
                    return null;
                }
            default:
                try {
                    return new ByteArrayInputStream(rawStr.getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    this.logger.error("convert string to utf8 bytes fail, exception: " + e.getMessage());
                    return null;
                }
        }
    }

    private void putOssObject(Writer sWriter) throws IOException {
        String logProjectName = this.event.getLogProjectName();
        OSSClient ossClient = new OSSClient(this.parameter.getOssEndpoint(), this.accessKeyId, this.accessKeySecret, this.securityToken);
        String objectName = calculateObjectName();
        ByteArrayInputStream byteStream = compressObject(sWriter.toString());
        if (byteStream == null) {
            throw new IOException("process data bytes fail before putObject");
        }
        int retryTime = 0;
        while (true) {
            String errorCode, errorMessage;
            try {
                ossClient.putObject(this.parameter.getOssBucket(), objectName, byteStream);
                ossClient.shutdown();
                return;
            } catch (OSSException oe) {
                errorCode = oe.getErrorCode();
                errorMessage = oe.getErrorMessage().replaceAll("\\n", " ");
                this.logger.warn("PutObject to OSS fail, project_name: " + logProjectName + ", job_name: "
                        + this.event.getJobName() + ", task_id: " + this.event.getTaskId() + ", retry_time: "
                        + retryTime + "/" + MAX_RETRY_TIMES + ", error_code: " + errorCode + ", error_message: "
                        + errorMessage + ", request_id: " + oe.getRequestId());
            } catch (ClientException ce) {
                errorCode = ce.getErrorCode();
                errorMessage = ce.getErrorMessage().replaceAll("\\n", " ");
                this.logger.warn("PutObject to OSS fail, project_name: " + logProjectName + ", job_name: "
                        + this.event.getJobName() + ", task_id: " + this.event.getTaskId() + ", retry_time: "
                        + retryTime + "/" + MAX_RETRY_TIMES + ", error_code: " + errorCode + ", error_message: "
                        + errorMessage + ", request_id: " + ce.getRequestId());

            } catch (Exception e) {
                errorCode = "UnknownException";
                errorMessage = e.getMessage().replaceAll("\\n", " ");
                this.logger.error("PutObject to OSS fail, project_name: " + logProjectName + ", job_name: "
                        + this.event.getJobName() + ", task_id: " + this.event.getTaskId() + ", retry_time: "
                        + retryTime + "/" + MAX_RETRY_TIMES + ", error_message: " + errorMessage);
            }
            ++retryTime;
            if (retryTime >= this.MAX_RETRY_TIMES) {
                throw new IOException("PutObject fail, retry_time: " + retryTime + ", error_code: " + errorCode
                        + ", error_message: " + errorMessage);
            }
            try {
                Thread.sleep(this.OSS_RETRY_SLEEP_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}