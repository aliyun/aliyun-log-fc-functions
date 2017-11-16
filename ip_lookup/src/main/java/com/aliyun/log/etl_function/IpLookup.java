package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.StreamRequestHandler;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.aliyun.fc.runtime.*;
import com.aliyun.log.etl_function.common.FunctionEvent;
import com.aliyun.log.etl_function.common.FunctionResponse;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.*;
import com.aliyun.openservices.log.request.PutLogsRequest;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;

public class IpLookup implements StreamRequestHandler {

    private final static int MAX_RETRY_TIMES = 30;
    private final static int RETRY_SLEEP_MILLIS = 200;
    private final static int QUOTA_RETRY_SLEEP_MILLIS = 2000;
    private final static Boolean IGNORE_FAIL = false;
    private static ArrayList<IpData> ipDataDictCache = new ArrayList<IpData>();
    private FunctionComputeLogger logger = null;
    private FunctionEvent event = null;
    private IpLookupParameter parameter = null;
    private Client targetClient = null;
    private String accessKeyId = "";
    private String accessKeySecret = "";
    private String securityToken = "";
    private int sampleErrorIpCount = 0;

    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        this.logger = context.getLogger();
        this.event = new FunctionEvent(this.logger);
        this.event.parseFromInputStream(inputStream);
        this.parameter = new IpLookupParameter(this.logger);
        this.parameter.parseFromJsonObject(this.event.getParameterJsonObject());

        Credentials credentials = context.getExecutionCredentials();
        this.accessKeyId = credentials.getAccessKeyId();
        this.accessKeySecret = credentials.getAccessKeySecret();
        this.securityToken = credentials.getSecurityToken();

        if (ipDataDictCache.size() == 0) {
            initIpData();
        }

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
            int invalidRetryTime = 0;
            int retryTime = 0;
            while (true) {
                String errorCode, errorMessage;
                int sleepMillis = RETRY_SLEEP_MILLIS;
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
                if (processData(fastLogGroup)) {
                    response.addShipLines(fastLogGroup.getLogsCount());
                    response.addShipBytes(logGroupBytes.length);
                }
            }
            cursor = nextCursor;
            if (cursor.equals(logEndCurosr)) {
                /*
                this.logger.debug("read logstore shard to defined cursor success, project_name: " + logProjectName +
                        ", job_name: " + this.event.getJobName() + ", task_id: " + this.event.getTaskId()
                        + ", current cursor: " + cursor + ", defined cursor: " + logEndCurosr);
                */
                break;
            }
        }
        outputStream.write(response.toJsonString().getBytes());
    }

    private boolean processData(FastLogGroup fastLogGroup) throws IOException {
        String ipKeyName = this.parameter.getIpKeyName();
        String countryKeyName = this.parameter.getCountryKeyName();
        String provinceKeyName = this.parameter.getProvinceKeyName();
        String cityKeyName = this.parameter.getCityKeyName();
        String ispKeyName = this.parameter.getIspKeyName();
        String targetProjectName = this.parameter.getTargetLogProjectName();
        String targetLogstoreName = this.parameter.getTargetLogLogstoreName();
        ArrayList<LogItem> logItems = new ArrayList<LogItem>();
        for (int lIdx = 0; lIdx < fastLogGroup.getLogsCount(); ++lIdx) {
            FastLog log = fastLogGroup.getLogs(lIdx);
            LogItem item = new LogItem();
            item.SetTime(log.getTime());
            for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                FastLogContent content = log.getContents(cIdx);
                String key = content.getKey();
                String value = content.getValue();
                if (key.equals(ipKeyName)) {
                    IpData ipData = findIpData(value);
                    if (ipData != null) {
                        if (countryKeyName.length() > 0) {
                            item.PushBack(countryKeyName, ipData.getCountry());
                        }
                        if (provinceKeyName.length() > 0) {
                            item.PushBack(provinceKeyName, ipData.getProvince());
                        }
                        if (cityKeyName.length() > 0) {
                            item.PushBack(cityKeyName, ipData.getCity());
                        }
                        if (ispKeyName.length() > 0) {
                            item.PushBack(ispKeyName, ipData.getIsp());
                        }
                    } else {
                        if (sampleErrorIpCount % 100 == 0) {
                            this.logger.warn("can not find ip data for " + value);
                        }
                        ++sampleErrorIpCount;
                    }
                }
                item.PushBack(key, value);
            }
            logItems.add(item);
        }
        if (logItems.size() == 0) {
            return true;
        }
        PutLogsRequest req = new PutLogsRequest(targetProjectName, targetLogstoreName, fastLogGroup.hasTopic() ? fastLogGroup.getTopic() : "",
                fastLogGroup.hasSource() ? fastLogGroup.getSource() : "", logItems);
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

    private void initIpData() throws IOException {

        this.logger.info("start to init IpData from oss object");
        InputStream inputStream = null;
        OSSObject ossObject = null;
        OSSClient ossClient = new OSSClient(this.parameter.getOssEndpointOfIpData(), this.accessKeyId, this.accessKeySecret, this.securityToken);
        int retryTime = 0;
        while (true) {
            ++retryTime;
            try {
                ossObject = ossClient.getObject(new GetObjectRequest(this.parameter.getOssBucketOfIpData(), this.parameter.getOssObjectOfIpData()));
                inputStream = ossObject.getObjectContent();
                break;
            } catch (OSSException oe) {
                this.logger.warn("get OSS object catch OSSException, error_code: " + oe.getErrorCode() + ", error_message: "
                        + oe.getErrorMessage() + ", request_id: " + oe.getRequestId() + ", host_id: " + oe.getHostId());
            } catch (ClientException ce) {
                this.logger.error("get OSS object catch ClientException, error_code: " + ce.getErrorCode() + ", error_message: "
                        + ce.getErrorMessage() + ", request_id: " + ce.getRequestId());
            }
            if (retryTime >= 10) {
                throw new IOException("get OSS object of ipData fail");
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            GZIPInputStream ungzip = new GZIPInputStream(inputStream);
            byte[] buffer = new byte[1024 * 1024];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
        } catch (IOException e) {
            this.logger.error("gzip uncompress error, exception: " + e.getMessage());
            throw new IOException("unzip ipData file fail");
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(out.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        if (ipDataDictCache.size() == 0) {
            while ((line = reader.readLine()) != null) {
                String fields[] = line.trim().split(",");
                if (fields.length == 16) {
                    try {
                        ipDataDictCache.add(new IpData(Long.parseLong(fields[0].trim()),
                                Long.parseLong(fields[1].trim()),
                                trimQuote(fields[4].trim()),
                                trimQuote(fields[8].trim()),
                                trimQuote(fields[10].trim()),
                                trimQuote(fields[14].trim())));
                    } catch (NumberFormatException e) {
                        this.logger.warn("invalid ipData, " + line + ", exception: " + e.getMessage());
                    }
                } else {
                    this.logger.warn("invalid ipData, " + line + ", unexpected fields count");
                }
            }
        }
        ossClient.shutdown();
        this.logger.info("finish init IpData, size: " + ipDataDictCache.size());
    }

    private IpData findIpData(String ip) {
        String[] ipFields = ip.trim().split("\\.");
        if (ipFields.length != 4) {
            if (sampleErrorIpCount % 1000 == 0) {
                this.logger.error("invalid ip value: " + ip);
            }
            ++sampleErrorIpCount;
            return null;
        }
        long ipKey;
        try {
            ipKey = (Long.parseLong(ipFields[0]) << 24) +
                    (Long.parseLong(ipFields[1]) << 16) +
                    (Long.parseLong(ipFields[2]) << 8) +
                    (Long.parseLong(ipFields[3]));
        } catch (NumberFormatException e) {
            if (sampleErrorIpCount % 1000 == 0) {
                this.logger.error("invalid ip value: " + ip + ", exception: " + e.getMessage());
            }
            ++sampleErrorIpCount;
            return null;
        }
        int start = 0;
        int end = ipDataDictCache.size() - 1;
        int mid;
        while (start <= end) {
            mid = (start + end) / 2;
            IpData midIpData = ipDataDictCache.get(mid);
            if (ipKey < midIpData.getBeginIp()) {
                end = mid - 1;
            } else if (ipKey <= midIpData.getEndIp()) {
                return midIpData;
            } else {
                start = mid + 1;
            }
        }
        return null;
    }

    private String trimQuote(String str) {
        if (str.length() >= 2 && str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"') {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }
}