package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.StreamRequestHandler;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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
    private static byte[] ipData = null;
    private static int textOffset;
    private static int[] index;
    private static int[] indexData1;
    private static int[] indexData2;
    private static byte[] indexData3;
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

        if (ipData == null) {
            initIpData();
        }

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
            int invalidRetryTime = 0;
            int retryTime = 0;
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
                if (processData(fastLogGroup)) {
                    response.addShipLines(fastLogGroup.getLogsCount());
                    response.addShipBytes(logGroupBytes.length);
                }
            }
            if (cursor.equals(nextCursor)) {
                /*
                this.logger.debug("read logstore shard to defined cursor success, project_name: " + logProjectName +
                        ", job_name: " + this.event.getJobName() + ", task_id: " + this.event.getTaskId()
                        + ", current cursor: " + cursor + ", defined cursor: " + logEndCurosr);
                */
                break;
            }
            cursor = nextCursor;
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
                    IpData ipData = find(value);
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
        int objectLength = (int) ossObject.getObjectMetadata().getContentLength();
        byte[] dataBytes = new byte[objectLength];
        int length = 0;
        int readLength;
        while (true) {
            readLength = inputStream.read(dataBytes, length, 1024 * 1024);
            if (readLength < 0) {
                break;
            }
            length += readLength;
        }
        ossClient.shutdown();
        loadBinary(dataBytes);
        ipData = dataBytes;
        this.logger.info("finish init IpData, object size: " + objectLength);
    }

    private void loadBinary(byte[] data) {
        textOffset = bigEndian(data, 0);
        index = new int[256];
        for (int i = 0; i < 256; i++) {
            index[i] = littleEndian(data, 4 + i * 4);
        }

        int nidx = (textOffset - 4 - 1024 - 1024) / 8;
        indexData1 = new int[nidx];
        indexData2 = new int[nidx];
        indexData3 = new byte[nidx];

        for (int i = 0, off = 0; i < nidx; i++) {
            off = 4 + 1024 + i * 8;
            indexData1[i] = bigEndian(data, off);
            indexData2[i] = ((int) data[off + 6] & 0xff) << 16 | ((int) data[off + 5] & 0xff) << 8 | ((int) data[off + 4] & 0xff);
            indexData3[i] = data[off + 7];
        }
    }

    private int bigEndian(byte[] data, int offset) {
        int a = (((int) data[offset]) & 0xff);
        int b = (((int) data[offset + 1]) & 0xff);
        int c = (((int) data[offset + 2]) & 0xff);
        int d = (((int) data[offset + 3]) & 0xff);
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    private int littleEndian(byte[] data, int offset) {
        int a = (((int) data[offset]) & 0xff);
        int b = (((int) data[offset + 1]) & 0xff);
        int c = (((int) data[offset + 2]) & 0xff);
        int d = (((int) data[offset + 3]) & 0xff);
        return (d << 24) | (c << 16) | (b << 8) | a;
    }

    private int findIndexOffset(long ip, int start, int end) {
        int mid = 0;
        while (start < end) {
            mid = (start + end) / 2;
            long l = 0xffffffffL & ((long) indexData1[mid]);
            if (ip > l) {
                start = mid + 1;
            } else {
                end = mid;
            }
        }
        long l = ((long) indexData1[end]) & 0xffffffffL;
        if (l >= ip) {
            return end;
        }
        return start;
    }

    private byte parseOctet(String ipPart) {
        // Note: we already verified that this string contains only hex digits.
        int octet = Integer.parseInt(ipPart);
        // Disallow leading zeroes, because no clear standard exists on
        // whether these should be interpreted as decimal or octal.
        if (octet < 0 || octet > 255 || (ipPart.startsWith("0") && ipPart.length() > 1)) {
            throw new NumberFormatException("invalid ip part");
        }
        return (byte) octet;
    }

    private IpData find(String ip) {
        byte[] b = new byte[4];
        try {
            String[] s = ip.split("\\.");
            if (s.length != 4) {
                throw new NumberFormatException("the ip is not v4");
            }
            b[0] = parseOctet(s[0]);
            b[1] = parseOctet(s[1]);
            b[2] = parseOctet(s[2]);
            b[3] = parseOctet(s[3]);
        } catch (Exception e) {
            return null;
        }
        return find(b);
    }

    private IpData find(byte[] ipBin) {
        int end = indexData1.length - 1;
        int a = 0xff & ((int) ipBin[0]);
        if (a != 0xff) {
            end = index[a + 1];
        }
        long ip = (long) bigEndian(ipBin, 0) & 0xffffffffL;
        int idx = findIndexOffset(ip, index[a], end);
        int off = indexData2[idx];
        return buildInfo(ipData, textOffset - 1024 + off, 0xff & (int) indexData3[idx]);
    }

    private IpData buildInfo(byte[] bytes, int offset, int len) {
        String str = new String(bytes, offset, len, Charset.forName("UTF-8"));
        String[] ss = str.split("\t", -1);
        if (ss.length == 4) {
            return new IpData(ss[0], ss[1], ss[2], "");
        } else if (ss.length == 5) {
            return new IpData(ss[0], ss[1], ss[2], ss[4]);
        } else if (ss.length == 3) {
            return new IpData(ss[0], ss[1], ss[2], "");
        } else if (ss.length == 2) {
            return new IpData(ss[0], ss[1], "", "");
        }
        return null;
    }

}