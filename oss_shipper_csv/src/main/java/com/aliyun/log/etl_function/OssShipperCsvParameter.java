package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.FunctionComputeLogger;
import com.aliyun.log.etl_function.common.Consts;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;

public class OssShipperCsvParameter{

    public enum COMPRESS_TYPE {SNAPPY, GZIP, NONE};

    private FunctionComputeLogger logger;
    private char delimiterChar = ',';
    private char quoteChar = '\"';
    private char escapeChar = '\"';
    private String nullColumnValue = "";
    private ArrayList<String> columnNames;
    private boolean enableHeader = false;
    private String ossEndpoint;
    private String ossBucket;
    private String ossPrefix;
    private String ossPostfix;
    private String ossDateFormat;
    private COMPRESS_TYPE compressType;

    public OssShipperCsvParameter(FunctionComputeLogger logger) {
        this.logger = logger;
    }

    public void parseFromJsonObject(JSONObject jsonObj) throws IOException {
        try {
            JSONObject targetObj = jsonObj.getJSONObject("target");
            this.ossEndpoint = targetObj.getString("ossEndpoint");
            this.ossBucket = targetObj.getString("ossBucket");
            this.ossPrefix = targetObj.getString("ossPrefix");
            this.ossPostfix = targetObj.getString("ossPostfix");
            this.ossDateFormat = targetObj.getString("ossDateFormat");
            JSONObject transoformObj = jsonObj.getJSONObject("transform");
            this.delimiterChar = transoformObj.getString("delimiterChar").charAt(0);
            this.quoteChar = transoformObj.getString("quoteChar").charAt(0);
            this.escapeChar = transoformObj.getString("escapeChar").charAt(0);
            this.nullColumnValue = transoformObj.getString("nullColumnValue");
            this.columnNames = new ArrayList<String>(transoformObj.getJSONArray("columnNames"));
            this.enableHeader = transoformObj.getBoolean("enableHeader");
            if (transoformObj.containsKey("compressType")) {
                String ct = transoformObj.getString("compressType").toLowerCase();
                if (ct.equals("gzip")) {
                    this.compressType = COMPRESS_TYPE.GZIP;
                } else if (ct.equals("snappy")) {
                    this.compressType = COMPRESS_TYPE.SNAPPY;
                } else {
                    this.compressType = COMPRESS_TYPE.NONE;
                }
            } else {
                this.compressType = COMPRESS_TYPE.NONE;
            }
        } catch (JSONException e) {
            this.logger.error("invalid function parameter, exception: " + e.getMessage());
            throw new IOException("invalid function parameter, exception: " + e.getMessage());
        }
    }

    public char getDelimiterChar() {
        return delimiterChar;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public char getEscapeChar() {
        return escapeChar;
    }

    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    public String getNullColumnValue() {
        return nullColumnValue;
    }

    public boolean isEnableHeader() {
        return enableHeader;
    }

    public String getOssEndpoint() {
        return ossEndpoint;
    }

    public String getOssBucket() {
        return ossBucket;
    }

    public String getOssPrefix() {
        return ossPrefix;
    }

    public String getOssPostfix() {
        return ossPostfix;
    }

    public String getOssDateFormat() {
        return ossDateFormat;
    }

    public COMPRESS_TYPE getCompressType() {
        return compressType;
    }
}
