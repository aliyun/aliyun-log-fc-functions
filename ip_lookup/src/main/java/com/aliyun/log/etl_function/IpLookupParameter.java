package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.FunctionComputeLogger;
import com.aliyun.log.etl_function.common.Consts;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import java.io.IOException;

public class IpLookupParameter {

    private FunctionComputeLogger logger;
    private String targetLogEndpoint;
    private String targetLogProjectName;
    private String targetLogLogstoreName;
    private String ossEndpointOfIpData;
    private String ossBucketOfIpData;
    private String ossObjectOfIpData;
    private String ipKeyName;
    private String countryKeyName;
    private String regionKeyName;
    private String provinceKeyName;
    private String cityKeyName;
    private String ispKeyName;

    public IpLookupParameter(FunctionComputeLogger logger) {
        this.logger = logger;
    }

    public void parseFromJsonObject(JSONObject jsonObj) throws IOException {
        try {
            JSONObject targetObj = jsonObj.getJSONObject(Consts.EVENT_TARGET_FIELD_NAME);
            this.targetLogEndpoint = targetObj.getString(Consts.EVENT_LOG_ENDPOINT_FIELD_NAME);
            this.targetLogProjectName = targetObj.getString(Consts.EVENT_LOG_PROJECT_FIELD_NAME);
            this.targetLogLogstoreName = targetObj.getString(Consts.EVENT_LOG_LOGSTORE_FIELD_NAME);
            JSONObject transformObj = jsonObj.getJSONObject(Consts.EVENT_TRANSFORM_FIELD_NAME);
            this.ossEndpointOfIpData = transformObj.getString("ossEndpointOfIpData");
            this.ossBucketOfIpData = transformObj.getString("ossBucketOfIpData");
            this.ossObjectOfIpData = transformObj.getString("ossObjectOfIpData");
            if (transformObj.containsKey("ipKeyName")) {
                this.ipKeyName = transformObj.getString("ipKeyName");
            } else {
                this.ipKeyName = "";
            }
            if (transformObj.containsKey("countryKeyName")) {
                this.countryKeyName = transformObj.getString("countryKeyName");
            } else {
                this.countryKeyName = "";
            }
            if (transformObj.containsKey("regionKeyName")) {
                this.regionKeyName = transformObj.getString("regionKeyName");
            } else {
                this.regionKeyName = "";
            }
            if (transformObj.containsKey("provinceKeyName")) {
                this.provinceKeyName = transformObj.getString("provinceKeyName");
            } else {
                this.provinceKeyName = "";
            }
            if (transformObj.containsKey("cityKeyName")) {
                this.cityKeyName = transformObj.getString("cityKeyName");
            } else {
                this.cityKeyName = "";
            }
            if (transformObj.containsKey("ispKeyName")) {
                this.ispKeyName = transformObj.getString("ispKeyName");
            } else {
                this.ispKeyName = "";
            };
        } catch (JSONException e) {
            this.logger.error("invalid function parameter, exception: " + e.getMessage());
            throw new IOException("invalid function parameter, exception: " + e.getMessage());
        }
    }

    public String getTargetLogEndpoint() {
        return targetLogEndpoint;
    }

    public String getTargetLogProjectName() {
        return targetLogProjectName;
    }

    public String getTargetLogLogstoreName() {
        return targetLogLogstoreName;
    }

    public String getOssEndpointOfIpData() {
        return ossEndpointOfIpData;
    }

    public String getOssBucketOfIpData() {
        return ossBucketOfIpData;
    }

    public String getOssObjectOfIpData() {
        return ossObjectOfIpData;
    }

    public String getIpKeyName() {
        return ipKeyName;
    }

    public String getCountryKeyName() {
        return countryKeyName;
    }

    public String getRegionKeyName() { return regionKeyName; }

    public String getProvinceKeyName() {
        return provinceKeyName;
    }

    public String getCityKeyName() {
        return cityKeyName;
    }

    public String getIspKeyName() {
        return ispKeyName;
    }
}
