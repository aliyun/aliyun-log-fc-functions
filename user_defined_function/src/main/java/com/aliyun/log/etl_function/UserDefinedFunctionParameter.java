package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.FunctionComputeLogger;
import com.aliyun.log.etl_function.common.Consts;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import java.io.IOException;

public class UserDefinedFunctionParameter{

    private FunctionComputeLogger logger;
    /* define your own member variables below for this function, e.g.,
     private String targetLogEndpoint;
     */

    public UserDefinedFunctionParameter(FunctionComputeLogger logger) {
        this.logger = logger;
    }

    public void parseFromJsonObject(JSONObject jsonObj) throws IOException {
        try {
            /* parse member variables from jsonObject here, e.g.,
            JSONObject targetObj = jsonObj.getJSONObject(Consts.EVENT_TARGET_FIELD_NAME);
            this.targetLogEndpoint = targetObj.getString(Consts.EVENT_LOG_ENDPOINT_FIELD_NAME);
            */
        } catch (JSONException e) {
            this.logger.error("invalid function parameter, exception: " + e.getMessage());
            throw new IOException("invalid function parameter, exception: " + e.getMessage());
        }
    }

    /* getters for member variables, e.g. ,
    public String getTargetLogEndpoint() {
        return targetLogEndpoint;
    }
    */
}
