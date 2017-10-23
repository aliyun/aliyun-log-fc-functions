package com.aliyun.log.etl_function.common;

import net.sf.json.JSONObject;

public class FunctionResponse {

    private long ingestLines = 0;
    private long ingestBytes = 0;
    private long shipLines = 0;
    private long shipBytes = 0;

    public FunctionResponse() {
    }

    public void addIngestLines(long lines) {
        this.ingestLines += lines;
    }

    public void addIngestBytes(long bytes) {
        this.ingestBytes += bytes;
    }

    public void addShipLines(long lines) {
        this.shipLines += lines;
    }

    public void addShipBytes(long bytes) {
        this.shipBytes += bytes;
    }

    public void setIngestLines(long ingestLines) {
        this.ingestLines = ingestLines;
    }

    public void setIngestBytes(long ingestBytes) {
        this.ingestBytes = ingestBytes;
    }

    public void setShipLines(long shipLines) {
        this.shipLines = shipLines;
    }

    public void setShipBytes(long shipBytes) {
        this.shipBytes = shipBytes;
    }

    public String toJsonString() {
        JSONObject retObj = new JSONObject();
        retObj.put(Consts.RET_INGEST_LINES_FIELD_NAME, this.ingestLines);
        retObj.put(Consts.RET_INGEST_BYTES_FIELD_NAME, this.ingestBytes);
        retObj.put(Consts.RET_SHIP_LINES_FIELD_NAME, this.shipLines);
        retObj.put(Consts.RET_SHIP_BYTES_FIELD_NAME, this.shipBytes);
        return retObj.toString();
    }
}
