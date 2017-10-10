package com.aliyun.log.etl_function;

public class IpData {

    private long beginIp;
    private long endIp;
    private String country;
    private String region;
    private String province;
    private String city;
    private String isp;

    public IpData(long beginIp, long endIp, String country, String region, String province, String city, String county, String isp) {
        this.beginIp = beginIp;
        this.endIp = endIp;
        this.country = country;
        this.region = region;
        this.province = province;
        this.city = city;
        this.isp = isp;
    }

    public long getBeginIp() {
        return beginIp;
    }

    public long getEndIp() {
        return endIp;
    }

    public String getCountry() {
        return country;
    }

    public String getRegion() {
        return region;
    }

    public String getProvince() {
        return province;
    }

    public String getCity() {
        return city;
    }

    public String getIsp() {
        return isp;
    }
}
