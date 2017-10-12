package com.aliyun.log.etl_function;

public class IpData {

    private long beginIp;
    private long endIp;
    private String country;
    private String province;
    private String city;
    private String isp;

    public IpData(long beginIp, long endIp, String country, String province, String city, String isp) {
        this.beginIp = beginIp;
        this.endIp = endIp;
        this.country = country;
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

    public String getProvince() { return province; }

    public String getCity() {
        return city;
    }

    public String getIsp() {
        return isp;
    }
}
