package com.aliyun.log.etl_function;

public class IpData {

    private String country;
    private String province;
    private String city;
    private String isp;

    public IpData(String country, String province, String city, String isp) {
        this.country = country;
        this.province = province;
        this.city = city;
        this.isp = isp;
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
