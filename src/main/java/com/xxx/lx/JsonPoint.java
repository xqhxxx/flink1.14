package com.xxx.lx;


import java.io.Serializable;

public class JsonPoint implements Serializable {
    //    private static final long serialVersionUID = 1L;
    public JsonPoint(){}

    //测点编码
    private String p;
    //测点时间
    private String t;
    //测点值
//    private float v;
    //测点值 出现值为：“” or “true”
    private String v;
    //质量位
    private String q;

    public String getP() {
        return p;
    }

    public void setP(String p) {
        this.p = p;
    }

    public String getT() {
        return t;
    }


    public void setT(String t) {
        this.t = t;
    }

    public String getV() {
        return v;
    }

    public void setV(String v) {
        this.v = v;
    }

    public String getQ() {
        return q;
    }

    public void setQ(String q) {
        this.q = q;
    }

    public JsonPoint(String p, String t, String v, String q) {
        this.p = p;
        this.t = t;
        this.v = v;
        this.q = q;
    }

    public JsonPoint(String p, String t, String v) {
        this.p = p;
        this.t = t;
        this.v = v;
    }

    @Override
    public String toString() {
        return "Points{" +
                "p='" + p + '\'' +
                ", t='" + t + '\'' +
                ", v='" + v + '\'' +
                ", q='" + q + '\'' +
                '}';
    }
}

