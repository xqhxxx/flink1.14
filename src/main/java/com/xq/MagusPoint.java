package com.xq;

import java.sql.Timestamp;

/**
 * @author xqh
 * @date 2022/4/15
 * @apiNote
 */
public class MagusPoint {
    public String id;
    public String gn;
    public Long tm;
    public String av;
    public String ds;

    public MagusPoint() {
    }

    public MagusPoint(String id, String gn, Long tm, String av, String ds) {
        this.id = id;
        this.gn = gn;
        this.tm = tm;
        this.av = av;
        this.ds = ds;
    }

    @Override
    public String toString() {
        return "MagusPoint{" +
                "id='" + id + '\'' +
                ", gn='" + gn + '\'' +
                ", tm=" + new Timestamp(tm) +
                ", av='" + av + '\'' +
                ", ds='" + ds + '\'' +
                '}';
    }
}

