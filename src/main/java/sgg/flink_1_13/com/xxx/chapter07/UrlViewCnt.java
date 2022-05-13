package sgg.flink_1_13.com.xxx.chapter07;

/**
 * @author xqh
 * @date 2022/4/22
 * @apiNote
 */
public class UrlViewCnt {

    public String url;
    public Long count;
    public Long windowStart;
    public Long windowEnd;


    public UrlViewCnt() {
    }


    public UrlViewCnt(String url, Long count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlViewCnt{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
