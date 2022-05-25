/*
package com.xq;

import com.magus.jdbc.net.OPSubscribe;
import com.magus.jdbc.net.SubscribeResultSet;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

*/
/**
 * @author xqh
 * @date 2022/4/15
 * @apiNote
 *//*

public class MagusSource implements SourceFunction<StringBuffer> {
    private Boolean running = true;
    static StringBuffer buffer = new StringBuffer();

    @Override
    public void run(SourceContext<StringBuffer> ctx) throws Exception {
        Class.forName("com.magus.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:openplant://10.191.177.6:8200/RTDB", "sis", "openplant");
        if (conn.getAutoCommit()) {
            conn.setAutoCommit(false);
        }
        String table_name = "Realtime";
        ArrayList<Integer> ids = new ArrayList<>();
//                ids.add(ProUtils.getIntValue(Configs.OP_NODE_ID));//4 三峡
        ids.add(4);//535559
        OPSubscribe subscribe = new OPSubscribe(conn, table_name, ids, new MagusRT());
        Thread.sleep(1000 * 2);
        //conn.rollback();
        conn.close();

        //new MagusRT();
        while (running) {
            if (buffer.length() > 0) {
                ctx.collect(buffer);
                buffer.delete(0,buffer.length());
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static class MagusRT implements SubscribeResultSet {
        @Override
        public void onResponse(ResultSet result) throws SQLException {
            //处理订阅到的数据结果，
            StringBuffer sb = new StringBuffer();
            while (result.next()) {
                int ID = result.getInt(1);
                String GN = result.getString(2);
                Long TM = result.getDate(3).getTime();
                String DS = result.getString(4);
                String AV = result.getString(5);
                sb.append(ID).append(",")
                        .append(GN).append(",")
                        .append(TM).append(",")
                        .append(DS).append(",")
                        .append(AV).append("\n");

                //0422
                //if ("W3.SX.02162MEA11AP001-MEA25ADI1RUN".equals(GN)){
//                    System.out.println(sb);
                //}
            }
            buffer = sb;
        }
    }

}
*/
