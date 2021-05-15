package Util;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SyncData {

    private static Logger log = Logger.getLogger(JdbcScsCheckUtil.class);

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://192.168.13.131:3306/oms?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true";
    private static final String USER = "canal";
    private static final String PSWD = "Canal_1234";
    private static final String SLAVE_DB_URL = "jdbc:mysql://192.168.13.135:3306/seagull?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true";
    private static final String SLAVE_USER = "hsmro";
    private static final String SLAVE_PSWD = "hsMro135@#";

    private static List<String> errorList = new ArrayList<>();


    public Connection getConnection(String url, String user, String pswd, String dirver) throws Exception {
        Connection conn = null;
        try {
            Class.forName(JDBC_DRIVER).newInstance();
            conn = DriverManager.getConnection(url, user, pswd);
            return conn;
        } catch (Exception e) {
            log.error("从库连接 ERROR:", e);
            closeConn(null, conn);
            throw e;
        }
    }

    private static void closeConn(Statement stmtMaster, Connection connMaster) {
        try {
            if (stmtMaster != null) {
                stmtMaster.close();
            }
            if (connMaster != null) {
                connMaster.close();
            }
            log.info(" conn close");
        } catch (SQLException ex) {
            log.error("Can't close stmt/conn because of " + ex);
        }
    }


    public void synch(String tableSchema, String tableName, String targetTableNameAll) {
        Connection connMaster = null;
        Statement stmtMaster = null;

        Connection connSlave = null;
        Statement stmtSlave = null;

        StringBuilder bakInsertSQL = null;

        int i = 0;
        try {
            if (targetTableNameAll == null || targetTableNameAll.length() == 0) {
                targetTableNameAll = tableSchema + "_" + tableName;
            }
            SyncData syncData = new SyncData();
            connMaster = syncData.getConnection(DB_URL, USER, PSWD, JDBC_DRIVER);
            connSlave = syncData.getConnection(SLAVE_DB_URL, SLAVE_USER, SLAVE_PSWD, JDBC_DRIVER);
            stmtMaster = connMaster.createStatement();
            int start = 0;
// 每次同步的记录数
            int currows = 5000;

//        String fieldSql = String.format("select COLUMN_NAME from information_schema.columns where table_name='%s' AND table_schema='%s'", tableName, tableSchema);
//
//        ResultSet rs = stmtMaster.executeQuery(fieldSql);

            for (; ; ) {
                List<String> values = new ArrayList<>();
                String fieldDataSql = String.format("select * from %s.%s limit %s , %s ", tableSchema, tableName, start, currows);

                start = start + currows;
                log.info("SQL:" + fieldDataSql);
                ResultSet rsData = stmtMaster.executeQuery(fieldDataSql);

                final List<Map<String, Object>> dataColumnList = converResultSetToList(rsData);

                if (dataColumnList.size() == 0) {
                    return;
                }

                log.info(targetTableNameAll + " 查询条数：" + dataColumnList.size());

                bakInsertSQL = new StringBuilder();
                bakInsertSQL.append("replace  into " + targetTableNameAll + " (");

                List<String> fields = new ArrayList<String>();
                for (Map<String, Object> stringObjectMap : dataColumnList) {
                    boolean first = true;
                    for (Map.Entry<String, Object> stringObjectEntry : stringObjectMap.entrySet()) {
                        if (first) {
                            first = false;
                        } else {
                            bakInsertSQL.append(",");
                        }
                        bakInsertSQL.append(stringObjectEntry.getKey());
                        fields.add(stringObjectEntry.getKey());
                    }
                    bakInsertSQL.append(") values ");
                    break;
                }

                boolean firstAll = true;

                for (Map<String, Object> stringObjectMap : dataColumnList) {
                    if (firstAll) {
                        firstAll = false;
                        bakInsertSQL.append("(");
                    } else {
                        bakInsertSQL.append(",(");
                    }

                    i++;

                    boolean first = true;
                    for (String field : fields) {

                        Object o = stringObjectMap.get(field);
                        if (first) {
                            first = false;
                        } else {
                            bakInsertSQL.append(",");
                        }
                        bakInsertSQL.append(" ? ");
                        if (o != null) {
                            // bakInsertSQL.append( "'"+o.toString().replace("'", "\\'")+ "'");
                            values.add(o.toString());
                        } else {
                            values.add(null);
                            // bakInsertSQL.append(o);
                        }
                    }
                    bakInsertSQL.append(")");
                }

                if (stmtSlave == null) {
                    stmtSlave = connSlave.createStatement();
                }
                 log.info(targetTableNameAll + "开始新增SQL:" + bakInsertSQL.toString());
                log.info(targetTableNameAll + "开始新增SQL:");


                stmtSlave = connSlave.prepareStatement(bakInsertSQL.toString());
                PreparedStatement ps = (PreparedStatement) stmtSlave;
                int j = 1;
                for (String value : values) {
                    ps.setString(j, value);
                    j++;
                }

                ps.executeUpdate();
                ps.close();
                log.info(targetTableNameAll + "完成新增SQL：" + i);
            }
        } catch (Exception e) {
            log.info(targetTableNameAll + "同步异常：", e);
            this.addLog(bakInsertSQL.toString());
            errorList.add(targetTableNameAll + "位置" + i);
        } finally {
            closeConn(stmtMaster, connMaster);
            closeConn(stmtSlave, connSlave);
        }

    }


    private static List<Map<String, Object>> converResultSetToList(ResultSet resultSet) throws SQLException {
        if (null == resultSet) {
            return null;
        }
        List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
        ResultSetMetaData rsmd = resultSet.getMetaData();
        while (resultSet.next()) {
            Map<String, Object> rowData = new LinkedHashMap<String, Object>(16);
            for (int i = 0, columnCount = rsmd.getColumnCount(); i < columnCount; i++) {
                rowData.put(rsmd.getColumnName(i + 1), resultSet.getString(i + 1));
            }
            data.add(rowData);
        }
        return data;
    }


    public static void main(String[] args) throws Exception {
        ExecutorService pool = new ThreadPoolExecutor(5, 5, 20, TimeUnit.SECONDS, new LinkedBlockingQueue<>(500),
                new ThreadPoolExecutor.CallerRunsPolicy());


        SyncData syncData = new SyncData();
        pool.execute(() -> syncData.synch("wms", "wh_inbound", null));
        pool.execute(() -> syncData.synch("wms", "wh_inbound_detail", null));
        pool.execute(() -> syncData.synch("wms", "outbound_detail_info", null));
        pool.execute(() -> syncData.synch("wms", "outbound_info", null));
        pool.execute(() -> syncData.synch("report", "sales_send_receive_summary", null));
        pool.execute(() -> syncData.synch("report", "sales_profit_breakdown_info", null));
        pool.execute(() -> syncData.synch("report", "outbound_reconciliation_detail", null));
        pool.execute(() -> syncData.synch("report", "customer_sales_profit_breakdown_info", null));
        pool.execute(() -> syncData.synch("finance", "sale_account", null));
        pool.execute(() -> syncData.synch("finance", "sale_account_detail", null));
        pool.execute(() -> syncData.synch("finance", "supply_account", null));
        pool.execute(() -> syncData.synch("finance", "supply_account_detail", null));
        pool.execute(() -> syncData.synch("sku", "sku", null));
        pool.execute(() -> syncData.synch("oms", "running_param", null));
        pool.execute(() -> syncData.synch("bdm", "customer", null));

        pool.shutdown();
        for (; ; ) {

            Thread.sleep(10000);
            log.info("错误信息" + JSON.toJSONString(errorList));
            if (pool.isTerminated()) {
                break;
            }
        }
    }

    public void addLog(String str) {
        try {
            FileOutputStream fos = new FileOutputStream("log.txt", true);
//true表示在文件末尾追加
            fos.write(str.getBytes());
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
