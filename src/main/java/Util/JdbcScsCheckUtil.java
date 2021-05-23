package Util;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.*;

public class JdbcScsCheckUtil {

    static JdbcConnUtil.DataSourceConfig dataSourceConfigMaster = new JdbcConnUtil.DataSourceConfig("jdbc:mysql://192.168.13.129:3306/oms?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true&useSSL=false",
            "selectUser",
            "selectMro#",
            "com.mysql.jdbc.Driver");


    static JdbcConnUtil.DataSourceConfig dataSourceConfigSlave = new JdbcConnUtil.DataSourceConfig("jdbc:mysql://192.168.13.131:3306/oms?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true&useSSL=false",
            "selectUser",
            "selectMro#",
            "com.mysql.jdbc.Driver");


    /**
     * 配完以后可以先测试时 设为true，只检测一张表即结束
     */
    private static boolean hasTest = false;


    private static Logger log = Logger.getLogger(JdbcScsCheckUtil.class);


    public static void main(String[] args) {
        JdbcScsCheckUtil jdbcUtil = new JdbcScsCheckUtil();
        jdbcUtil.getConnection();
    }

    public void getConnection() {
        long start = System.currentTimeMillis();
        ExecutorService threadPool = Executors.newFixedThreadPool(16);
        List<String> errorList = Collections.synchronizedList(new ArrayList<>());
        String sql = "select TABLE_SCHEMA  schemaName,table_name tableName from information_schema.tables";
        List<LinkedHashMap<String, String>> linkedHashMaps = JdbcConnUtil.executeSql(dataSourceConfigMaster, sql);

        int index = 0;
        List<String> allNameList = new ArrayList<String>();
        for (LinkedHashMap<String, String> row : linkedHashMaps) {
            index++;

            String schemaName = row.get("schemaName");
            String tableName = row.get("tableName");

            boolean noNeed = schemaName.startsWith("information_schema")
                    || schemaName.startsWith("performance_schema")
                    || schemaName.startsWith("mysql")
                    || schemaName.startsWith("sys");
            if (!noNeed) {
                String allName = "`" + schemaName + "`.`" + tableName + "`";
                allNameList.add(allName);
            }
        }
        for (String allName : allNameList) {
            index++;
            threadPool.execute(() -> {
                checkTable(errorList, allName);
            });

            if (hasTest) {
                break;
            }


        }


        threadPool.shutdown();
        while (true) {
            if (threadPool.isTerminated()) {
                log.info("结束了！共计：" + index);
                break;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.info("错误的表数量" + errorList.size());
        errorList.forEach(o -> {
            log.info(o);
        });
    }

    private void checkTable(List<String> errorList, String allName) {
        List<LinkedHashMap<String, String>> resultSet = JdbcConnUtil.executeSql(dataSourceConfigMaster, "checksum table " + allName);
        String m = "";
        String s = "";
        for (LinkedHashMap<String, String> row : resultSet) {
            m = row.get("Table") + " " + row.get("Checksum");
        }

        List<LinkedHashMap<String, String>> jdbcSlave = JdbcConnUtil.executeSql(dataSourceConfigSlave, "checksum table " + allName);
        for (LinkedHashMap<String, String> row : jdbcSlave) {
            s = row.get("Table") + " " + row.get("Checksum");
        }
        if (!m.equals(s)) {
            String errorMsg = " ERROR:" + m + ",从库：" + s;
            errorList.add(errorMsg);
            log.error(errorMsg);
        } else {
            log.info(" pass table name:" + allName);
        }
    }

}
