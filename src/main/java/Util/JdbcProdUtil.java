package Util;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JdbcProdUtil {


    /**
     * 主库配置，有账号查询权限即可
     */
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://192.168.13.129:3306/oms?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true&useSSL=false";
    private static final String USER = "selectUser";
    private static final String PSWD = "selectMro#";


    /**
     * 从库
     */
    private static final String SLAVE_DB_URL = "jdbc:mysql://192.168.13.131:3306/oms?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true&useSSL=false";
    private static final String SLAVE_USER = "selectUser";
    private static final String SLAVE_PSWD = "selectMro#";

    /**
     * 配完以后可以先测试时 设为true，只检测一张表即结束
     */
    private static boolean hasTest = false;


    private static Logger log = Logger.getLogger(JdbcProdUtil.class);


    public static void main(String[] args) {
        JdbcProdUtil jdbcUtil = new JdbcProdUtil();
        jdbcUtil.getConnection();
    }

    public void getConnection() {
        JdbcSlaveUtil jdbcSlaveUtil = new JdbcSlaveUtil();
        jdbcSlaveUtil.setConnection(SLAVE_DB_URL, SLAVE_USER, SLAVE_PSWD);


        Connection conn = null;
        Statement stmt = null;
        PreparedStatement preparedStatement = null;
        int errorCount = 0;
        try {
            Class.forName(JDBC_DRIVER).newInstance();
            conn = DriverManager.getConnection(DB_URL, USER, PSWD);

            stmt = conn.createStatement();
            String sql = "select TABLE_SCHEMA  schemaName,table_name tableName from information_schema.tables";
            ResultSet rs = stmt.executeQuery(sql);

            int index = 0;
            List<String> allNameList = new ArrayList<String>();
            while (rs.next()) {
                index++;

                String schemaName = rs.getString("schemaName");
                String tableName = rs.getString("tableName");

                boolean noNeed = schemaName.startsWith("information_schema")
                        || schemaName.startsWith("performance_schema")
                        || schemaName.startsWith("mysql")
                        || schemaName.startsWith("xxl")
                        || schemaName.startsWith("sys")
                        || tableName.startsWith("distance");
                if (!noNeed) {
                    String allName = schemaName + "." + tableName;
                    allNameList.add(allName);
                }
            }
            conn.close();
            for (String allName : allNameList) {
                conn = DriverManager.getConnection(DB_URL, USER, PSWD);
                preparedStatement = conn.prepareStatement(
                        "checksum table " + allName);
                preparedStatement.setQueryTimeout(3600);
                ResultSet resultSet = preparedStatement.executeQuery();


                while (resultSet.next()) {
                    String resultSlave = resultSet.getString(1) + " " + resultSet.getString(2);

                    String ck = jdbcSlaveUtil.getRresultStr(allName);
                    if (!resultSlave.equals(ck)) {
                        errorCount++;
                        String s = "ERROR:" + resultSlave + ",从库：" + ck;
                        log.error(s);
                        FileUtil.writer(new Date().toString()+s);
                    } else {
                        log.info(" pass table name:" + allName);
                    }
                }
                preparedStatement.close();
                conn.close();
                //
                if (hasTest) {
                    break;
                }


            }

            log.info("错误的表数量" + errorCount);
        } catch (Exception e) {
            log.error("DB/SQL ERROR:", e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (conn != null) {
                    conn.close();
                }

                log.info("主库 conn close");
            } catch (SQLException e) {
                log.info("Can't close stmt/conn because of ", e);
            }
            jdbcSlaveUtil.closeConn();
        }
    }
}
