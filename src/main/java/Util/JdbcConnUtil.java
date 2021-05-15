package Util;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;


public class JdbcConnUtil {

    private static Logger log = Logger.getLogger(JdbcScsCheckUtil.class);

    public static void main(String[] args) {

        DataSourceConfig dataSourceConfig = new DataSourceConfig("jdbc:mysql://192.168.13.131:3306/oms?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true",
                "selectUser",
                "selectMro#",
                "com.mysql.jdbc.Driver");
        List<LinkedHashMap<String, String>> linkedHashMaps = JdbcConnUtil.executeSql(dataSourceConfig, "SELECT * FROM `basics`.`sys_log_bo` order by id desc LIMIT 0,100");
        if (linkedHashMaps == null) {
            log.error("发生错误");
        }
        int i = 0;
        for (LinkedHashMap<String, String> linkedHashMap : linkedHashMaps) {
            i++;
            log.info("row " + i + "data：" + JSON.toJSONString(linkedHashMap));
        }
    }


    public static List<LinkedHashMap<String, String>> executeSql(DataSourceConfig config, String sql) {
        log.debug("查询条件：" + sql);
        List<LinkedHashMap<String, String>> restult = new ArrayList<>();
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(config.getDirver()).newInstance();
            conn = DriverManager.getConnection(config.getUrl(), config.getUserName(), config.getPassword());

            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            // 处理查询结果集
            ResultSetMetaData rsmd = rs.getMetaData();
            // 返回字段个数
            int count = rsmd.getColumnCount();
            while (rs.next()) {
                LinkedHashMap<String, String> columMap = new LinkedHashMap<String, String>();
                restult.add(columMap);
                for (int k = 0; k < count; k++) {
                    String columLable = rsmd.getColumnLabel(k + 1); // 字段名称 number,username,password
                    Object columValue = rs.getObject(columLable); // 字段值 1001 admin 123456
                    columMap.put(columLable, String.valueOf(columValue));
                }

            }

        } catch (Exception e) {
            log.error("从库连接 ERROR:", e);
            return null;
        } finally {
            closeConn(stmt, conn);
        }
        return restult;
    }

    private static void closeConn(Statement stmtMaster, Connection connection) {
        try {
            if (stmtMaster != null) {
                try {
                    stmtMaster.close();
                } catch (Exception e) {
                    log.error(" stmtMaster.close() error ", e);
                }

            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    log.error("connMaster.close()  error", e);
                }

            }
        } catch (Exception ex) {
            log.error("Can't close stmt/conn because of " + ex);
        }
    }

    public static class DataSourceConfig {
        private String url;
        private String userName;
        private String password;
        private String dirver;


        public DataSourceConfig(String url, String userName, String password, String dirver) {
            this.url = url;
            this.userName = userName;
            this.password = password;
            this.dirver = dirver;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDirver() {
            return dirver;
        }

        public void setDirver(String dirver) {
            this.dirver = dirver;
        }
    }


}
