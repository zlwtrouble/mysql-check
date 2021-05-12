package Util;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JdbcConnect {


    private JdbcConnect() {

    }


    public JdbcConnect(String url, String user, String pswd, String driver) {
        url = url;
        user = user;
        pswd = pswd;

        if (driver != null) {
            driver = driver;
            ;
        }
    }

    private String driver = "com.mysql.jdbc.Driver";
    private static String url = "";
    private static String user = "";
    private static String pswd = "";


    private static Logger log = Logger.getLogger(JdbcConnect.class);


    //从库配置，有账号查询权限即可


    Connection conn = null;

    Statement stmt = null;

    PreparedStatement preparedStatement = null;


    public Connection getConnection() {
        if (conn != null) {
            return conn;
        }

        try {
            Class.forName(driver).newInstance();
            conn = DriverManager.getConnection(url, user, pswd);
        } catch (Exception e) {
            log.error("从库连接 ERROR:", e);
            closeConn();
        }
        return conn;
    }


    public synchronized List<Map<String, String>> executeSql(String sql) {
        List<Map<String, String>> result = new ArrayList<>();
        try {
            this.getConnection();
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setQueryTimeout(3600);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String resultStr = resultSet.getString(1) + " " + resultSet.getString(2);
                return null;
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

        return result;
    }

    public String getRresultStr(String allName) throws SQLException {
        try {
            this.getConnection();
            preparedStatement = conn.prepareStatement("checksum table " + allName);
            preparedStatement.setQueryTimeout(3600);
            ResultSet resultSet = preparedStatement.executeQuery();


            while (resultSet.next()) {
                String resultStr = resultSet.getString(1) + " " + resultSet.getString(2);
                return resultStr;
            }
            return null;
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (conn != null) {
                conn.close();
            }

        }
    }


    public void closeConn() {
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
            log.info("从库 conn close");
        } catch (SQLException e) {
            log.error("Can't close stmt/conn because of " + e);
        }
    }
}
