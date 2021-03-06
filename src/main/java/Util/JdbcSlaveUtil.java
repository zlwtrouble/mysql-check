package Util;

import org.apache.log4j.Logger;

import java.sql.*;

public class JdbcSlaveUtil {


    private static Logger log = Logger.getLogger(JdbcSlaveUtil.class);


    //从库配置，有账号查询权限即可

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";



    Connection conn = null;

    Statement stmt = null;

    PreparedStatement preparedStatement = null;

  private static   String url="";
    private static   String user="";
    private static  String pswd="";


    public void setConnection(String url1,String user1,String pswd1) {
        url=url1;
        user=user1;
        pswd=pswd1;;
    }

    public void getConnection() {
        try {
            Class.forName(JDBC_DRIVER).newInstance();
            conn = DriverManager.getConnection(url, user, pswd);
        } catch (Exception e) {
            log.error("从库连接 ERROR:", e);
            closeConn();
        }
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
            if(preparedStatement!=null){
                preparedStatement.close();
            }
            if(conn!=null){
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
