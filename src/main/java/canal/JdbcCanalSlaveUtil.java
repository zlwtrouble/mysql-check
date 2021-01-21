package canal;

import org.apache.log4j.Logger;

import java.sql.*;

public class JdbcCanalSlaveUtil {


    private static Logger log = Logger.getLogger(JdbcCanalSlaveUtil.class);


    //从库配置，有账号查询权限即可

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";



    Connection conn = null;

    Statement stmt = null;

    PreparedStatement preparedStatement = null;


    public void getConnection(String url,String user,String pswd) {
        try {
            Class.forName(JDBC_DRIVER).newInstance();
            conn = DriverManager.getConnection(url, user, pswd);
        } catch (Exception e) {
            log.error("从库连接 ERROR:", e);
            closeConn();
        }
    }

    public String getRresultStr(String allName) throws SQLException {
        preparedStatement = conn.prepareStatement("checksum table " + allName);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            String resultStr = allName + " " + resultSet.getString(2);
            return resultStr;
        }
        return null;
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
