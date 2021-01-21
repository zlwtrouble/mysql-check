package canal;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcCanalCheck {





    /**
     * 主库配置，有账号查询权限即可
     */
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://192.168.13.131:3306/oms?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true";
    private static final String USER = "canal";
    private static final String PSWD = "Canal_1234";


    /**
     * 从库
     */
    private static final String SLAVE_DB_URL = "jdbc:mysql://192.168.13.135:3306/seagull?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true";
    private static final String SLAVE_USER = "hsmro";
    private static final String SLAVE_PSWD = "hsMro135@#";

    /**
     * 配完以后可以先测试时 设为true，只检测一张表即结束
     */
    private static  boolean hasTest = false;



    private static Logger log = Logger.getLogger(JdbcCanalCheck.class);


    public static void main(String[] args) {
        JdbcCanalCheck jdbcUtil = new JdbcCanalCheck();
        jdbcUtil.checkError();
    }

    public void checkError() {
        JdbcCanalSlaveUtil jdbcSlaveUtil = new JdbcCanalSlaveUtil();
        jdbcSlaveUtil.getConnection(SLAVE_DB_URL,SLAVE_USER,SLAVE_PSWD);


        Connection conn = null;
        Statement stmt = null;
        PreparedStatement preparedStatement = null;
        int errorCount=0;
        try {
            Class.forName(JDBC_DRIVER).newInstance();
            conn = DriverManager.getConnection(DB_URL, USER, PSWD);

            stmt = conn.createStatement();

            int index = 0;
            List<String> allNameList = new ArrayList<String>();
            allNameList.add("oms.running_param");
            allNameList.add("wms.outbound_info");
            allNameList.add("wms.outbound_detail_info");
            allNameList.add("wms.wh_inbound");
            allNameList.add("wms.wh_inbound_detail");
            allNameList.add("finance.sale_account_detail");
            allNameList.add("finance.sale_account");
            allNameList.add("finance.supply_account");
            allNameList.add("finance.supply_account_detail");
            allNameList.add("report.sales_send_receive_summary");
            allNameList.add("report.outbound_reconciliation_detail");
            allNameList.add("report.customer_sales_profit_breakdown_info");
            allNameList.add("report.sales_profit_breakdown_info");
            allNameList.add("sku.sku");
            allNameList.add("bdm.customer");
            for (String allName : allNameList) {
                preparedStatement = conn.prepareStatement(
                        "checksum table " + allName);
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    String substring = allName.replaceFirst("\\.","_");
                    String resultSlave = substring + " " + resultSet.getString(2);

                    String ck = jdbcSlaveUtil.getRresultStr(substring);
                    if (!resultSlave.equals(ck)) {
                        errorCount++;
                        log.error(" ERROR:" + resultSlave + ",从库：" + ck);
                    }else{
                        log.info(" pass table name:" + allName);
                    }
                }
                //
                if(hasTest){
                    break;
                }


            }

            log.info("错误的表数量"+errorCount);
        } catch ( Exception e) {
            log.error("DB/SQL ERROR:" , e);
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
                log.info("Can't close stmt/conn because of " ,e);
            }
            jdbcSlaveUtil.closeConn();
        }
    }
}
