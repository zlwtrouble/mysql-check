package count;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.*;
import java.util.*;

public class mmm {


    private static Logger log = Logger.getLogger(mmm.class);

    String startSql;
    String fileName = "";

    String URL = "jdbc:mysql://192.168.13.129:3306/report?useUnicode=true&characterEncoding=UTF8&zeroDateTimeBehavior=convertToNull";
    String USER = "selectUser";
    String PASSWORD = "selectMro#";

    Map<String, String> replace = new LinkedHashMap<>();

    public static void main(String[] args) {
//        {
//            mmm mmm = new mmm();
//            mmm.fileName = "供应链";
//            mmm.startSql = "\n" +
//                    "SELECT\n" +
//                    "concat(\ttt1.TABLE_SCHEMA,'.',\ttt1.TABLE_NAME) a1,\n" +
//                    "\ttt1.TABLE_COMMENT a2,\n" +
//                    "\ttt1.TABLE_ROWS a3,\n" +
//                    "\ttt2.ccc a4,\n" +
//                    "IF\n" +
//                    "\t( tt1.UPDATE_TIME IS NULL, tt1.CREATE_TIME, UPDATE_TIME ) a5\n" +
//                    "\t\n" +
//                    "FROM\n" +
//                    "\tinformation_schema.TABLES tt1\n" +
//                    "\tLEFT JOIN ( SELECT table_schema, table_name, count( 1 ) ccc FROM information_schema.COLUMNS GROUP BY table_schema, table_name ) tt2 ON tt1.TABLE_SCHEMA = tt2.table_schema \n" +
//                    "\tAND tt1.TABLE_NAME = tt2.table_name \n" +
//                    "WHERE\n" +
//                    "\ttt1.table_schema IN (\n" +
//                    "\t\t'bdm',\n" +
//                    "\t\t'contract',\n" +
//                    "\t\t'finance',\n" +
//                    "\t\t'sap',\n" +
//                    "\t\t'admin',\n" +
//                    "\t\t'mall',\n" +
//                    "\t\t'sku',\n" +
//                    "\t\t'wms',\n" +
//                    "\t\t'oms',\n" +
//                    "\t\t'report',\n" +
//                    "\t\t'basics',\n" +
//                    "\t\t'bid'\n" +
//                    "\t) ";
//            mmm.URL = "jdbc:mysql://192.168.13.129:3306/report?useUnicode=true&characterEncoding=UTF8&zeroDateTimeBehavior=convertToNull";
//            mmm.USER = "selectUser";
//            mmm.PASSWORD = "selectMro#";
//            mmm.jdbc();
//        }


//        {
//            mmm mmm = new mmm();
//            mmm.fileName = "仓配";
//            mmm.startSql = "\n" +
//                    "SELECT\n" +
//                    "concat(\ttt1.TABLE_SCHEMA,'.',\ttt1.TABLE_NAME) a1,\n" +
//                    "\ttt1.TABLE_COMMENT a2,\n" +
//                    "\ttt1.TABLE_ROWS a3,\n" +
//                    "\ttt2.ccc a4,\n" +
//                    "IF\n" +
//                    "\t( tt1.UPDATE_TIME IS NULL, tt1.CREATE_TIME, UPDATE_TIME ) a5\n" +
//                    "\t\n" +
//                    "FROM\n" +
//                    "\tinformation_schema.TABLES tt1\n" +
//                    "\tLEFT JOIN ( SELECT table_schema, table_name, count( 1 ) ccc FROM information_schema.COLUMNS GROUP BY table_schema, table_name ) tt2 ON tt1.TABLE_SCHEMA = tt2.table_schema \n" +
//                    "\tAND tt1.TABLE_NAME = tt2.table_name \n" +
//                    "WHERE\n" +
//                    "\ttt1.table_schema IN (\n" +
//                    "\t\t'admin',\n" +
//                    "\t\t'basics',\n" +
//                    "\t\t'bms',\n" +
//                    "\t\t'oms',\n" +
//                    "\t\t'portal',\n" +
//                    "\t\t'tms',\n" +
//                    "\t\t'wms'\n" +
//                    "\t) ";
//            mmm.URL = "jdbc:mysql://192.168.169.192:3306/oms?useUnicode=true&characterEncoding=UTF8&zeroDateTimeBehavior=convertToNull";
//            mmm.USER = "selectUser";
//            mmm.PASSWORD = "selectUser@607175";
//            mmm.jdbc();
//        }


        {
            mmm mmm = new mmm();
            mmm.fileName = "tms";
            mmm.startSql = "\n" +
                    "SELECT\n" +
                    "concat(\ttt1.TABLE_SCHEMA,'.',\ttt1.TABLE_NAME) a1,\n" +
                    "\ttt1.TABLE_COMMENT a2,\n" +
                    "\ttt1.TABLE_ROWS a3,\n" +
                    "\ttt2.ccc a4,\n" +
                    "IF\n" +
                    "\t( tt1.UPDATE_TIME IS NULL, tt1.CREATE_TIME, UPDATE_TIME ) a5\n" +
                    "\t\n" +
                    "FROM\n" +
                    "\tinformation_schema.TABLES tt1\n" +
                    "\tLEFT JOIN ( SELECT table_schema, table_name, count( 1 ) ccc FROM information_schema.COLUMNS GROUP BY table_schema, table_name ) tt2 ON tt1.TABLE_SCHEMA = tt2.table_schema \n" +
                    "\tAND tt1.TABLE_NAME = tt2.table_name \n" +
                    "WHERE\n" +
                    "\ttt1.table_schema IN ('tms_prod') ";
            mmm.URL = "jdbc:mysql://192.168.13.252:3306/tms_prod?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true";
            mmm.USER = "selectAll";
            mmm.PASSWORD = "qetu@135v";
            mmm.jdbc();
        }
    }

    public void jdbc() {
        List<Map<String, String>> result = new ArrayList<>();
        PreparedStatement statement = null;
        Connection conn = null;
        ResultSet rs = null;
        String sql = null;

        // 1.加载驱动程序
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // 2.获得数据库链接
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            // 3.通过数据库的连接操作数据库，实现增删改查（使用Statement类）
            String name = "张三";
            //预编译
            sql = startSql;


            {
                statement = conn.prepareStatement(sql);
                rs = statement.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();
                int count = rsmd.getColumnCount();
                while (rs.next()) {
                    LinkedHashMap<String, String> columMap = new LinkedHashMap<String, String>();
                    result.add(columMap);
                    for (int k = 0; k < count; k++) {
                        String columLable = rsmd.getColumnLabel(k + 1); // 字段名称 number,username,password
                        Object columValue = rs.getObject(columLable); // 字段值 1001 admin 123456
                        columMap.put(columLable, String.valueOf(columValue));
                    }
                }

                rs.close();
                statement.close();
            }

            for (Map<String, String> stringStringMap : result) {
                {
                    sql = String.format("select count(*) from %s", stringStringMap.get("a1"));
                    log.info(sql);
                    statement = conn.prepareStatement(sql);
                    rs = statement.executeQuery();
                    while (rs.next()) {
                        String string = rs.getString(1);
                        System.out.println(string);
                        if (string == null || string.length() == 0) {
                            stringStringMap.put("a3", "0");
                        } else {
                            try {
                                Integer integer = Integer.valueOf(string);
                                stringStringMap.put("a3", string);
                            } catch (Exception e) {
                                log.error("转换异常", e);
                                stringStringMap.put("a4", "错误");
                            }
                        }
                    }
                    rs.close();
                    statement.close();
                }
//                break;
            }


            ;
        } catch (Exception e) {
            log.error(sql);
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        replace.put(",", "，");
        for (Map<String, String> stringStringMap : result) {
            String a1 = stringStringMap.get("a1");
            if (a1 != null) {
                for (Map.Entry<String, String> stringStringEntry : replace.entrySet()) {
                    a1 = a1.replaceAll(stringStringEntry.getKey(), stringStringEntry.getValue());
                }
            }
        }


        if (result.size() == 0) {
            log.info("没有数据");
            return;
        }

        List<List<String>> dataList = new ArrayList<>();
        for (Map<String, String> stringStringMap : result) {
            dataList.add(new ArrayList<>(stringStringMap.values()));
        }


        writeCsv(fileName, new ArrayList<>(result.get(0).keySet()), dataList);
        log.info("完成条数：" + result.size());
    }


    public static void writeCsv(String fileName, List<String> titleName, List<List<String>> dataList) {
        String CSV_COLUMN_SEPARATOR = ",";
        String CSV_ROW_SEPARATOR = System.lineSeparator();
        File file = new File("D:" + File.separator + "csv" + File.separator + fileName + System.currentTimeMillis() + ".csv");
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        Writer out = null;
        try {
            // 保证线程安全
            StringBuffer buf = new StringBuffer();
            out = new FileWriter(file);

            // 组装表头
            for (String title : titleName) {
                buf.append(title).append(CSV_COLUMN_SEPARATOR);
            }
            buf.append(CSV_ROW_SEPARATOR);

            //组装行数据
            for (List<String> row : dataList) {
                for (int columnIndex = 0; columnIndex < dataList.get(0).size(); columnIndex++) {
                    Object o = row.get(columnIndex);
                    String data = o == null ? "" : o.toString();
                    {
                        //组装数据
                        buf.append(data).append(CSV_COLUMN_SEPARATOR);
                    }
                }
                buf.append(CSV_ROW_SEPARATOR);
            }

            //输出
            out.write(buf.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                try {
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    log.error("导出写Csv异常");
                }
            }
        }
    }

}
