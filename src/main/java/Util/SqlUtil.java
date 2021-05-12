package Util;

import java.text.MessageFormat;

public class SqlUtil {


    public static String fitCheckSum(String allName) {
        if (allName == null) {
            throw new RuntimeException("内容不能为空");
        }
        return MessageFormat.format("checksum table {0}", allName);
    }
}
