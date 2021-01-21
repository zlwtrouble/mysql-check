package Util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class FileUtil {


    public static void writer(String str) {
        FileWriter fw = null;
        try {
            try {
//如果文件存在，则追加内容；如果文件不存在，则创建文件
                File f = new File("\\jdbc.log");
                fw = new FileWriter(f, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            PrintWriter pw = new PrintWriter(fw);
            pw.println(str);
            pw.flush();

            fw.flush();
            pw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fw!=null){
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
