package org.musicrank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtils {
    private static Configuration conf;

    private static Connection connection;

    private static Admin admin;

    public static void init(){
        try {
            conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection();
            admin = connection.getAdmin();

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    static {
        init();
    }

    public static void close(){
        try {
            if (admin!=null){
                admin.close();
            }
            if (connection!=null){
                connection.close();
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static TableName getTabelName(String tablename){
        return TableName.valueOf(tablename);
    }

    public static Configuration getConf() {
        return conf;
    }

    public static Connection getConnection() {
        return connection;
    }

    public static Admin getAdmin() {
        return admin;
    }

    public  static void createTable(String tableName, String familyName, boolean isDrop){
        try {
            TableName tn = getTabelName(tableName);
            if (admin.tableExists(tn)){
                //先禁用后删除
                admin.disableTable(tn);
                admin.deleteTable(tn);
                //提示
                System.out.println("表存在，已删除原表");
            } else if (admin.tableExists(tn)&&!isDrop) {
                return;
            }
            byte[] fm = Bytes.toBytes(familyName);

            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(fm);
            //创建列簇结构对象
            ColumnFamilyDescriptor cf = cfdb.build();

            //同上步骤先创建构造器后 通过构造器创建对象
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);

            //将列簇与表关联
            tdb.setColumnFamily(cf);

            TableDescriptor t = tdb.build();


            //创建表
            admin.createTable(t);
            //提示
            System.out.println("表："+tableName+"创建成功");

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
