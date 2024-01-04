package org.musicrank;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//显示歌曲播放排行榜
public class Step4 {
    public static void run() {
        try {
            String tableName = "rank";
            //获取表
            Table rankTable = HbaseUtils.getConnection().getTable(HbaseUtils.getTabelName(tableName));
            //定义Scan用于读取Hbase表中数据
            Scan scan = new Scan();
            //读取操作
            ResultScanner rows = rankTable.getScanner(scan);
            //定义列……
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name_list");
            scan.addColumn(fm,c1);
            //循环读取数据
            for (Result r :rows){
                //读取行键
                String rowkey = Bytes.toString(r.getRow());
                //读取歌曲列表
                String name_list = "";
                if (r.containsColumn(fm,c1)){
                    name_list = Bytes.toString(r.getValue(fm,c1));
                }
                System.out.println(rowkey.split("_")[1]+"\t"+name_list);
            }


        }catch (Exception e){
            e.printStackTrace();
        }


    }

}
