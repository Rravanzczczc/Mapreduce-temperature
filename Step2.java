package org.musicrank;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
//统计歌曲播放次数

public class Step2 {
    private static class PlayCountMapper extends TableMapper<Text, IntWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //读取HBASE中的一行
            //定义列簇，列名
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");

            if(value.containsColumn(fm,c1)) {
                //提取歌曲名称
                String name = Bytes.toString(value.getValue(fm, c1));
                //输出
                context.write(new Text(name), new IntWritable(1));
            }
        }
    }
    private static class PlayCountReducer extends TableReducer<Text, IntWritable, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            //痛击歌曲播放次数
            int playCount = 0;
            for (IntWritable v : values){
                playCount += v.get();
            }

            //定义行键，列簇，列名，值
            byte[] rk = Bytes.toBytes(key.toString());
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("play_count");
            byte[] v1 = Bytes.toBytes(String.valueOf(playCount));
            //构建Put对象
            Put put = new Put(rk);
            put.addColumn(fm,c1,v1);
            //输出
            context.write(NullWritable.get(),put);
        }
    }

    public static void run() {
        try {
            String tableName = "play_count";
            //创建表
            HbaseUtils.createTable(tableName,"info",true);
            //创建jO
            Job job = Job.getInstance(HbaseUtils.getConf(),"play_count");
            //定义Scan用于读取Hbase表中数据
            Scan scan = new Scan();
            //定义列……
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            scan.addColumn(fm,c1);
            //设置mapper
            TableMapReduceUtil.initTableMapperJob("play_history", scan, PlayCountMapper.class, Text.class, IntWritable.class, job);

            //设置reducer
            TableMapReduceUtil.initTableReducerJob(tableName, PlayCountReducer.class, job);

            //运行
            boolean flag = job.waitForCompletion(true);

            if (flag){
                System.out.println("歌曲播放次数统计完成");
            }


        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
