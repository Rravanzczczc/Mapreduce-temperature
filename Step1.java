package org.musicrank;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
//数据导入
public class Step1 {
    private static class ImportMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException{

            String line = value.toString();

            if(StringUtils.isBlank(line)){
                return;
            }

            //拆分数据
            String[] items = line.split("\t");

            //验证

            if(items.length != 6){
                return;
            }

            //输出
            context.write(value,NullWritable.get());

        }
    }

    private static class ImportReducer extends TableReducer<Text, NullWritable, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            //拆分数据
            String[] items = key.toString().split("\t");

            //定义
            byte[] rk = Bytes.toBytes(items[0]);
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("singer");
            byte[] c3 = Bytes.toBytes("gender");
            byte[] c4 = Bytes.toBytes("rythme");
            byte[] c5 = Bytes.toBytes("terminal");
            byte[] v1 = Bytes.toBytes(items[1]);
            byte[] v2 = Bytes.toBytes(items[2]);
            byte[] v3 = Bytes.toBytes(items[3]);
            byte[] v4 = Bytes.toBytes(items[4]);
            byte[] v5 = Bytes.toBytes(items[5]);


            Put put = new Put(rk);
            put.addColumn(fm,c1,v1);
            put.addColumn(fm,c2,v2);
            put.addColumn(fm,c3,v3);
            put.addColumn(fm,c4,v4);
            put.addColumn(fm,c5,v5);
            //输出
            context.write(NullWritable.get(),put);
        }
    }

    public static void run() {
        try {
            String tableName = "play_history";
            //创建表
            HbaseUtils.createTable(tableName,"info",true);
            //设置输出表名
            HbaseUtils.getConf().set(TableOutputFormat.OUTPUT_TABLE, tableName);
            //创建jOB
            Job job = Job.getInstance(HbaseUtils.getConf(),"data import");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,"hdfs://master:9000/music_played");
            //设置mapper
            job.setMapperClass(ImportMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            //设置reducer
            job.setReducerClass(ImportReducer.class);
            //设置输出
            job.setOutputFormatClass(TableOutputFormat.class);
            //运行
            boolean flag = job.waitForCompletion(true);

            if (flag){
                System.out.println("音乐播放数据导入成功");
            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
