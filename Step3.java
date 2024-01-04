package org.musicrank;

import javafx.scene.control.Tab;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
//生成歌曲播放排行榜
public class Step3 {

    private static class RankMapper extends TableMapper<IntWritable, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, IntWritable, Text>.Context context) throws IOException, InterruptedException {

            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("play_count");

            //读取行键，歌曲名称
            String name = Bytes.toString(value.getRow());

            if (value.containsColumn(fm,c1)){
                //读取播放次数
                String playCount = Bytes.toString(value.getValue(fm,c1));
                //输出
                context.write(new IntWritable(Integer.parseInt(playCount)),new Text(name));
            }


        }
    }

    private static class RankReducer extends TableReducer<IntWritable, Text, NullWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            //解决歌曲排行名次相同时，歌曲并列
            StringBuffer name_list = new StringBuffer();

            for (Text v : values){
                name_list.append(v.toString()+"\t");
            }

            //定义行键，列簇，列名，值
            byte[] rk = Bytes.toBytes(1000-key.get()+"_"+key.get());
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name_list");
            byte[] v1 = Bytes.toBytes(String.valueOf(name_list.toString()));
            //构建Put对象
            Put put = new Put(rk);
            put.addColumn(fm,c1,v1);
            //输出
            context.write(NullWritable.get(),put);

        }
    }

    private static class IntWritableSortDesc extends IntWritable.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void run() {
        try {
            String tableName = "rank";
            //创建表
            HbaseUtils.createTable(tableName,"info",true);
            //创建jOb
            Job job = Job.getInstance(HbaseUtils.getConf(),"rank ");
            //定义Scan用于读取Hbase表中数据
            Scan scan = new Scan();
            //定义列……
            byte[] fm = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("play_count");
            scan.addColumn(fm,c1);
            //*****设置排序
            job.setSortComparatorClass(IntWritableSortDesc.class);
            //设置mapper
            TableMapReduceUtil.initTableMapperJob("play_count", scan, RankMapper.class, IntWritable.class, Text.class, job);

            //设置reducer
            TableMapReduceUtil.initTableReducerJob(tableName, RankReducer.class, job);

            //运行
            boolean flag = job.waitForCompletion(true);

            if (flag){
                System.out.println("歌曲排行生成成功");
            }

        }catch (Exception e){
            e.printStackTrace();
        }



    }
}
