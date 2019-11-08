package sy533;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SaleMain {
    public static void main(String[] args) throws Exception{
        //创建一个job和任务入口（指定主类）
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Sale.class);
        //指定job的mapper和输出的类型 k2 v2
        job.setMapperClass(SaleMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Sale.class);
        //指定job的reducer和输出类型 k4 v4
        job.setReducerClass(SaleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //指定job的输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //执行任务
        job.waitForCompletion(true);
    }
}
