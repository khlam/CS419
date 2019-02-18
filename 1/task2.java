import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class task2 {
    public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lists = value.toString().split(",");
            Arrays.sort(lists);
            for (int i = 0; i < lists.length; i++) {
                for (int j = i+1; j < lists.length; j++) {
                    context.write(new Text( "(" + lists[i].replaceAll("^\\s+", "") + ", " + lists[j].replaceAll("^\\s+", "") + ")"), new IntWritable(1));
                }
            }
        }
    }

    public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private int totalUnique;
        
        @Override
        protected void setup(Context context) {
            totalUnique = 0;
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Total Unique Pairs"), new IntWritable(totalUnique));
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalUnique++;
            context.write(word, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJobName("TASK 1: Map-Reduce Word Count");
        job.setJarByClass(task2.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}