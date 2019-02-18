import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class task1 {
    public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer i = new StringTokenizer(value.toString());
            while (i.hasMoreTokens()) {
                context.write(new Text(i.nextToken().replaceAll("[^A-Za-z]+", "").toLowerCase()), new IntWritable(1));
            }
        }
    }

    public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private int totalUnique;
        private int totalWords;
        
        @Override
        protected void setup(Context context) {
            totalUnique = 0;
            totalWords = 0;
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Unique Words"), new IntWritable(totalUnique));
            context.write(new Text("Total Words"), new IntWritable(totalWords));
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
                totalWords++;
            }
            totalUnique++;
            context.write(word, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setNumReduceTasks(1);
        job.setJobName("TASK 1: Map-Reduce Word Count");
        job.setJarByClass(task1.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}