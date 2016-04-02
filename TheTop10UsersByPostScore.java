import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TheTop10UsersByPostScore {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text user = new Text();
        private IntWritable score = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // File has been preprocessed, line[0] is OwnerUserId, line[1] is
            // Score.
            String[] line = value.toString().split(",");
            try {
                user.set(line[0]);
                score.set(Integer.valueOf(line[1]));
                context.write(user, score);
            } catch (Exception e) {
                // Skip csv top line and dirty data.
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalScore = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            try {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                totalScore.set(sum);
                context.write(key, totalScore);
            } catch (Exception e) {
                // Skip csv top line and dirty data.
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err
                    .println("Usage: TheTop10UsersByPostScore <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "");
        job.setJarByClass(TheTop10UsersByPostScore.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileInputFormat.setMinInputSplitSize(job, 10);
        FileOutputFormat.setOutputPath(job, new Path(
                otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
