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

public class Top10PostByScore {

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private Text score = new Text();
        private Text title = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // File has been preprocessed, line[0] is Score, line[1] is Title.
            String[] line = value.toString().split(",");
            try {
                // Formating the Score.
                DecimalFormat df = new DecimalFormat("0000");
                String scorestr = df.format(Integer.parseInt(line[0]));
                score.set(scorestr);
                title.set(line[1]);
                context.write(score, title);
            } catch (Exception e) {
                // Skip csv top line and dirty data.
            }
        }
    }

    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Top10PostByScore <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "");
        job.setJarByClass(Top10PostByScore.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileInputFormat.setMinInputSplitSize(job, 10);
        FileOutputFormat.setOutputPath(job, new Path(
                otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
