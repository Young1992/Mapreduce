import java.io.IOException;
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

public class TheNumberOfJava {

    public static class MyMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text user = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // File has been preprocessed, line[0] is OwnerUserId, line[1] is
            // Title line[2] is Body.
            String[] line = value.toString().split(",");
            if (line.length > 2) {
                if (line[1].contains("java") || line[2].contains("java")) {
                    user.set(line[0]);
                    context.write(user, one);
                }
            } else if (line.length > 1) {
                if (line[1].contains("java")) {
                    user.set(line[0]);
                    context.write(user, one);
                }
            }
        }
    }

    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: TheNumberOfJava <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "");
        job.setJarByClass(TheNumberOfJava.class);
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
