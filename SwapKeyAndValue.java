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

public class SwapKeyAndValue {

    public static class MyMapper extends
            Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable k = new IntWritable();
        private IntWritable v = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            v.set(Integer.valueOf(line[0]));
            k.set(Integer.valueOf((line[1])));
            context.write(k, v);
        }
    }

    public static class MyReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(key, val);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: SwapKeyAndValue <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "");
        job.setJarByClass(SwapKeyAndValue.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
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
