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
import org.apache.hadoop.util.GenericOptionsParser;

// Get number of documents containing t
// The result is the number of documents containing t
public class IDF {
    public static class MyMapper extends
            Mapper<Object, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // File has been preprocessed, line[0] is OwnerUserId, line[1] is
            // Title line[2] is Body
            String[] line = value.toString().split("\t");
            StringTokenizer token = new StringTokenizer(line[1] + " " + line[2]);  
            while (token.hasMoreTokens()) {  
                k.set(line[0]);
                v.set(token.nextToken());
                context.write(k, v);  
            }  
        }
    }

    public static class MyReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            int TotalNumberOfDucuments = 0;  
            for (Text val : values) {  
                TotalNumberOfDucuments += 1;
            }  
            context.write(key, new Text(String.valueOf(TotalNumberOfDucuments)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: IDF <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "");
        job.setJarByClass(IDF.class);
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
