import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Get number of times appears in the document
public class TF {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // File has been preprocessed, line[0] is OwnerUserId, line[1] is
            // Title line[2] is Body
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]), new Text(line[1] + " " + line[2]));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> words = new HashMap<String, Integer>();
            for (Text val : values) {
                StringTokenizer token = new StringTokenizer(val.toString());
                while (token.hasMoreTokens()) {
                    String word = token.nextToken();
                    Integer time = words.get(word);
                    if (time == null) {
                        words.put(word, 1);
                    } else {
                        words.put(word, time + 1);
                    }
                }
                Iterator<String> itor = words.keySet().iterator();
                while (itor.hasNext()) {
                    String k = (String) itor.next();
                    Integer v = words.get(k);
                    context.write(new Text(k), new Text(String.valueOf(v)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: TF <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "");
        job.setJarByClass(TF.class);
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
