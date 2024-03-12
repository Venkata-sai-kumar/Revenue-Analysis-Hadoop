import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class SortByValueMR {

    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        private DoubleWritable value = new DoubleWritable();
        private Text key = new Text();

        public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            // Split the input line into key-value pairs
            String[] parts = line.toString().split("\t");
           // String[] parts = line.toString().split("\\t");
           // String[] parts = line.toString().split("  ");
            if (parts.length == 2) {
                String keyValue = parts[0];
                double value = Double.parseDouble(parts[1]);
                
                // Emit key-value pair with value as the key and keyValue as the value
                context.write(new DoubleWritable(value), new Text(keyValue));
            }
        }
    }

    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        private TreeMap<Double, String> sortedMap = new TreeMap<>();

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Iterate through the values and add them to the sortedMap
            for (Text value : values) {
                sortedMap.put(key.get(), value.toString());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit the key-value pairs from the sortedMap in descending order of values
            for (Map.Entry<Double, String> entry : sortedMap.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new DoubleWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(SortByValueMR.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
