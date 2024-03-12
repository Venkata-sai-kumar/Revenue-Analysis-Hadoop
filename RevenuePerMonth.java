import java.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class RevenuePerMonth {

    public static class RevenueMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text yearMonth = new Text();
        private DoubleWritable revenue = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] parts = value.toString().split(",");
            // String[] parts = line.toString().split("\\t");
           // String[] parts = line.toString().split("  ");

            // Skip header line
            if (parts[0].equals("hotel")) { // Basically to ignore first row of datasets
                return;
            }

            try {
                String arrival_year = parts[0];
                String arrival_month = parts[1];
                String booking_status = parts[2];
                double avg_price_per_room = Double.parseDouble(parts[3]);
                int stays_in_weekend_nights = Integer.parseInt(parts[4]);
                int stays_in_week_nights = Integer.parseInt(parts[5]);

                if (booking_status.equals("0")) { // That is if booking was not cancelled
                    int total_nights = stays_in_weekend_nights + stays_in_week_nights;
                    double revenue = avg_price_per_room * total_nights;

                    yearMonth.set(arrival_year + "_" + arrival_month);
                    this.revenue.set(revenue);

                    context.write(yearMonth, this.revenue);
                }
            } catch (NumberFormatException e) {
                System.err.println("Error: " + e);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }

            // Write the result
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(RevenuePerMonth.class);
        job.setMapperClass(RevenueMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path (output from the first job)
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
