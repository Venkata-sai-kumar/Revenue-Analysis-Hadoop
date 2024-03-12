import java.util.*;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MergeDatasets {

    public static class MergeMapper extends Mapper<Object, Text, NullWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String formattedValue = formatData(value.toString());
            if (formattedValue != null) {
                context.write(NullWritable.get(), new Text(formattedValue));
            }
        }

        private String formatData(String input) {
            String[] fields = input.split(",");

            // Skip header line
            //if (fields[0].equals("hotel") || fields[0].equals("Booking_ID")) {
             //   return;
           // }

            String arrival_year;
            String arrival_month;
            String booking_status;
            String avg_price_per_room;
            String stays_in_weekend_nights;
            String stays_in_week_nights;
            String monthToString = "0";
            String monthInFull;
            String bstatusRaw;
            String bstatus = "0";

            

            // Assuming the order of the columns is consistent with the provided schema
            // For hotel-bookings dataset
            if (fields.length > 11) {
                arrival_year = fields[3];

                monthInFull = fields[4];
                monthInFull = monthInFull.toUpperCase();
                if (monthInFull.equals("JANUARY")) {  monthToString = "1"; }
                if (monthInFull.equals("FEBRUARY")) {  monthToString = "2"; }
                if (monthInFull.equals("MARCH")) {  monthToString = "3"; }
                if (monthInFull.equals("APRIL")) {  monthToString = "4"; }
                if (monthInFull.equals("MAY")) {  monthToString = "5"; }
                if (monthInFull.equals("JUNE")) { monthToString =  "6"; }
                if (monthInFull.equals("JULY")) {  monthToString = "7"; }
                if (monthInFull.equals("AUGUST")) {  monthToString = "8"; }
                if (monthInFull.equals("SEPTEMBER")) {  monthToString = "9"; }
                if (monthInFull.equals("OCTOBER")) {  monthToString = "10"; }
                if (monthInFull.equals("NOVEMBER")) { monthToString =  "11"; }
                if (monthInFull.equals("DECEMBER")) {  monthToString = "12"; }


                arrival_month = monthToString;
                booking_status = fields[1];
                avg_price_per_room = fields[11];
                stays_in_weekend_nights = fields[7];
                stays_in_week_nights = fields[8];
            }
            // For customer-reservations dataset
            else {
                arrival_year = fields[4];
                arrival_month = fields[5];

                bstatusRaw = fields[9];

                bstatusRaw = bstatusRaw.toUpperCase();
                if (bstatusRaw.equals("NOT_CANCELED")) {  bstatus = "0"; }
                if (bstatusRaw.equals("CANCELED")) {  bstatus = "1"; }

                booking_status = bstatus;
                avg_price_per_room = fields[8];
                stays_in_weekend_nights = fields[1];
                stays_in_week_nights = fields[2];
            }

            // Formatting the record to a common format
            return String.join(",",
                arrival_year,
                arrival_month,
                booking_status,
                avg_price_per_room,
                stays_in_weekend_nights,
                stays_in_week_nights
            );
        }

    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MergeDatasets.class);
        job.setMapperClass(MergeMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1); // This is a Map only job

        FileInputFormat.addInputPath(job, new Path(args[0])); // Path to the first dataset
        FileInputFormat.addInputPath(job, new Path(args[1])); // Path to the second dataset
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
