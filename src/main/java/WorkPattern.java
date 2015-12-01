import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

public class WorkPattern {

    public static class Time implements WritableComparable<Time> {
        private static final String[] weekDays;

        static {
            weekDays = new String[7];
            weekDays[0] = "Sun";
            weekDays[1] = "Mon";
            weekDays[2] = "Tue";
            weekDays[3] = "Wed";
            weekDays[4] = "Thu";
            weekDays[5] = "Fri";
            weekDays[6] = "Sat";
        }

        private int dayOfWeek;
        private int hour;

        public void write(DataOutput out) throws IOException {
            out.writeInt(dayOfWeek);
            out.writeInt(hour);
        }

        public void readFields(DataInput in) throws IOException {
            this.dayOfWeek = in.readInt();
            this.hour = in.readInt();
        }

        public int compareTo(Time t) {
            if (dayOfWeek != t.dayOfWeek)
                return Integer.compare(dayOfWeek, t.dayOfWeek);
            return Integer.compare(hour, t.hour);
        }

        @Override
        public String toString() {
            return weekDays[dayOfWeek-1] + ":" + Integer.toString(hour);
        }
    }


    public static class MyMapper extends Mapper<Object, Text, Time, IntWritable> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Event evt = Event.parseEvent(value.toString());

            Calendar cal = Calendar.getInstance();
            cal.setTime(evt.getCreatedAt());

            Time t = new Time();
            t.dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
            t.hour = cal.get(Calendar.HOUR_OF_DAY);

            context.write(t, new IntWritable(evt.getScore()));
        }
    }

    public static class MyReducer extends Reducer<Time, IntWritable, Time, IntWritable> {
        public void reduce(Time key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: workpattern <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "workpattern");
        job.setJarByClass(ScoreDistribution.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Time.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
