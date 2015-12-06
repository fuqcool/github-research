import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class ScoreDistribution {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Event evt = Event.parseEvent(value.toString());

            if (evt.getScore() != 0) {
                context.write(new Text(evt.getRepo()), new IntWritable(evt.getScore()));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, NullWritable, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(NullWritable.get(), new IntWritable(sum));
        }
    }

    public static class MyMapper2 extends Mapper<Object, Text, IntWritable, IntWritable> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            int n = Integer.parseInt(value.toString().trim());

            context.write(new IntWritable(n / 500), new IntWritable(1));
        }
    }


    public static class MyReducer2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
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

        if (otherArgs.length < 3) {
            System.err.println("Usage: scoredist <in> <tmp> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "scoredist");
        job.setJarByClass(ScoreDistribution.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


        Configuration conf2 = new Configuration();

        Job job2 = new Job(conf2, "scoredist2");
        job2.setJarByClass(ScoreDistribution.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

        boolean succeed = job.waitForCompletion(true) && job2.waitForCompletion(true);
        System.exit(succeed ? 0 : 1);
    }
}
