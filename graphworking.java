import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {

public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = s.nextInt();
            //double y = s.nextDouble();
            context.write(new IntWritable(x),new IntWritable(1));
            s.close();
        }
    }
public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,LongWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            //double sum = 0.0;
            long count = 0;
            for (IntWritable v: values) {
                //sum += v.get();
                count++;
            };
            context.write(key,new LongWritable(count));
        }
    }
	
	public static void main ( String[] args ) throws Exception {
	Job job = Job.getInstance();
	//Path newOutputPath = new Path("output1");
        job.setJobName("MyJob");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//FileOutputFormat.setOutputPath(job,new Path("output1"));
        job.waitForCompletion(true);
		}
		}