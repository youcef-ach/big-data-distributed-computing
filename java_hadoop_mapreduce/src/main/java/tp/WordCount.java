package tp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Create a new Job
        Job job = Job.getInstance(conf, "word count");
        
        // Tell Hadoop which Jar file to send to the cluster
        job.setJarByClass(WordCount.class);
        
        // Set the Mapper and Reducer classes
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); // Optional optimization
        job.setReducerClass(IntSumReducer.class);
        
        // Set the output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Set Input and Output paths from command line arguments
        // args[0] = Input Directory (e.g., /input)
        // args[1] = Output Directory (e.g., /output)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Wait for the job to finish and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}