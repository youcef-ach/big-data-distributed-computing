package tp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        
        // Loop through all the "1"s associated with this word
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        result.set(sum);
        // Write the final count: (Word, Sum)
        context.write(key, result);
    }
}