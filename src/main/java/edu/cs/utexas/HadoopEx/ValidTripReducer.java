package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ValidTripReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        // Simply output all valid lines. Since there's only one key ("valid"),
        // all valid lines are processed here.
        for (Text line : values) {
            // Here we output an empty key and the line as value.
            context.write(null, line);
        }
    }
}
