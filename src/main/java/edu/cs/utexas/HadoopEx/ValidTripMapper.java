package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ValidTripMapper extends Mapper<LongWritable, Text, Text, Text> {
    // Use a constant key so that all valid lines go to the same reducer.
    private final static Text constantKey = new Text("valid");

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        try {
            // Attempt to construct a Trip. If the line is invalid, the Trip constructor
            // will throw an IllegalArgumentException.
            Trip trip = new Trip(value.toString());
            // If successful, emit the original line.
            context.write(constantKey, value);
        } catch (IllegalArgumentException e) {
            // Ignore invalid lines.
        }
    }
}
