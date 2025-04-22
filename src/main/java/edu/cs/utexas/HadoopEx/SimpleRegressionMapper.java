package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class SimpleRegressionMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private FloatWritable one = new FloatWritable(1);
    public void map(Object key, Text value, Context context) {

        try {
            Trip trip = new Trip(value.toString());

            context.write(new Text("n"), one);
            context.write(new Text("x"), new FloatWritable(trip.getTripDistance()));
            context.write(new Text("xx"), new FloatWritable(trip.getTripDistance() * trip.getTripDistance()));
            context.write(new Text("y"), new FloatWritable(trip.getFareAmount()));
            context.write(new Text("xy"), new FloatWritable(trip.getTripDistance() * trip.getFareAmount()));

        } catch (Exception e) {};
    }
    
}
