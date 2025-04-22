package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

public class SimpleRegressionReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private float n = 0;
    private float x = 0;
    private float xx = 0;
    private float y = 0;
    private float xy = 0;

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
        throws IOException, InterruptedException {

        float sum = 0;

        for (FloatWritable value : values) {
            sum += value.get();
        }

        if (key.toString().equals("n"))
            n = sum;
        if (key.toString().equals("x"))
            x = sum;
        if (key.toString().equals("xx"))
            xx = sum;
        if (key.toString().equals("y"))
            y = sum;
        if (key.toString().equals("xy"))
            xy = sum;

        // context.write(new Text(key), new FloatWritable(sum));
    }

    public void cleanup(Context context) 
        throws IOException, InterruptedException {

        float m_numerator = (n * xy) - (x * y);
        float m_denominator = (n * xx) - (x * x);
        float b_numerator = (xx * y) - (x * xy);
        float b_denominator = (n * xx) - (x * x);

        context.write(new Text("m"), new FloatWritable(m_numerator / m_denominator));
        context.write(new Text("b"), new FloatWritable(b_numerator / b_denominator));
    }
    
}
