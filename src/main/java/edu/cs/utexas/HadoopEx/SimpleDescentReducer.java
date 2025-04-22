package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleDescentReducer extends Reducer<Text, GradientDescentWritable, Text, Text> {

    private int vectorLength;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // Retrieve parameter vector from configuration
        String thetaStr = conf.get("weights");
        String[] thetaParts = thetaStr.split(",");
        vectorLength = thetaParts.length;
    }

    @Override
    public void reduce(Text key, Iterable<GradientDescentWritable> values, Context context)
            throws IOException, InterruptedException {

        GradientDescentWritable aggregated = new GradientDescentWritable(0, 0, new double[vectorLength]);

        for (GradientDescentWritable val : values) {
            aggregated.add(val);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("gradient:");
        double[] avgGradient = aggregated.getGradient();
        for (int i = 0; i < avgGradient.length; i++) {
            sb.append(avgGradient[i]);
            if (i < avgGradient.length - 1) {
                sb.append(",");
            }
        }
        sb.append(" | cost:").append(aggregated.getCost());
        sb.append(" | count:").append(aggregated.getCount());

        context.write(key, new Text(sb.toString()));
    }
}
