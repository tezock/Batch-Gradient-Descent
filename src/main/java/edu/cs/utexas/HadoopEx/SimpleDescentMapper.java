package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.io.DataOutput;
import java.io.IOException;

import java.util.Random;


public class SimpleDescentMapper extends Mapper<Object, Text, Text, GradientDescentWritable> {
    
    private double[] theta;        // Current parameter vector (θ)
    private int vectorLength;      // Number of features (bias term + features)
    private Random random;
    private int numReducers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        random = new Random();
        numReducers = context.getNumReduceTasks();
        // Retrieve parameter vector from configuration
        String thetaStr = conf.get("weights");
        String[] thetaParts = thetaStr.split(",");
        theta = new double[thetaParts.length];
        for (int i = 0; i < thetaParts.length; i++) {
            theta[i] = Double.parseDouble(thetaParts[i]);
        }
        vectorLength = theta.length;
    }

    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException{

        try {

            Trip trip = new Trip(value.toString());

            // construct feature vector: bias term + features
            double[] features = new double[vectorLength];
            features[0] = 1.0; // Bias term
            features[1] = trip.getTripDistance();

            // Compute dot product (prediction)
            double prediction = 0.0;
            for (int i = 0; i < vectorLength; i++) {
                prediction += theta[i] * features[i];
            }

            double actualValue = trip.getFareAmount();
            double error = actualValue - prediction;
            double costContribution = error * error;

            // Compute gradient contribution (error * feature)
            double[] gradientContribution = new double[vectorLength];
            for (int i = 0; i < vectorLength; i++) {
                gradientContribution[i] = -1.0 * error * features[i];
            }
            int composite_id = random.nextInt(numReducers);
            context.write(new Text("aggregate_" + composite_id), new GradientDescentWritable(1, costContribution, gradientContribution));
        } catch (Exception e) {};
    }
}
