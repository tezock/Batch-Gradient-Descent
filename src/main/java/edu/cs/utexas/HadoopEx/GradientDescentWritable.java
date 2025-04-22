package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;


public class GradientDescentWritable implements Writable {

    private double[] pureGradients;
    private double n;
    private double cost;

    public GradientDescentWritable() {};

    public GradientDescentWritable(double n, double cost, double[] pureGradients) {

        this.n = n;
        this.cost = cost;
        this.pureGradients = pureGradients;
    }

    public void add(GradientDescentWritable other) {
        
        // add the costs together
        this.cost += other.cost;
        
        // Add the number of entries
        this.n += other.n;
        
        // build toward the 
        for (int i = 0; i < this.pureGradients.length; i++) {
            this.pureGradients[i] += other.pureGradients[i];
        }
    }

    public void multiplyTransform(double factor) {

        for (int i = 0; i < pureGradients.length; i++) {
            pureGradients[i] *= factor;
        }
    }

    public double getCost() {
        return cost;
    }

    public double getCount() {
        return n;
    }

    // should ONLY be called from the driver, as that's where the complete gradient is
    public double[] getGradient() {

        return pureGradients;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(cost);
        out.writeDouble(n);
        out.writeInt(pureGradients.length);
        for (double gradient : pureGradients) {
            out.writeDouble(gradient);
        }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        cost = in.readDouble();
        n = in.readDouble();
        int length = in.readInt();
        pureGradients = new double[length];
        for (int i = 0; i < length; i++) {
            pureGradients[i] = in.readDouble();
        }
    }
}
