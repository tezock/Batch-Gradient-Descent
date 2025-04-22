package edu.cs.utexas.HadoopEx;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;


public class AssignmentDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	// public static final String X = "x";
	// public static final String Y = "y";
	// public static final String XX = "xx";
	// public static final String xy = "xy";
	// public static final String n = "n";


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AssignmentDriver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

            String datasetPath = args[0];
            String output1 = args[1];
			String output2 = args[2];
			String logFile1 = args[3];
			String output3 = args[4];
			String logFile2 = args[5];

			/*
			 * Job 1 Details
			 * 
			 * Map Task
			 * Input: Taxi Lines
			 * Output: values for {n, x, xx, y, xy}
			 * 
			 * Reduce Task
			 * Input: {n, x, xx, y, xy} -> iterable<values>
			 * Output: values for {m, b}
			 * 
			 */

			Job job = new Job(conf, "Simple Linear Regression");
			job.setJarByClass(getClass());

			// specify a Mapper
			job.setMapperClass(SimpleRegressionMapper.class);

			// specify a Reducer
			job.setReducerClass(SimpleRegressionReducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(datasetPath));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(output1));
			job.setOutputFormatClass(TextOutputFormat.class);

			// specify 1 reducer
			job.setNumReduceTasks(1);

			int status = job.waitForCompletion(true) ? 0 : 1;
            if (status != 0) return status;


			status = runTask2(datasetPath, output2, logFile1);
			if (status != 0) return status;

			return runTask3(datasetPath, output3, logFile2);
            // return status;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}

	private int runTask2(String inputFileName, String outputFileName, String logFileName) 
		throws IOException, InterruptedException, ClassNotFoundException {

		double[] predictions = {0.001, 0.001};
		double learningRate = 0.001;
		int max_iterations = 100;

		double prev_cost = Float.MAX_VALUE;

		for (int task_iter = 1; task_iter <= max_iterations; task_iter++) {

			Configuration jobConf = new Configuration();
			jobConf.set("weights", encodeWeights(predictions));
			
			Job job = Job.getInstance(jobConf, "GradientDescent Iteration " + task_iter);
			job.setJarByClass(getClass());
			job.setMapperClass(SimpleDescentMapper.class);
			job.setReducerClass(SimpleDescentReducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputValueClass(GradientDescentWritable.class);

			// specify input and output directories

			FileInputFormat.addInputPath(job, new Path(inputFileName));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(outputFileName + "/iter" + task_iter));
			job.setOutputFormatClass(TextOutputFormat.class);

			job.waitForCompletion(true);

			GradientDescentWritable aggregated = GradientDescentWritableParser.aggregateFromDirectory(outputFileName + "/iter" + task_iter, jobConf);
			
			appendToFile(logFileName, "Iteration " + task_iter + " Of Task 2", jobConf);
			appendToFile(logFileName, "Current Gradient   :" + Arrays.toString(aggregated.getGradient()), jobConf);
			aggregated.multiplyTransform(2.0 / aggregated.getCount());
			appendToFile(logFileName, "Transformed Gradient   :" + Arrays.toString(aggregated.getGradient()), jobConf);
			appendToFile(logFileName, "Current Cost       :" + Double.toString(aggregated.getCost() / aggregated.getCount()), jobConf);
			appendToFile(logFileName, "Number of Entries  :" + Double.toString(aggregated.getCount()), jobConf);
			appendToFile(logFileName, "Current Predictions:" + Arrays.toString(predictions), jobConf);
			appendToFile(logFileName, "Diff          :" + Math.abs((aggregated.getCost() / aggregated.getCount()) / prev_cost), jobConf);
			appendToFile(logFileName, "", jobConf);
			
			prev_cost = aggregated.getCost() / aggregated.getCount();

			double[] gradient = aggregated.getGradient();
			
			for (int i = 0; i < gradient.length; i++) {
				predictions[i] -= learningRate * gradient[i];
			}
		}

		return 0;
	}

	private int runTask3(String inputFileName, String outputFileName, String logFileName) 
		throws IOException, InterruptedException, ClassNotFoundException {

		double[] predictions = {0.1, 0.1, 0.1, 0.1, 0.1};
		double learningRate = 0.00000001;
		int max_iterations = 100;

		double prev_cost = Float.MAX_VALUE;

		for (int task_iter = 1; task_iter <= max_iterations; task_iter++) {

			Configuration jobConf = new Configuration();
			jobConf.set("weights", encodeWeights(predictions));
			
			Job job = Job.getInstance(jobConf, "GradientDescent Iteration " + task_iter);
			job.setJarByClass(getClass());
			job.setMapperClass(ComplexDescentMapper.class);
			job.setReducerClass(SimpleDescentReducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputValueClass(GradientDescentWritable.class);

			// specify input and output directories

			FileInputFormat.addInputPath(job, new Path(inputFileName));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(outputFileName + "/iter" + task_iter));
			job.setOutputFormatClass(TextOutputFormat.class);

			job.waitForCompletion(true);

			GradientDescentWritable aggregated = GradientDescentWritableParser.aggregateFromDirectory(outputFileName + "/iter" + task_iter, jobConf);
			
			appendToFile(logFileName, "Iteration " + task_iter + " Of Task 3", jobConf);
			appendToFile(logFileName, "Current Gradient   :" + Arrays.toString(aggregated.getGradient()), jobConf);
			aggregated.multiplyTransform(2.0 / aggregated.getCount());
			appendToFile(logFileName, "Transformed Gradient   :" + Arrays.toString(aggregated.getGradient()), jobConf);
			appendToFile(logFileName, "Current Cost       :" + Double.toString(aggregated.getCost() / aggregated.getCount()), jobConf);
			appendToFile(logFileName, "Number of Entries  :" + Double.toString(aggregated.getCount()), jobConf);
			appendToFile(logFileName, "Current Predictions:" + Arrays.toString(predictions), jobConf);
			appendToFile(logFileName, "Diff          :" + Math.abs((aggregated.getCost() / aggregated.getCount()) / prev_cost), jobConf);
			appendToFile(logFileName, "", jobConf);

			prev_cost = aggregated.getCost() / aggregated.getCount();

			double[] gradient = aggregated.getGradient();
			
			for (int i = 0; i < gradient.length; i++) {
				predictions[i] -= learningRate * gradient[i];
			}

		}

		return 0;
	}

	

	/* Encodes some weights into a string that can be passed into a job. */
	public static String encodeWeights(double[] arr) {
        if (arr == null || arr.length == 0) {
            System.out.println(""); // Handle empty array case
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arr.length; i++) {
            // sb.append(String.format("%.2f", arr[i])); // Format to 2 decimal places
			sb.append(Double.toString(arr[i]));
            if (i < arr.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

	/**
     * Appends the given text to the specified file.
     *
     * @param filename The file to append to.
     * @param text The text to append.
     */
    // public static void appendToFile(String filename, String text) {
    //     // Open file in append mode (true)
    //     try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
    //         writer.write(text);
    //         writer.newLine();  // Adds a new line after the text (optional)
    //     } catch (IOException e) {
    //         System.err.println("Error appending to file: " + e.getMessage());
    //     }
    // }

	public static void appendToFile(String filename, String text, Configuration conf) {
    try {
        Path path = new Path(filename);
        FileSystem fs = path.getFileSystem(conf);
        FSDataOutputStream out;
        if (fs.exists(path)) {
            try {
                // Try to open in append mode
                out = fs.append(path);
            } catch (UnsupportedOperationException e) {
                // Cloud storage (e.g. GCS) often doesn't support append.
                // Here you might choose to create a new file or merge files later.
                System.err.println("Append not supported on this filesystem. " +
                                   "Consider writing to a new file.");
                return;
            }
        } else {
            // Create the file if it does not exist.
            out = fs.create(path);
        }
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        writer.write(text);
        writer.newLine();
        writer.close();
        out.close();
    } catch (IOException e) {
        System.err.println("Error appending to file: " + e.getMessage());
    }
}
}
