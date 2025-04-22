package edu.cs.utexas.HadoopEx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class GradientDescentWritableParser {

    /**
     * Aggregates the GradientDescentWritable objects from all files in the directory that have a prefix "part-r-".
     *
     * @param directoryPath The path to the directory containing the files.
     * @return The aggregated GradientDescentWritable.
     * @throws IOException If the directory is invalid or reading any file fails.
     */
    // public static GradientDescentWritable aggregateFromDirectory(String directoryPath) throws IOException {
    //     File dir = new File(directoryPath);
    //     if (!dir.isDirectory()) {
    //         throw new IllegalArgumentException("Provided path is not a directory: " + directoryPath);
    //     }
        
    //     // Filter to only include files starting with "part-r-"
    //     File[] partFiles = dir.listFiles(new FilenameFilter() {
    //         @Override
    //         public boolean accept(File dir, String name) {
    //             return name.startsWith("part-r-");
    //         }
    //     });
        
    //     if (partFiles == null || partFiles.length == 0) {
    //         throw new IOException("No files with prefix 'part-r-' found in directory: " + directoryPath);
    //     }
        
    //     // Initialize aggregator with the zero value from the first file
    //     GradientDescentWritable aggregator = null;
    //     for (File file : partFiles) {
    //         // Use your parseFromFile method to read and parse the file into a GradientDescentWritable
    //         GradientDescentWritable gdw = parseFromFile(file.getAbsolutePath());
    //         if (aggregator == null) {
    //             aggregator = gdw;
    //         } else {
    //             if (gdw != null) {
    //                 aggregator.add(gdw);
    //             }
    //         }
    //     }
    //     return aggregator;
    // }

    public static GradientDescentWritable aggregateFromDirectory(String directoryPath, Configuration conf) throws IOException {
        Path dirPath = new Path(directoryPath);
        FileSystem fs = dirPath.getFileSystem(conf);
        // if (!fs.isDirectory(dirPath)) {
        //     throw new IllegalArgumentException("Provided path is not a directory: " + directoryPath);
        // }
        
        // List files in the directory that start with "part-r-"
        FileStatus[] statuses = fs.listStatus(dirPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith("part-r-");
            }
        });
        
        if (statuses == null || statuses.length == 0) {
            throw new IOException("No files with prefix 'part-r-' found in directory: " + directoryPath);
        }
        
        GradientDescentWritable aggregator = null;
        for (FileStatus status : statuses) {
            FSDataInputStream in = fs.open(status.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = reader.readLine();
            reader.close();
            if (line == null || line.trim().isEmpty()) {
                continue;
            }
            GradientDescentWritable gdw = parseGradientDescentWritable(line.trim());
            if (aggregator == null) {
                aggregator = gdw;
            } else {
                if (gdw != null) {
                    aggregator.add(gdw);
                }
            }
        }
        return aggregator;
    }

    /**
     * Reads the first line from the given file and parses it to create
     * a GradientDescentWritable.
     *
     * Expected file format (single line):
     * aggregate	gradient:-95.4114433298472,-1326.9322982560852 | cost:1.072721621620552E8 | count:85171.0
     *
     * @param fileName The name of the file.
     * @return A GradientDescentWritable representing the parsed data.
     * @throws IOException If the file is empty or an I/O error occurs.
     */
    public static GradientDescentWritable parseFromFile(String fileName) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line = reader.readLine();
            if (line == null || line.trim().isEmpty()) {
                throw new IOException("No valid data in file: " + fileName);
            }
            return parseGradientDescentWritable(line.trim());
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses a line into a GradientDescentWritable.
     *
     * @param line The input line.
     * @return A GradientDescentWritable built from the line's data.
     */
    public static GradientDescentWritable parseGradientDescentWritable(String line) {
        double[] gradients = parseGradient(line);
        double cost = parseCost(line);
        double count = parseCount(line);
        return new GradientDescentWritable(count, cost, gradients);
    }

    /**
     * Parses the gradient values from the input line.
     *
     * @param line The line containing the gradient values.
     * @return An array of doubles representing the gradients.
     */
    public static double[] parseGradient(String line) {
        int start = line.indexOf("gradient:");
        if (start == -1) {
            return new double[0];
        }
        start += "gradient:".length();
        int end = line.indexOf("|", start);
        String gradientStr = (end != -1) ? line.substring(start, end).trim() 
                                         : line.substring(start).trim();
        String[] tokens = gradientStr.split(",");
        double[] result = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            result[i] = Double.parseDouble(tokens[i].trim());
        }
        return result;
    }

    /**
     * Parses the cost value from the input line.
     *
     * @param line The line containing the cost.
     * @return The cost as a double.
     */
    public static double parseCost(String line) {
        int start = line.indexOf("cost:");
        if (start == -1) {
            return Double.NaN;
        }
        start += "cost:".length();
        int end = line.indexOf("|", start);
        String costStr = (end != -1) ? line.substring(start, end).trim() 
                                     : line.substring(start).trim();
        return Double.parseDouble(costStr);
    }

    /**
     * Parses the count value from the input line.
     *
     * @param line The line containing the count.
     * @return The count as a double.
     */
    public static double parseCount(String line) {
        int start = line.indexOf("count:");
        if (start == -1) {
            return Double.NaN;
        }
        start += "count:".length();
        int end = line.indexOf("|", start);
        String countStr = (end != -1) ? line.substring(start, end).trim() 
                                      : line.substring(start).trim();
        return Double.parseDouble(countStr);
    }

    // For testing purposes:
    public static void main(String[] args) {
        String fileName = "data.txt"; // Replace with your actual file name/path
        try {
            GradientDescentWritable writable = parseFromFile(fileName);
            System.out.println("Parsed GradientDescentWritable:");
            System.out.println("Count: " + writable.getCount());
            System.out.println("Cost: " + writable.getCost());
            System.out.println("Gradients: " + Arrays.toString(writable.getGradient()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
