package edu.cs.utexas.HadoopEx;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

public class Trip {

    // the indices of each field
    private static final int MEDALLION = 0;
    private static final int HACK_LICENSE = 1;
    private static final int FARE_AMOUNT = 11;
    private static final int SURCHARGE = 12;
    private static final int MTA_TAX = 13;
    private static final int TIP_AMOUNT = 14;
    private static final int TOLLS_AMOUNT = 15;
    private static final int TOTAL_AMOUNT = 16;
    private static final int TRIP_TIME_IN_SECS = 4;
    private static final int PICKUP_LONGITUDE = 6;
    private static final int PICKUP_LATITUDE = 7;
    private static final int DROPOFF_LONGITUDE = 8;
    private static final int DROPOFF_LATITUDE = 9;
    private static final int PAYMENT_TYPE = 10;
    private static final int PICKUP_DATETIME = 2;
    private static final int DROPOFF_DATETIME = 3;
    private static final int TRIP_DISTANCE = 5;

    
    // the fields representing a trip
    private String medallion;
    private String hackLicense;
    private float totalAmount;
    private long tripTimeInSecs;
    private float tripDistance;
    private String line;
    private float fareAmount;
    private float tollsAmount;



    // irrelevant fields for the current use-case
    // private String pickupDatetime;
    // private String dropoffDatetime;
    // private String pickupLongitude;
    // private String pickupLatitude;
    // private String dropoffLongitude;
    // private String dropoffLatitude;
    // private String paymentType;
    // private String surcharge;
    // private String mtaTax;
    // private String tipAmount;

    // regex patterns for validating dates
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    public Trip() {};

    
    /*
     * Initializes a 'trip' object.
     * 
     * Each trip instance represents a valid trip.
     */
    public Trip(String line) {
        this.line = line;
        // split the line by delimeter
        String[] parsedLine = line.split(",");
        
        // handle invalid lines
        if (!lineIsValid(parsedLine))
            throw new IllegalArgumentException("invalid line");

        // initialize relevant fields
        medallion = parsedLine[MEDALLION];
        hackLicense = parsedLine[HACK_LICENSE];
        totalAmount = Float.parseFloat(parsedLine[TOTAL_AMOUNT]);
        tripTimeInSecs = Long.parseLong(parsedLine[TRIP_TIME_IN_SECS]);
        tripDistance = Float.parseFloat(parsedLine[TRIP_DISTANCE]);
        fareAmount = Float.parseFloat(parsedLine[FARE_AMOUNT]);
        tollsAmount = Float.parseFloat(parsedLine[TOLLS_AMOUNT]);
    }
    
    /*
     * Gets the driver for the current trip
     */
    public String getDriver() {
        return hackLicense;
    }

    /*
     * Returns the taxi used for the current trip
     */
    public String getTaxi() {
        return medallion;
    }

    /*
     * Returns the total amount/cost of the current trip
     */
    public float getAmount() {
        return totalAmount;
    }

    /*
     * Returns the total amount/cost of the current trip
     */
    public float getFareAmount() {
        return fareAmount;
    }

    /*
     * Returns the duration of the trip in seconds.
     */
    public long getTripDuration() {
        return tripTimeInSecs;
    }

    /*
     * Returns the distance of the trip
     */
    public float getTripDistance() {
        return tripDistance;
    }

    public float getTollsAmount() {
        return tollsAmount;
    }

    /*
     * Helper method to determine whether a given string is a float or not.
     * If an exception is caught, we simply return false.
     */
    private static boolean isFloat(String str) {
        try {
            Float.parseFloat(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
     * Helper method to determine whether a given string is a long or not.
     * If an exception is caught, we simply return false.
     */
    private static boolean isLong(String str) {
        try {
            Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean hasGPSError() {

        String[] parsedLine = line.split(",");

        float pickup_latitude = Float.parseFloat(parsedLine[PICKUP_LATITUDE]);
        float pickup_longitude = Float.parseFloat(parsedLine[PICKUP_LONGITUDE]);
        float dropoff_latitude = Float.parseFloat(parsedLine[DROPOFF_LATITUDE]);
        float dropoff_longitude = Float.parseFloat(parsedLine[DROPOFF_LONGITUDE]);

        if (Math.abs(pickup_latitude) == 0 || Math.abs(dropoff_latitude) == 0)
            return true;
        if (Math.abs(pickup_longitude) == 0 || Math.abs(dropoff_longitude) == 0)
            return true;

        return false;
    }

    /*
     * Helper method to ensure that the line used to construct this 'Trip'
     * is valid.
     */
    private boolean lineIsValid(String[] parsedLine) {

        // ensure line length
        if (parsedLine.length != 17) 
            return false;

        // ensure money values are floats
        if (!isFloat(parsedLine[FARE_AMOUNT]))
            return false;
        if (!isFloat(parsedLine[SURCHARGE]))
            return false;
        if (!isFloat(parsedLine[MTA_TAX]))
            return false;
        if (!isFloat(parsedLine[TIP_AMOUNT]))
            return false;
        if (!isFloat(parsedLine[TOLLS_AMOUNT]))
            return false;
        if (!isFloat(parsedLine[TOTAL_AMOUNT]))
            return false;

        // ensure the total charge matches (w/ flexibility for floating point math)
        float fareAmount = Float.parseFloat(parsedLine[FARE_AMOUNT]);
        float surcharge = Float.parseFloat(parsedLine[SURCHARGE]);
        float mtaTax = Float.parseFloat(parsedLine[MTA_TAX]);
        float tipAmount = Float.parseFloat(parsedLine[TIP_AMOUNT]);
        float tollsAmount = Float.parseFloat(parsedLine[TOLLS_AMOUNT]);

        float testAmount = fareAmount + surcharge + mtaTax + tipAmount + tollsAmount;
        float totalAmount = Float.parseFloat(parsedLine[TOTAL_AMOUNT]);

        if (Math.abs(testAmount - totalAmount) >= 0.05)
            return false;
        
        // ignore rides with total amount >= 500.
        if (totalAmount >= 500)
            return false;

        // do checks for longitude & latitude to ensure they're reasonable
        if (!isFloat(parsedLine[PICKUP_LATITUDE]))
            return false;
        if (!isFloat(parsedLine[PICKUP_LONGITUDE]))
            return false;
        if (!isFloat(parsedLine[DROPOFF_LATITUDE]))
            return false;
        if (!isFloat(parsedLine[DROPOFF_LONGITUDE]))
            return false;

        float pickup_latitude = Float.parseFloat(parsedLine[PICKUP_LATITUDE]);
        float pickup_longitude = Float.parseFloat(parsedLine[PICKUP_LONGITUDE]);
        float dropoff_latitude = Float.parseFloat(parsedLine[DROPOFF_LATITUDE]);
        float dropoff_longitude = Float.parseFloat(parsedLine[DROPOFF_LONGITUDE]);

        if (Math.abs(pickup_latitude) > 90 || Math.abs(dropoff_latitude) > 90)
            return false;
        if (Math.abs(pickup_longitude) > 180 || Math.abs(dropoff_longitude) > 180)
            return false;

        // validate the trip length
        if (!isLong(parsedLine[TRIP_TIME_IN_SECS]))
            return false;

        LocalDateTime start = validateAndParse(parsedLine[PICKUP_DATETIME]);
        LocalDateTime end = validateAndParse(parsedLine[DROPOFF_DATETIME]);

        if (start == null || end == null)
            return false;
        
        long actualDuration = java.time.Duration.between(start, end).getSeconds();
        long statedDuration = Long.parseLong(parsedLine[TRIP_TIME_IN_SECS]);

        if (actualDuration != statedDuration || actualDuration < 30)
            return false;

        // ensure that we know the payment type
        if ("UNK".equals(parsedLine[PAYMENT_TYPE]))
            return false;

        // validate the trip's distance
        if (!isFloat(parsedLine[TRIP_DISTANCE]))
            return false;
        float tripDistance = Float.parseFloat(parsedLine[TRIP_DISTANCE]);

        if (tripDistance == 0)
            return false;

        // remove trips that are < 2min or > 1 hour
        if (actualDuration < 120 || actualDuration > 3600)
            return false;

        // handle rides with a fare < 3 dollars or > 200 dollars
        if (fareAmount < 3.00 || fareAmount > 200)
            return false;

        // handle rides less than 1 mile or more than 50 miles
        if (tripDistance < 1 || tripDistance > 50)
            return false;

        // handle toll amounts less than 3 dollars
        if (tollsAmount < 3) 
            return false;

        // if we meet data cleaning criteria, return true!
        return true;
    }

    /**
     * Extracts the hour from the pickup datetime field
     * @return The hour (0-23) of the pickup time, or -1 if parsing fails
     */
    public int getPickupHour() {
        try {
            String[] parsedLine = line.split(",");
            String pickupDatetime = parsedLine[PICKUP_DATETIME].trim();
            
            // Use the existing DateTimeFormatter and validation method
            LocalDateTime pickupTime = validateAndParse(pickupDatetime);
            if (pickupTime != null) {
                return pickupTime.getHour();
            }
            return -1;
        } catch (Exception e) {
            return -1;
        }
    }

    public static LocalDateTime validateAndParse(String dateStr) {

        // check if format matches expected pattern
        if (!DATE_PATTERN.matcher(dateStr).matches()) {
            return null;
        }

        // attempt to parse the date
        try {
            return LocalDateTime.parse(dateStr, FORMATTER);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

}