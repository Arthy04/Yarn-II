package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeHeightSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private final static int HEIGHT_COLUMN_INDEX = 6;
    private final static int GENRE_COLUMN_INDEX = 2;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("GEOPOINT")) {
            // Skip header row
            return;
        }

        String[] columns = value.toString().split(";");
        if (columns.length > Math.max(HEIGHT_COLUMN_INDEX, GENRE_COLUMN_INDEX)) {
            try {
                double height = Double.parseDouble(columns[HEIGHT_COLUMN_INDEX].trim());
                String genre = columns[GENRE_COLUMN_INDEX].trim();
                context.write(new DoubleWritable(height), new Text(genre));
            } catch (NumberFormatException e) {
                // Skip invalid height values
            }
        }
    }
}
