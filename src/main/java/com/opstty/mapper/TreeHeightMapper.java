package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeHeightMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final static int SPECIES_COLUMN_INDEX = 2;
    private final static int HEIGHT_COLUMN_INDEX = 6;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] columns = value.toString().split(";");
            if (columns.length > HEIGHT_COLUMN_INDEX) {
                String species = columns[SPECIES_COLUMN_INDEX].trim();
                double height = Double.parseDouble(columns[HEIGHT_COLUMN_INDEX].trim());

                context.write(new Text(species), new DoubleWritable(height));
            }
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            // Handle parsing errors or missing columns
            // Log and skip the record if needed
        }
    }
}
