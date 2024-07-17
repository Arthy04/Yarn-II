package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeKindMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static int SPECIES_COLUMN_INDEX = 2;
    private final static LongWritable ONE = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] columns = value.toString().split(";");
            if (columns.length > SPECIES_COLUMN_INDEX) {
                String kind = columns[SPECIES_COLUMN_INDEX].trim();
                context.write(new Text(kind), ONE);
            }
        } catch (Exception e) {
            // Log and handle any exceptions as needed
        }
    }
}
