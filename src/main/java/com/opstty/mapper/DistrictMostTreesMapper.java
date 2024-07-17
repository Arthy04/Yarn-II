package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMostTreesMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static int DISTRICT_COLUMN_INDEX = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("ARRONDISSEMENT")) {
            // Skip header row
            return;
        }

        String[] columns = value.toString().split(";");
        if (columns.length > DISTRICT_COLUMN_INDEX) {
            String district = columns[DISTRICT_COLUMN_INDEX].trim();
            context.write(new Text(district), new LongWritable(1));
        }
    }
}
