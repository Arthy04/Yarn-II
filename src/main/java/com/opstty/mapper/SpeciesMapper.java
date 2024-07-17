package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static int SPECIES_COLUMN_INDEX = 3;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] columns = value.toString().split(";");
            if (columns.length > SPECIES_COLUMN_INDEX) {
                String species = columns[SPECIES_COLUMN_INDEX].trim();
                context.write(new Text(species), new Text(""));
            }
        } catch (Exception e) {

        }
    }
}
