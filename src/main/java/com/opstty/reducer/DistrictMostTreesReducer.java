package com.opstty.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictMostTreesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private Text districtWithMostTrees = new Text();
    private long maxTrees = Long.MIN_VALUE;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }

        // Find the district with the maximum number of trees
        if (sum > maxTrees) {
            maxTrees = sum;
            districtWithMostTrees.set(key);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(districtWithMostTrees, new LongWritable(maxTrees));
    }
}
