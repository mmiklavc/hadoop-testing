package com.michaelmiklavcic;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class CMSTopStateReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
//        float sum = 0;
//        for(FloatWritable val : values) {
//            sum += val.get();
//        }
        BigDecimal sum = new BigDecimal(0);
        for(Text val : values) {
            sum = sum.add(new BigDecimal(val.toString()));
        }
        context.write(key, new Text(sum.toString()));
    }

}
