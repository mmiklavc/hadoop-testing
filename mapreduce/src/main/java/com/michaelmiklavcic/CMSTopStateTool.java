package com.michaelmiklavcic;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.*;

public class CMSTopStateTool extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CMSTopStateTool(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        final String jobName = args[0];
        final String dbName = args[1];
        final String inTableName = args[2];
        final String outPath = args[3];
//        final String outTableName = args[3];

        Job job = Job.getInstance(conf, jobName);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setJarByClass(CMSTopStateTool.class);
        job.setMapperClass(CMSTopStateMapper.class);
        job.setReducerClass(CMSTopStateReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        HCatInputFormat.setInput(job, dbName, inTableName);
//        HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName, outTableName, null));

//        HCatSchema schema = HCatOutputFormat.getTableSchema(conf);
//        System.err.println("INFO: output schema explicitly set for writing:" + schema);
//        HCatOutputFormat.setSchema(job, schema);
//        job.setOutputFormatClass(HCatOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

}
