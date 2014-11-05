package com.michaelmiklavcic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

public class CMSStatePaymentsTool extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CMSStatePaymentsTool(), args);
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
        System.out.println("jobname: " + jobName);
        System.out.println("dbName: " + dbName);
        System.out.println("inTableName: " + inTableName);
        System.out.println("outPath: " + outPath);

        Job job = Job.getInstance(conf, jobName);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setJarByClass(CMSStatePaymentsTool.class);
        job.setMapperClass(CMSStatePaymentsMapper.class);
        job.setReducerClass(CMSStatePaymentsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        HCatInputFormat.setInput(job, dbName, inTableName);
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

}
